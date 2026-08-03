// Package kv is the job queue over RESP.
//
// Same contract as the table-backed queue in the parent module, same Worker,
// same handlers -- one line different in main. Use it when the volume outgrows
// what a table handles comfortably, and accept what it costs: a job pushed here
// is not committed by the transaction that produced it.
//
// That trade is the whole difference between the two. The table-backed queue
// cannot lose a job that its transaction committed, and cannot keep one whose
// transaction rolled back. This one is faster and offers neither.
//
// The implementation stays inside plain RESP: a list for what is ready, a
// sorted set for what is scheduled or leased, and a hash for the payloads. No
// RedisJSON, no RediSearch, no Lua -- which is what keeps Dragonfly a drop-in
// replacement (doc 11).
package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/arandu-io/framework/data"
	"github.com/arandu-io/framework/jobs"
	"github.com/arandu-io/framework/security"
	"github.com/redis/go-redis/v9"
)

// Store is the queue over RESP.
type Store struct {
	client *redis.Client
	prefix string
}

// Options is what it takes to connect.
type Options struct {
	// Address is host:port.
	Address string
	// Password and Database come from configuration, never from a literal.
	Password string
	Database int
	// Prefix namespaces the keys, so two applications can share one server.
	Prefix string
}

// New returns the store.
func New(opts Options) *Store {
	if opts.Address == "" {
		opts.Address = "127.0.0.1:6379"
	}
	return &Store{
		client: redis.NewClient(&redis.Options{
			Addr:        opts.Address,
			Password:    opts.Password,
			DB:          opts.Database,
			DialTimeout: 5 * time.Second,
			ReadTimeout: 3 * time.Second,
		}),
		prefix: opts.Prefix,
	}
}

// Wrap returns a store over a client the application already opened.
func Wrap(client *redis.Client, prefix string) *Store {
	return &Store{client: client, prefix: prefix}
}

var _ jobs.Queue = (*Store)(nil)

// Close releases the pool.
func (s *Store) Close() error { return s.client.Close() }

// Ping verifies the connection, for the health check.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("queue/kv: %w", err)
	}
	return nil
}

// The key layout. Three keys per queue, and one hash for every job body:
//
//	<prefix>:q:<queue>:scheduled   sorted set, score = when it becomes eligible
//	<prefix>:q:<queue>:leased      sorted set, score = when the lease expires
//	<prefix>:q:jobs                hash, id -> the serialized job
//	<prefix>:q:parked              sorted set, score = when it gave up
//
// Scheduled and leased are the same shape on purpose: reserving is moving an id
// from one to the other, and an expired lease is just an id whose score passed.
func (s *Store) key(parts ...string) string {
	key := "q"
	for _, p := range parts {
		key += ":" + p
	}
	if s.prefix == "" {
		return key
	}
	return s.prefix + ":" + key
}

// Push adds a job.
func (s *Store) Push(ctx context.Context, g security.Grant, j jobs.Job) error {
	tenant := data.Tenant(g)
	if tenant == "" {
		return jobs.ErrNoTenant
	}
	if j.Name == "" {
		return jobs.ErrNoName
	}
	j.TenantID = tenant
	if j.Queue == "" {
		j.Queue = jobs.DefaultQueue
	}
	if j.RunAt.IsZero() {
		j.RunAt = time.Now().UTC()
	}

	body, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("queue/kv: serializing %s: %w", j.Name, err)
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.key("jobs"), j.ID, body)
	pipe.ZAdd(ctx, s.key(j.Queue, "scheduled"), redis.Z{
		Score:  float64(j.RunAt.UnixMilli()),
		Member: j.ID,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue/kv: pushing %s: %w", j.Name, err)
	}
	return nil
}

// Reserve takes jobs whose time has come, and leases them.
//
// It also sweeps expired leases back into the scheduled set first, which is
// what makes a worker that died recoverable: its jobs simply become eligible
// again when the lease passes.
func (s *Store) Reserve(ctx context.Context, queue string, n int, lease time.Duration) ([]jobs.Job, error) {
	if queue == "" {
		queue = jobs.DefaultQueue
	}
	if n <= 0 {
		n = 1
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	now := time.Now().UTC()

	if err := s.recoverExpiredLeases(ctx, queue, now); err != nil {
		return nil, err
	}

	scheduled := s.key(queue, "scheduled")
	ids, err := s.client.ZRangeByScore(ctx, scheduled, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(now.UnixMilli(), 10),
		Count: int64(n),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("queue/kv: reading %s: %w", queue, err)
	}

	out := make([]jobs.Job, 0, len(ids))
	for _, id := range ids {
		// ZRem is the claim: exactly one worker removes the member, and the
		// others get zero back. That is the compare-and-set, in plain RESP.
		removed, err := s.client.ZRem(ctx, scheduled, id).Result()
		if err != nil {
			return nil, fmt.Errorf("queue/kv: claiming %s: %w", id, err)
		}
		if removed == 0 {
			continue
		}

		j, err := s.load(ctx, id)
		if err != nil {
			// The body is gone and the id was claimed: nothing to run, and
			// leaving it out of the lease set is what stops it coming back.
			continue
		}
		j.Attempts++

		body, err := json.Marshal(j)
		if err != nil {
			return nil, fmt.Errorf("queue/kv: serializing %s: %w", j.ID, err)
		}
		pipe := s.client.TxPipeline()
		pipe.HSet(ctx, s.key("jobs"), j.ID, body)
		pipe.ZAdd(ctx, s.key(queue, "leased"), redis.Z{
			Score:  float64(now.Add(lease).UnixMilli()),
			Member: j.ID,
		})
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("queue/kv: leasing %s: %w", j.ID, err)
		}
		out = append(out, j)
	}
	return out, nil
}

// recoverExpiredLeases moves jobs whose lease passed back into the queue.
func (s *Store) recoverExpiredLeases(ctx context.Context, queue string, now time.Time) error {
	leased := s.key(queue, "leased")
	expired, err := s.client.ZRangeByScore(ctx, leased, &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(now.UnixMilli(), 10),
	}).Result()
	if err != nil {
		return fmt.Errorf("queue/kv: reading the leases of %s: %w", queue, err)
	}

	for _, id := range expired {
		removed, err := s.client.ZRem(ctx, leased, id).Result()
		if err != nil {
			return fmt.Errorf("queue/kv: recovering %s: %w", id, err)
		}
		if removed == 0 {
			continue
		}
		if err := s.client.ZAdd(ctx, s.key(queue, "scheduled"), redis.Z{
			Score: float64(now.UnixMilli()), Member: id,
		}).Err(); err != nil {
			return fmt.Errorf("queue/kv: requeueing %s: %w", id, err)
		}
	}
	return nil
}

// Ack removes a finished job.
func (s *Store) Ack(ctx context.Context, j jobs.Job) error {
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.key(j.Queue, "leased"), j.ID)
	pipe.HDel(ctx, s.key("jobs"), j.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue/kv: acknowledging %s: %w", j.ID, err)
	}
	return nil
}

// Fail schedules the retry, or parks the job.
func (s *Store) Fail(ctx context.Context, j jobs.Job, cause error, retryAt time.Time, park bool) error {
	if cause != nil {
		j.LastError = cause.Error()
	}
	body, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("queue/kv: serializing %s: %w", j.ID, err)
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.key("jobs"), j.ID, body)
	pipe.ZRem(ctx, s.key(j.Queue, "leased"), j.ID)

	if park {
		pipe.ZAdd(ctx, s.key("parked"), redis.Z{
			Score: float64(time.Now().UTC().UnixMilli()), Member: j.ID,
		})
	} else {
		if retryAt.IsZero() {
			retryAt = time.Now().UTC()
		}
		pipe.ZAdd(ctx, s.key(j.Queue, "scheduled"), redis.Z{
			Score: float64(retryAt.UTC().UnixMilli()), Member: j.ID,
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue/kv: recording the failure of %s: %w", j.ID, err)
	}
	return nil
}

// Parked lists the jobs that gave up, most recent failure first.
func (s *Store) Parked(ctx context.Context, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	ids, err := s.client.ZRevRange(ctx, s.key("parked"), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("queue/kv: reading the parked jobs: %w", err)
	}

	out := make([]jobs.Job, 0, len(ids))
	for _, id := range ids {
		j, err := s.load(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, j)
	}
	return out, nil
}

// Retry puts a parked job back in line with its attempts reset.
func (s *Store) Retry(ctx context.Context, id string) error {
	j, err := s.load(ctx, id)
	if err != nil {
		return err
	}
	j.Attempts = 0
	j.LastError = ""
	j.RunAt = time.Now().UTC()

	body, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("queue/kv: serializing %s: %w", id, err)
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.key("jobs"), id, body)
	pipe.ZRem(ctx, s.key("parked"), id)
	pipe.ZAdd(ctx, s.key(j.Queue, "scheduled"), redis.Z{
		Score: float64(j.RunAt.UnixMilli()), Member: id,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue/kv: retrying %s: %w", id, err)
	}
	return nil
}

// Pending is how many jobs are waiting.
func (s *Store) Pending(ctx context.Context, queue string) (int, error) {
	if queue == "" {
		queue = jobs.DefaultQueue
	}
	count, err := s.client.ZCard(ctx, s.key(queue, "scheduled")).Result()
	if err != nil {
		return 0, fmt.Errorf("queue/kv: counting %s: %w", queue, err)
	}
	return int(count), nil
}

// Oldest is how long the oldest waiting job has been waiting.
func (s *Store) Oldest(ctx context.Context, queue string) (time.Duration, error) {
	if queue == "" {
		queue = jobs.DefaultQueue
	}

	first, err := s.client.ZRangeWithScores(ctx, s.key(queue, "scheduled"), 0, 0).Result()
	if err != nil {
		return 0, fmt.Errorf("queue/kv: measuring the age of %s: %w", queue, err)
	}
	if len(first) == 0 {
		return 0, nil
	}

	runAt := time.UnixMilli(int64(first[0].Score))
	if wait := time.Since(runAt); wait > 0 {
		return wait, nil
	}
	// A job scheduled for the future is not late.
	return 0, nil
}

func (s *Store) load(ctx context.Context, id string) (jobs.Job, error) {
	raw, err := s.client.HGet(ctx, s.key("jobs"), id).Bytes()
	if err != nil {
		return jobs.Job{}, fmt.Errorf("queue/kv: reading %s: %w", id, err)
	}
	var j jobs.Job
	if err := json.Unmarshal(raw, &j); err != nil {
		return jobs.Job{}, fmt.Errorf("queue/kv: decoding %s: %w", id, err)
	}
	return j, nil
}
