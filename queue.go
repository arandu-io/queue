// Package queue is the job queue over the application's database.
//
// The contract lives in the core, in framework/jobs: Job, Handler, Queue and
// the Worker. This package is one implementation of it, and it is the default
// one for a reason -- it needs nothing installed. The jobs table sits in the
// database the application already has, and a job is committed by the same
// transaction as the row it is about.
//
// That last part is what a Redis queue cannot offer. Pushing a job inside
// data.Transaction means the job exists if and only if the write did, which is
// the outbox guarantee applied to work instead of to events.
//
// For volume beyond what a table handles comfortably, github.com/arandu-io/queue/kv
// is the same contract over RESP. Same Worker, same handlers, one line different
// in main.
package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/arandu-io/framework/data"
	"github.com/arandu-io/framework/jobs"
	"github.com/arandu-io/framework/security"
)

// Store is the queue backed by the application's database.
type Store struct {
	db *data.DB
}

// New returns the store.
func New(db *data.DB) *Store { return &Store{db: db} }

var _ jobs.Queue = (*Store)(nil)

// Push adds a job.
//
// Inside data.Transaction it joins it, which is the property this driver exists
// for: the job is committed by the same transaction as the row it describes, so
// it cannot refer to a write that rolled back.
func (s *Store) Push(ctx context.Context, g security.Grant, j jobs.Job) error {
	tenant := data.Tenant(g)
	if tenant == "" {
		return jobs.ErrNoTenant
	}
	if j.Name == "" {
		return jobs.ErrNoName
	}

	runAt := j.RunAt
	if runAt.IsZero() {
		runAt = time.Now().UTC()
	}
	queue := j.Queue
	if queue == "" {
		queue = jobs.DefaultQueue
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (
			id, queue, name, tenant_id, payload, authorized_by, action,
			run_at, attempts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, queue, j.Name, tenant, string(j.Payload), j.AuthorizedBy, j.Action,
		runAt, j.Attempts)
	if err != nil {
		return fmt.Errorf("queue: pushing %s: %w", j.Name, err)
	}
	return nil
}

// Reserve takes jobs off the queue and hides them for the lease.
//
// Two statements rather than one, and the reason is portability: the tight form
// is UPDATE ... RETURNING with a FOR UPDATE SKIP LOCKED subquery, which
// Postgres has and SQLite does not. Selecting the candidates and then claiming
// each one by its id -- with reserved_until in the WHERE -- is correct on every
// engine, because the claim itself is the compare-and-set.
//
// The cost is that two workers can pick the same candidate and one of them
// loses the claim. It gets nothing back, which is exactly right.
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
	candidates, err := s.query(ctx, `
		WHERE queue = ? AND failed_at IS NULL AND run_at <= ?
		  AND (reserved_until IS NULL OR reserved_until < ?)
		ORDER BY run_at
		LIMIT ?`, queue, now, now, n)
	if err != nil {
		return nil, err
	}

	until := now.Add(lease)
	out := make([]jobs.Job, 0, len(candidates))
	for _, j := range candidates {
		res, err := s.db.ExecContext(ctx, `
			UPDATE jobs SET reserved_until = ?, attempts = attempts + 1
			WHERE id = ? AND (reserved_until IS NULL OR reserved_until < ?)`,
			until, j.ID, now)
		if err != nil {
			return nil, fmt.Errorf("queue: reserving %s: %w", j.ID, err)
		}
		claimed, err := res.RowsAffected()
		if err != nil || claimed == 0 {
			// Another worker got it between the select and the update. Not an
			// error: that is the compare-and-set doing its job.
			continue
		}
		// Attempts was incremented by the claim, so the worker sees the number
		// this delivery is.
		j.Attempts++
		out = append(out, j)
	}
	return out, nil
}

// Ack removes a finished job.
//
// Deleted rather than marked done. A jobs table that keeps every job ever run
// is a table that needs its own cleanup job, and the history that matters --
// what ran, how long it took, what it queried -- is on the console.
func (s *Store) Ack(ctx context.Context, j jobs.Job) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, j.ID); err != nil {
		return fmt.Errorf("queue: acknowledging %s: %w", j.ID, err)
	}
	return nil
}

// Fail records the failure and schedules the retry, or parks the job.
func (s *Store) Fail(ctx context.Context, j jobs.Job, cause error, retryAt time.Time, park bool) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}

	if park {
		_, err := s.db.ExecContext(ctx,
			`UPDATE jobs SET failed_at = ?, last_error = ?, reserved_until = NULL WHERE id = ?`,
			time.Now().UTC(), message, j.ID)
		if err != nil {
			return fmt.Errorf("queue: parking %s: %w", j.ID, err)
		}
		return nil
	}

	if retryAt.IsZero() {
		retryAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET run_at = ?, last_error = ?, reserved_until = NULL WHERE id = ?`,
		retryAt.UTC(), message, j.ID)
	if err != nil {
		return fmt.Errorf("queue: scheduling the retry of %s: %w", j.ID, err)
	}
	return nil
}

// Parked lists the jobs that gave up, most recent failure first.
func (s *Store) Parked(ctx context.Context, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.query(ctx, `WHERE failed_at IS NOT NULL ORDER BY failed_at DESC LIMIT ?`, limit)
}

// Retry puts a parked job back in line with its attempts reset.
//
// Without it the only way out of a dead letter queue is SQL by hand, which is
// how it becomes a table nobody touches.
func (s *Store) Retry(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET failed_at = NULL, attempts = 0, last_error = NULL,
		                reserved_until = NULL, run_at = ?
		WHERE id = ?`, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("queue: retrying %s: %w", id, err)
	}
	return nil
}

// Pending is how many jobs are waiting.
func (s *Store) Pending(ctx context.Context, queue string) (int, error) {
	if queue == "" {
		queue = jobs.DefaultQueue
	}
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM jobs WHERE queue = ? AND failed_at IS NULL`, queue).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("queue: counting %s: %w", queue, err)
	}
	return count, nil
}

// Oldest is how long the oldest waiting job has been waiting.
//
// A stopped worker looks exactly like an idle one, and this is what tells them
// apart. It feeds the health check.
func (s *Store) Oldest(ctx context.Context, queue string) (time.Duration, error) {
	if queue == "" {
		queue = jobs.DefaultQueue
	}

	// ORDER BY ... LIMIT 1 rather than min(run_at): an aggregate loses the
	// declared type of the column, and SQLite then hands back a string that
	// will not scan into a time.Time.
	var oldest time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT run_at FROM jobs
		WHERE queue = ? AND failed_at IS NULL
		ORDER BY run_at
		LIMIT 1`, queue).Scan(&oldest)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("queue: measuring the age of %s: %w", queue, err)
	}
	if wait := time.Since(oldest); wait > 0 {
		return wait, nil
	}
	// A job scheduled for the future is not late.
	return 0, nil
}

// query runs the standard projection with a caller-supplied tail.
func (s *Store) query(ctx context.Context, tail string, args ...any) ([]jobs.Job, error) {
	// The newline is load-bearing: without it a tail starting with WHERE
	// concatenates into "FROM jobsWHERE", which SQLite reads as a table alias
	// and then fails on the next word -- an error that names a keyword three
	// tokens away from the actual problem.
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, queue, name, tenant_id, payload, authorized_by, action,
		       run_at, attempts, last_error
		FROM jobs
		`+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("queue: reading the queue: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []jobs.Job
	for rows.Next() {
		var j jobs.Job
		var payload string
		var lastError sql.NullString
		if err := rows.Scan(&j.ID, &j.Queue, &j.Name, &j.TenantID, &payload,
			&j.AuthorizedBy, &j.Action, &j.RunAt, &j.Attempts, &lastError); err != nil {
			return nil, fmt.Errorf("queue: reading the queue: %w", err)
		}
		j.Payload = []byte(payload)
		j.LastError = lastError.String
		out = append(out, j)
	}
	return out, rows.Err()
}
