package queue_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/framework/data"
	"github.com/arandu-io/framework/jobs"
	"github.com/arandu-io/framework/kernel"
	"github.com/arandu-io/framework/security"
	"github.com/arandu-io/queue"

	_ "modernc.org/sqlite"
)

// Everything below runs against a real database, because everything this
// package claims is a claim about rows: that a reserve hides the job from other
// workers, that a lease expiring brings it back, that a job pushed inside a
// transaction disappears when the transaction rolls back. A fake proves none of
// those.
//
// SQLite, so it needs nothing installed.

const tenant = "11111111-1111-4111-8111-111111111111"

func grant() security.Grant { return security.SystemGrant("invoice.send", tenant) }

func store(t *testing.T) (*queue.Store, *data.DB) {
	t.Helper()

	sqldb, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "queue.sqlite"))
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	// One writer: SQLite serializes writes anyway, and a larger pool only turns
	// the wait into "database is locked".
	sqldb.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqldb.Close() })

	db := data.Wrap(sqldb, data.DialectSQLite)
	migrations := queue.NewModule(nil).Migrations()
	if _, err := data.Migrate(context.Background(), db, migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return queue.New(db), db
}

func push(t *testing.T, s *queue.Store, name string) jobs.Job {
	t.Helper()
	j, err := jobs.New(grant(), "", name, map[string]string{"n": name})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Push(context.Background(), grant(), j); err != nil {
		t.Fatalf("Push: %v", err)
	}
	return j
}

func TestPushAndReserve(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()

	pushed := push(t, s, "invoice.send")

	reserved, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(reserved) != 1 {
		t.Fatalf("reserved %d jobs, want 1", len(reserved))
	}

	got := reserved[0]
	if got.ID != pushed.ID || got.Name != "invoice.send" || got.TenantID != tenant {
		t.Fatalf("reserved %+v", got)
	}
	// The Grant that pushed it travelled with it, which is what the worker
	// reissues the work under.
	if got.Action != "invoice.send" {
		t.Errorf("the action did not survive: %q", got.Action)
	}
	// Attempts counts this delivery, so a handler can tell a first try from a
	// retry without asking the driver.
	if got.Attempts != 1 {
		t.Errorf("attempts = %d on the first delivery, want 1", got.Attempts)
	}

	var payload struct {
		N string `json:"n"`
	}
	if err := got.Decode(&payload); err != nil || payload.N != "invoice.send" {
		t.Errorf("payload = %+v (%v)", payload, err)
	}
}

// TestAReservedJobIsHiddenFromOtherWorkers is the property that makes more than
// one worker safe. Without it, every worker runs every job.
func TestAReservedJobIsHiddenFromOtherWorkers(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	first, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("Reserve: %v, %d jobs", err, len(first))
	}

	second, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("a second worker reserved %d jobs that were already taken", len(second))
	}
}

// TestAnExpiredLeaseComesBack is what makes a worker crash recoverable, and it
// is why delivery is at-least-once rather than at-most-once.
func TestAnExpiredLeaseComesBack(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	if _, err := s.Reserve(ctx, "", 10, 50*time.Millisecond); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	time.Sleep(120 * time.Millisecond)

	again, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("the job did not come back after the lease expired")
	}
	if again[0].Attempts != 2 {
		t.Errorf("attempts = %d on the second delivery, want 2", again[0].Attempts)
	}
}

// TestAJobPushedInATransactionRollsBackWithIt is the reason this driver is the
// default. A Redis queue cannot offer it: the job would exist and the row it is
// about would not.
func TestAJobPushedInATransactionRollsBackWithIt(t *testing.T) {
	s, db := store(t)
	ctx := context.Background()
	refused := errors.New("the rule said no")

	err := data.Transaction(ctx, db, func(ctx context.Context) error {
		j, _ := jobs.New(grant(), "", "invoice.send", nil)
		if err := s.Push(ctx, grant(), j); err != nil {
			return err
		}
		return refused
	})
	if !errors.Is(err, refused) {
		t.Fatalf("err = %v", err)
	}

	reserved, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(reserved) != 0 {
		t.Fatalf("%d jobs survived a rolled back transaction", len(reserved))
	}
}

func TestAckRemovesTheJob(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	if err := s.Ack(ctx, reserved[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if pending, err := s.Pending(ctx, ""); err != nil || pending != 0 {
		t.Fatalf("pending = %d (%v), want 0 after the ack", pending, err)
	}
	// Even after the lease expires: an acknowledged job never comes back.
	time.Sleep(10 * time.Millisecond)
	again, _ := s.Reserve(ctx, "", 10, time.Minute)
	if len(again) != 0 {
		t.Fatal("an acknowledged job came back")
	}
}

func TestFailSchedulesTheRetry(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	err := s.Fail(ctx, reserved[0], errors.New("the broker refused"), time.Now().Add(time.Hour), false)
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Scheduled for an hour from now, so it is not eligible.
	soon, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(soon) != 0 {
		t.Fatal("a job scheduled for the future was reserved now")
	}
	// And it is not late either, or the health check would fire on a backlog
	// that does not exist.
	if lag, err := s.Oldest(ctx, ""); err != nil || lag != 0 {
		t.Errorf("a future job reports lag %s (%v)", lag, err)
	}
}

// TestAParkedJobDoesNotBlockTheQueue: a worker stuck on one bad payload stops
// draining everything behind it, which turns one malformed job into an outage
// of every other.
func TestAParkedJobDoesNotBlockTheQueue(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	if err := s.Fail(ctx, reserved[0], errors.New("the payload is malformed"), time.Time{}, true); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	parked, err := s.Parked(ctx, 10)
	if err != nil {
		t.Fatalf("Parked: %v", err)
	}
	if len(parked) != 1 || parked[0].LastError != "the payload is malformed" {
		t.Fatalf("parked = %+v", parked)
	}

	// What comes after it still runs.
	push(t, s, "report.monthly")
	next, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(next) != 1 || next[0].Name != "report.monthly" {
		t.Fatalf("the parked job blocked the queue: %+v", next)
	}
}

// TestRetryBringsAParkedJobBack: without it the only way out of a dead letter
// queue is SQL by hand, which is how it becomes a table nobody touches.
func TestRetryBringsAParkedJobBack(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	_ = s.Fail(ctx, reserved[0], errors.New("the consumer was down"), time.Time{}, true)

	parked, _ := s.Parked(ctx, 10)
	if err := s.Retry(ctx, parked[0].ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	back, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(back) != 1 {
		t.Fatal("the retried job did not come back")
	}
	// Attempts reset, so the retry gets the full budget rather than parking on
	// the first failure.
	if back[0].Attempts != 1 {
		t.Errorf("attempts = %d after a retry, want the count to have restarted", back[0].Attempts)
	}
}

// TestTheLagTellsAStoppedWorkerFromAnIdleOne is the number the health check is
// built on.
func TestTheLagTellsAStoppedWorkerFromAnIdleOne(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()

	if lag, err := s.Oldest(ctx, ""); err != nil || lag != 0 {
		t.Fatalf("an empty queue reports lag %s (%v)", lag, err)
	}

	push(t, s, "invoice.send")
	lag, err := s.Oldest(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if lag <= 0 || lag > time.Minute {
		t.Fatalf("lag = %s, want a small positive duration", lag)
	}

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	_ = s.Ack(ctx, reserved[0])
	if lag, err := s.Oldest(ctx, ""); err != nil || lag != 0 {
		t.Fatalf("after draining, lag = %s (%v)", lag, err)
	}
}

// TestQueuesAreSeparate: a password reset and a monthly report should not wait
// behind each other, which is the only reason queues have names.
func TestQueuesAreSeparate(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()

	slow, _ := jobs.New(grant(), "reports", "report.monthly", nil)
	if err := s.Push(ctx, grant(), slow); err != nil {
		t.Fatal(err)
	}
	push(t, s, "invoice.send")

	def, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil || len(def) != 1 || def[0].Name != "invoice.send" {
		t.Fatalf("the default queue returned %+v (%v)", def, err)
	}

	reports, err := s.Reserve(ctx, "reports", 10, time.Minute)
	if err != nil || len(reports) != 1 || reports[0].Name != "report.monthly" {
		t.Fatalf("the reports queue returned %+v (%v)", reports, err)
	}
}

// TestAJobWithoutATenantIsRefused is RULE 14 at the driver, not only at the
// constructor: a caller can build a Job struct by hand.
func TestAJobWithoutATenantIsRefused(t *testing.T) {
	s, _ := store(t)

	err := s.Push(context.Background(), security.SystemGrant("invoice.send", ""),
		jobs.Job{ID: "j-1", Name: "invoice.send", Payload: []byte("{}")})
	if !errors.Is(err, jobs.ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
}

// TestTheModuleReportsABacklog: a queue nobody drains has to fail the health
// check, or the first sign is a customer asking why nothing happened.
func TestTheModuleReportsABacklog(t *testing.T) {
	s, db := store(t)
	ctx := context.Background()
	module := queue.NewModule(s)

	if err := module.Health(ctx); err != nil {
		t.Fatalf("an empty queue is unhealthy: %v", err)
	}
	if got := module.Diagnose(ctx); len(got) != 0 {
		t.Fatalf("an empty queue diagnosed %v", got)
	}

	// A job old enough to be a backlog.
	old, _ := jobs.New(grant(), "", "invoice.send", nil)
	old.RunAt = time.Now().Add(-10 * time.Minute)
	if err := s.Push(ctx, grant(), old); err != nil {
		t.Fatal(err)
	}

	if err := module.Health(ctx); err == nil {
		t.Fatal("a queue ten minutes behind is reported healthy")
	}
	diagnosis := module.Diagnose(ctx)
	if len(diagnosis) == 0 {
		t.Fatal("a backlog produced no diagnosis")
	}
	if !contains(diagnosis[0], "aru work") {
		t.Errorf("the diagnosis does not say what to check: %q", diagnosis[0])
	}

	_ = db
}

// TestTheMigrationIsPortable: one migration, three engines.
func TestTheMigrationIsPortable(t *testing.T) {
	for _, m := range queue.NewModule(nil).Migrations() {
		for _, engineSpecific := range []string{"jsonb", "timestamptz", "SERIAL", "AUTO_INCREMENT", "SKIP LOCKED"} {
			if contains(m.Up, engineSpecific) {
				t.Errorf("%s uses %q, which is one engine's spelling", m.ID, engineSpecific)
			}
		}
		if m.Down == "" {
			t.Errorf("%s cannot be rolled back", m.ID)
		}
	}
	var _ kernel.Migratable = queue.NewModule(nil)
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestMaxAttemptsDeliversExactlyThatManyTimes is a bug an audit found: the
// delivery was counted twice.
//
// Reserve increments attempts in the row and returns the job carrying the new
// count. The worker then added one more, so MaxAttempts of N delivered N-1
// times -- and MaxAttempts of 2 parked on the first failure, with no retry at
// all. The in-memory queue the worker tests use did not increment, which is why
// it never showed up there.
//
// This runs the real worker loop against the real driver, where the increment
// actually happens.
func TestMaxAttemptsDeliversExactlyThatManyTimes(t *testing.T) {
	for _, maxAttempts := range []int{2, 3} {
		t.Run(fmt.Sprintf("max%d", maxAttempts), func(t *testing.T) {
			s, _ := store(t)
			push(t, s, "invoice.send")

			var mu sync.Mutex
			deliveries := 0

			w := jobs.NewWorker(s, jobs.WorkerOptions{
				Poll:        time.Millisecond,
				Concurrency: 1,
				MaxAttempts: maxAttempts,
				// No wait between attempts: the point is the count, not the timing.
				Backoff: func(int) time.Duration { return 0 },
			})
			w.HandleFunc("invoice.send", func(context.Context, security.Grant, jobs.Job) error {
				mu.Lock()
				deliveries++
				mu.Unlock()
				return errors.New("the broker refused")
			})

			ctx, stop := context.WithCancel(context.Background())
			go func() { _ = w.Run(ctx) }()

			// Wait for the job to park, which is what ends the retries.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				parked, err := s.Parked(context.Background(), 10)
				if err == nil && len(parked) == 1 {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			stop()
			// Let the loop notice the cancellation before reading the count.
			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			got := deliveries
			mu.Unlock()

			if got != maxAttempts {
				t.Errorf("MaxAttempts of %d delivered %d times", maxAttempts, got)
			}

			parked, err := s.Parked(context.Background(), 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(parked) != 1 {
				t.Fatalf("%d jobs parked, want 1", len(parked))
			}
			if parked[0].Attempts != maxAttempts {
				t.Errorf("the parked job counted %d attempts, want %d", parked[0].Attempts, maxAttempts)
			}
		})
	}
}

// TestAJobBeingRunIsNotABacklog is a bug an audit found, and it turned a worker
// doing its job into an outage.
//
// Pending and Oldest counted reserved jobs, so a two-minute handler with a
// one-minute health threshold made /_arandu/health answer 503 for the whole run
// -- and a load balancer takes the instance out of rotation for a 503. Reserve
// always knew that a reserved job is running rather than waiting; the two
// queries the health check reads did not.
func TestAJobBeingRunIsNotABacklog(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()

	// Pushed in the past, so it is already "late" by any threshold.
	j, err := jobs.New(grant(), "", "invoice.send", nil)
	if err != nil {
		t.Fatal(err)
	}
	j.RunAt = time.Now().Add(-time.Hour)
	if err := s.Push(ctx, grant(), j); err != nil {
		t.Fatal(err)
	}

	if lag, err := s.Oldest(ctx, ""); err != nil || lag < time.Minute {
		t.Fatalf("before the reserve the job should look late: lag = %s (%v)", lag, err)
	}

	reserved, err := s.Reserve(ctx, "", 1, time.Minute)
	if err != nil || len(reserved) != 1 {
		t.Fatalf("Reserve: %v, %d jobs", err, len(reserved))
	}

	// A worker is holding it. It is running, not waiting.
	if lag, err := s.Oldest(ctx, ""); err != nil || lag != 0 {
		t.Errorf("a job being run reports lag %s (%v) -- the health check would 503", lag, err)
	}
	if pending, err := s.Pending(ctx, ""); err != nil || pending != 0 {
		t.Errorf("a job being run counts as %d pending (%v)", pending, err)
	}
}

// TestAnExpiredLeaseIsABacklogAgain is the other side: a worker that died holds
// nothing, and the job it was running is waiting again. Reporting it as still
// running is how a stopped worker looks healthy forever.
func TestAnExpiredLeaseIsABacklogAgain(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()

	j, err := jobs.New(grant(), "", "invoice.send", nil)
	if err != nil {
		t.Fatal(err)
	}
	j.RunAt = time.Now().Add(-time.Hour)
	if err := s.Push(ctx, grant(), j); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Reserve(ctx, "", 1, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)

	if lag, err := s.Oldest(ctx, ""); err != nil || lag < time.Minute {
		t.Errorf("after the lease expired the job reports lag %s (%v), want the full wait", lag, err)
	}
	if pending, err := s.Pending(ctx, ""); err != nil || pending != 1 {
		t.Errorf("after the lease expired %d jobs are pending (%v), want 1", pending, err)
	}
}

// TestAForgedJobIsRefused is an escalation the contract allowed.
//
// jobs.New builds a job from the Grant, so what it produces always matches. But
// Push takes a Job, and a Job is a struct anybody can fill in -- and the worker
// rebuilds the Grant from the stored row. So a Grant for invoice.view could
// enqueue a job whose action is invoice.delete, in another tenant, and the
// handler would run under a SystemGrant carrying both. Every Policy downstream
// would say yes, because the Grant is legitimate: it was minted from a row
// nobody checked. Found by audit.
func TestAForgedJobIsRefused(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()

	pushedBy := security.SystemGrant("invoice.view", tenant)

	t.Run("an action the Grant does not carry", func(t *testing.T) {
		j, err := jobs.New(pushedBy, "", "invoice.send", nil)
		if err != nil {
			t.Fatal(err)
		}
		j.Action = "invoice.delete"

		err = s.Push(ctx, pushedBy, j)
		if !errors.Is(err, jobs.ErrForged) {
			t.Fatalf("error = %v, want ErrForged", err)
		}
		// And the message says what to do about it.
		if !strings.Contains(err.Error(), "jobs.New") {
			t.Errorf("the error does not say how to build a job correctly: %v", err)
		}
	})

	t.Run("a tenant the Grant does not carry", func(t *testing.T) {
		j, err := jobs.New(pushedBy, "", "invoice.send", nil)
		if err != nil {
			t.Fatal(err)
		}
		j.TenantID = "22222222-2222-4222-8222-222222222222"

		if err := s.Push(ctx, pushedBy, j); !errors.Is(err, jobs.ErrForged) {
			t.Fatalf("error = %v, want ErrForged", err)
		}
	})

	t.Run("nothing was stored", func(t *testing.T) {
		if pending, err := s.Pending(ctx, ""); err != nil || pending != 0 {
			t.Fatalf("%d jobs were stored despite the refusals (%v)", pending, err)
		}
	})
}
