package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/arandu-io/framework/httpx"
	"github.com/arandu-io/framework/jobs"
	"github.com/arandu-io/framework/kernel"
)

// maxLag is how far behind the queue may fall before the health check fails.
//
// Five minutes on a worker that polls every second means nothing is draining --
// the process is down, every handler is failing, or nobody ever started
// `aru work`. The last one is the common case, and it is the one that otherwise
// shows up as a customer asking why they never got the email.
const maxLag = 5 * time.Minute

// hintLag is when a backlog stops being normal and starts being worth
// mentioning on an error page, well before the health check trips.
const hintLag = time.Minute

// Module brings the jobs table, and reports on the queue.
//
// It registers no routes and runs no worker: the worker is `aru work`, a
// separate process from the same image. What this module owns is the schema and
// the answer to "is anything draining this".
type Module struct {
	store *Store
	// queues are the ones the health check and the diagnosis look at. Empty
	// means the default queue only.
	queues []string
}

// NewModule returns the module.
//
// Name the queues the application actually uses. A queue nobody watches is a
// queue that can stop draining without anyone noticing, which is the failure
// this module exists to make visible.
func NewModule(s *Store, queues ...string) *Module {
	if len(queues) == 0 {
		queues = []string{jobs.DefaultQueue}
	}
	return &Module{store: s, queues: queues}
}

var (
	_ kernel.Module     = (*Module)(nil)
	_ kernel.Migratable = (*Module)(nil)
	_ kernel.Health     = (*Module)(nil)
	_ kernel.Diagnostic = (*Module)(nil)
)

// Name is the module identifier.
func (*Module) Name() string { return "queue" }

// Routes registers nothing.
func (*Module) Routes(*httpx.Router) {}

// Migrations returns the jobs table.
func (*Module) Migrations() []kernel.Migration {
	return []kernel.Migration{{
		ID: "2026_07_31_000010_create_jobs_table",
		// Portable types only: TEXT, INTEGER and TIMESTAMP mean the same thing
		// on SQLite, Postgres and MySQL.
		Up: `
CREATE TABLE jobs (
    id             VARCHAR(255) PRIMARY KEY,
    -- queue is indexed, so VARCHAR rather than TEXT: see data.KeyText.
    queue          VARCHAR(255) NOT NULL,
    name           TEXT NOT NULL,
    tenant_id      VARCHAR(255) NOT NULL,
    payload        TEXT NOT NULL,
    authorized_by  TEXT NOT NULL,
    action         TEXT NOT NULL,
    run_at         TIMESTAMP NOT NULL,
    reserved_until TIMESTAMP,
    attempts       INTEGER NOT NULL DEFAULT 0,
    failed_at      TIMESTAMP,
    last_error     TEXT
);

-- The reserve query filters on queue, failed_at and run_at and orders by
-- run_at. This index is that query.
CREATE INDEX idx_jobs_ready ON jobs (queue, failed_at, run_at);

-- The dead letter queue is read by the diagnosis, newest failure first.
CREATE INDEX idx_jobs_parked ON jobs (failed_at);
`,
		Down: `DROP TABLE jobs;`,
	}}
}

// Health fails when a queue stops draining.
func (m *Module) Health(ctx context.Context) error {
	for _, q := range m.queues {
		lag, err := m.store.Oldest(ctx, q)
		if err != nil {
			return err
		}
		if lag > maxLag {
			return fmt.Errorf("the oldest job on %s has been waiting %s -- is a worker running?",
				q, lag.Truncate(time.Second))
		}
	}
	return nil
}

// Diagnose reports a backlog and a dead letter queue, on the error page.
//
// Next to the failure somebody is already looking at, which is the moment they
// are most likely to act on it.
func (m *Module) Diagnose(ctx context.Context) []string {
	var out []string

	for _, q := range m.queues {
		lag, err := m.store.Oldest(ctx, q)
		if err != nil {
			continue
		}
		if lag > hintLag {
			pending, _ := m.store.Pending(ctx, q)
			out = append(out, fmt.Sprintf(
				"The %s queue has %d job(s) waiting, the oldest for %s. Is `aru work` running?",
				q, pending, lag.Truncate(time.Second)))
		}
	}

	if parked, err := m.store.Parked(ctx, 5); err == nil && len(parked) > 0 {
		out = append(out, fmt.Sprintf(
			"%d job(s) gave up after repeated failures, the most recent being %s: %s. They stay in the table until retried.",
			len(parked), parked[0].Name, parked[0].LastError))
	}
	return out
}
