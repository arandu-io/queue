package kv_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arandu-io/framework/jobs"
	"github.com/arandu-io/framework/security"
	kvqueue "github.com/arandu-io/queue/kv"
)

// The RESP driver against a real server. Everything it claims is a claim about
// the protocol -- that ZRem is the claim and exactly one worker wins it -- and a
// fake that implements the happy path proves none of it.
//
// Without KV_ADDRESS these skip. CI sets it.

const tenant = "11111111-1111-4111-8111-111111111111"

func grant() security.Grant { return security.SystemGrant("invoice.send", tenant) }

func store(t *testing.T) *kvqueue.Store {
	t.Helper()

	address := os.Getenv("KV_ADDRESS")
	if address == "" {
		t.Skip("KV_ADDRESS is not set: start a RESP server and set it, e.g. KV_ADDRESS=127.0.0.1:6379")
	}

	// A prefix per test AND per run, or a second run inside the same minute
	// inherits the first one's jobs.
	prefix := "test-" + strings.ReplaceAll(t.Name(), "/", "-") + "-" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	s := kvqueue.New(kvqueue.Options{Address: address, Prefix: prefix})
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("connecting to %s: %v", address, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func push(t *testing.T, s *kvqueue.Store, name string) jobs.Job {
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
	s := store(t)
	ctx := context.Background()
	pushed := push(t, s, "invoice.send")

	reserved, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(reserved) != 1 || reserved[0].ID != pushed.ID {
		t.Fatalf("reserved %+v", reserved)
	}
	if reserved[0].TenantID != tenant || reserved[0].Action != "invoice.send" {
		t.Errorf("the Grant did not survive: %+v", reserved[0])
	}
	if reserved[0].Attempts != 1 {
		t.Errorf("attempts = %d on the first delivery", reserved[0].Attempts)
	}
}

// TestOnlyOneWorkerGetsTheJob is the claim that makes more than one worker
// safe, and it is about ZRem being atomic -- which only a real server settles.
func TestOnlyOneWorkerGetsTheJob(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	first, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("Reserve: %v, %d", err, len(first))
	}
	second, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("a second worker got %d jobs that were already taken", len(second))
	}
}

// TestAnExpiredLeaseComesBack is what makes a crashed worker recoverable.
func TestAnExpiredLeaseComesBack(t *testing.T) {
	s := store(t)
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
		t.Fatal("the job did not come back after the lease expired")
	}
	if again[0].Attempts != 2 {
		t.Errorf("attempts = %d on the second delivery", again[0].Attempts)
	}
}

func TestAckRemovesTheJob(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, 50*time.Millisecond)
	if err := s.Ack(ctx, reserved[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Past the lease: an acknowledged job never comes back.
	time.Sleep(120 * time.Millisecond)
	again, _ := s.Reserve(ctx, "", 10, time.Minute)
	if len(again) != 0 {
		t.Fatal("an acknowledged job came back when its lease expired")
	}
	if pending, err := s.Pending(ctx, ""); err != nil || pending != 0 {
		t.Errorf("pending = %d (%v)", pending, err)
	}
}

func TestFailSchedulesTheRetry(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	err := s.Fail(ctx, reserved[0], errors.New("the broker refused"), time.Now().Add(time.Hour), false)
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}

	soon, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(soon) != 0 {
		t.Fatal("a job scheduled for the future was reserved now")
	}
	if lag, err := s.Oldest(ctx, ""); err != nil || lag != 0 {
		t.Errorf("a future job reports lag %s (%v)", lag, err)
	}
}

func TestAParkedJobDoesNotBlockTheQueue(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	if err := s.Fail(ctx, reserved[0], errors.New("the payload is malformed"), time.Time{}, true); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	parked, err := s.Parked(ctx, 10)
	if err != nil || len(parked) != 1 {
		t.Fatalf("Parked: %v, %d", err, len(parked))
	}
	if parked[0].LastError != "the payload is malformed" {
		t.Errorf("the parked job does not say why: %q", parked[0].LastError)
	}

	push(t, s, "report.monthly")
	next, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil || len(next) != 1 || next[0].Name != "report.monthly" {
		t.Fatalf("the parked job blocked the queue: %+v (%v)", next, err)
	}
}

func TestRetryBringsAParkedJobBack(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	push(t, s, "invoice.send")

	reserved, _ := s.Reserve(ctx, "", 10, time.Minute)
	_ = s.Fail(ctx, reserved[0], errors.New("the consumer was down"), time.Time{}, true)

	parked, _ := s.Parked(ctx, 10)
	if err := s.Retry(ctx, parked[0].ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	back, err := s.Reserve(ctx, "", 10, time.Minute)
	if err != nil || len(back) != 1 {
		t.Fatalf("the retried job did not come back: %v, %d", err, len(back))
	}
	if back[0].Attempts != 1 {
		t.Errorf("attempts = %d after a retry, want the count to have restarted", back[0].Attempts)
	}
}

func TestQueuesAreSeparate(t *testing.T) {
	s := store(t)
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

func TestAJobWithoutATenantIsRefused(t *testing.T) {
	s := store(t)

	err := s.Push(context.Background(), security.SystemGrant("invoice.send", ""),
		jobs.Job{ID: "j-1", Name: "invoice.send", Payload: []byte("{}")})
	if !errors.Is(err, jobs.ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
}

// TestNothingUsesLuaOrModules: the moment it does, Dragonfly stops being a
// drop-in replacement and four products become one (doc 11).
func TestNothingUsesLuaOrModules(t *testing.T) {
	source, err := os.ReadFile("kv.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{".Eval(", ".EvalSha(", ".JSONSet(", ".FT_"} {
		if strings.Contains(string(source), forbidden) {
			t.Errorf("the driver calls %s", forbidden)
		}
	}
}
