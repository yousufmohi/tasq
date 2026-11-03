//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/state"
	"github.com/yousufmo27/taskq/internal/testutil"
	"github.com/yousufmo27/taskq/worker"
)

// runWorker starts w in the background and returns a stop function that
// cancels it and waits for Run to return.
func runWorker(t *testing.T, w *worker.Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("worker.Run: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("worker did not stop within 10s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func newWorker(t *testing.T, rdb redis.UniversalClient, q string, conc int, h worker.Handler) *worker.Worker {
	t.Helper()
	return newWorkerOpts(t, rdb, worker.Options{Queue: q, Concurrency: conc}, h)
}

// newWorkerOpts applies fast test defaults to any option left zero.
func newWorkerOpts(t *testing.T, rdb redis.UniversalClient, o worker.Options, h worker.Handler) *worker.Worker {
	t.Helper()
	if o.Block == 0 {
		o.Block = 100 * time.Millisecond
	}
	if o.RetryBase == 0 {
		o.RetryBase = 10 * time.Millisecond
	}
	if o.RetryMax == 0 {
		o.RetryMax = 50 * time.Millisecond
	}
	if o.PromoteInterval == 0 {
		o.PromoteInterval = 20 * time.Millisecond
	}
	w, err := worker.New(rdb, o, h)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// assertDrained checks that no stream entry and no pending entry remain.
func assertDrained(t *testing.T, rdb redis.UniversalClient, q string) {
	t.Helper()
	ctx := context.Background()
	s := keys.Stream(q, keys.DefaultPriority)
	if n := rdb.XLen(ctx, s).Val(); n != 0 {
		t.Errorf("stream still has %d entries", n)
	}
	p, err := rdb.XPending(ctx, s, keys.Group).Result()
	if err != nil {
		t.Fatal(err)
	}
	if p.Count != 0 {
		t.Errorf("PEL still has %d entries", p.Count)
	}
}

// The walking skeleton: enqueue → process → ack, with jobs enqueued both
// before the consumer group exists and while workers are running.
func TestEnqueueProcessAck(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	const n = 200
	var ids []string
	enqueue := func(k int) {
		for i := range k {
			id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "echo", Payload: []byte(fmt.Sprint(i))})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
	}
	enqueue(n / 2) // before any worker: the group does not exist yet

	var calls sync.Map // id -> *int32
	h := func(_ context.Context, j *taskq.Job) error {
		v, _ := calls.LoadOrStore(j.ID, new(int32))
		atomic.AddInt32(v.(*int32), 1)
		return nil
	}
	stop1 := runWorker(t, newWorker(t, rdb, q, 8, h))
	stop2 := runWorker(t, newWorker(t, rdb, q, 8, h))
	enqueue(n / 2)

	testutil.WaitFor(t, 30*time.Second, "all jobs succeeded", func() bool {
		for _, id := range ids {
			j, err := c.GetJob(ctx, q, id)
			if err != nil || j.State != taskq.StateSucceeded {
				return false
			}
		}
		return true
	})
	stop1()
	stop2()

	for _, id := range ids {
		v, ok := calls.Load(id)
		if !ok {
			t.Fatalf("job %s never ran", id)
		}
		if got := atomic.LoadInt32(v.(*int32)); got != 1 {
			// No failures were injected, so a duplicate here is a bug.
			t.Errorf("job %s ran %d times", id, got)
		}
		j, _ := c.GetJob(ctx, q, id)
		if j.Attempts != 1 || j.FinishedAt.Before(j.StartedAt) || j.StartedAt.Before(j.CreatedAt) {
			t.Errorf("bad record: %+v", j)
		}
		if ttl := rdb.PTTL(ctx, keys.Job(q, id)).Val(); ttl <= 0 || ttl > 24*time.Hour {
			t.Errorf("succeeded job TTL = %v, want (0, 24h]", ttl)
		}
	}
	assertDrained(t, rdb, q)
}

// Cancelling Run must let in-flight handlers finish and be acknowledged.
func TestRunDrainsInFlightOnCancel(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	w := newWorker(t, rdb, q, 1, func(context.Context, *taskq.Job) error {
		close(started)
		<-release
		return nil
	})
	id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while a handler was still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	j, err := c.GetJob(ctx, q, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != taskq.StateSucceeded {
		t.Fatalf("state = %s, want succeeded", j.State)
	}
	assertDrained(t, rdb, q)
}

// A panicking handler must not kill the worker; the panic is a job failure.
func TestHandlerPanicIsContained(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	boom, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "boom", MaxAttempts: 1})
	ok, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "ok"})
	runWorker(t, newWorker(t, rdb, q, 1, func(_ context.Context, j *taskq.Job) error {
		if j.Type == "boom" {
			panic("kaboom")
		}
		return nil
	}))
	testutil.WaitFor(t, 10*time.Second, "ok job succeeded", func() bool {
		j, err := c.GetJob(ctx, q, ok)
		return err == nil && j.State == taskq.StateSucceeded
	})
	testutil.WaitFor(t, 10*time.Second, "panicked job dead-lettered", func() bool {
		j, err := c.GetJob(ctx, q, boom)
		return err == nil && j.State == taskq.StateDead
	})
	j, _ := c.GetJob(ctx, q, boom)
	if !strings.Contains(j.LastError, "kaboom") {
		t.Fatalf("last_error = %q, want the panic message", j.LastError)
	}
}

// Delivering an entry whose job hash is gone acks and deletes the entry
// without calling the handler.
func TestStartSkipsMissingJob(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()

	s := keys.Stream(q, keys.DefaultPriority)
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: []string{"id", "ghost"}}).Err(); err != nil {
		t.Fatal(err)
	}
	var called atomic.Bool
	runWorker(t, newWorker(t, rdb, q, 1, func(context.Context, *taskq.Job) error {
		called.Store(true)
		return nil
	}))
	testutil.WaitFor(t, 10*time.Second, "ghost entry removed", func() bool {
		return rdb.XLen(ctx, s).Val() == 0
	})
	if called.Load() {
		t.Fatal("handler called for a missing job")
	}
	assertDrained(t, rdb, q)
}

// deliver reads one entry for consumer from the queue's stream.
func deliver(t *testing.T, rdb redis.UniversalClient, q, consumer string) redis.XMessage {
	t.Helper()
	ctx := context.Background()
	s := keys.Stream(q, keys.DefaultPriority)
	err := rdb.XGroupCreateMkStream(ctx, s, keys.Group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		t.Fatal(err)
	}
	res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: keys.Group, Consumer: consumer, Streams: []string{s, ">"}, Count: 1, Block: -1,
	}).Result()
	if err != nil || len(res) != 1 || len(res[0].Messages) != 1 {
		t.Fatalf("deliver: %v %v", res, err)
	}
	return res[0].Messages[0]
}

func runStatus(t *testing.T, s *redis.Script, rdb redis.UniversalClient, keyz []string, args ...any) (string, []any) {
	t.Helper()
	res, err := s.Run(context.Background(), rdb, keyz, args...).Result()
	if err != nil {
		t.Fatal(err)
	}
	st, rest, err := scripts.Status(res)
	if err != nil {
		t.Fatal(err)
	}
	return st, rest
}

// Fencing: once a second start rotates the lease token (as after a reclaim),
// the first holder's complete is rejected and writes nothing.
func TestCompleteIsFencedByLeaseToken(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	if err != nil {
		t.Fatal(err)
	}
	msg := deliver(t, rdb, q, "A")
	k := []string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority)}
	startSrc := state.SourcesCSV(state.Start)
	completeSrc := state.SourcesCSV(state.Complete)

	if st, _ := runStatus(t, scripts.Start, rdb, k, keys.Group, msg.ID, "A", "tokA", startSrc); st != "OK" {
		t.Fatalf("start A: %s", st)
	}
	if st, _ := runStatus(t, scripts.Start, rdb, k, keys.Group, msg.ID, "B", "tokB", startSrc); st != "OK" {
		t.Fatalf("start B (reclaim): %s", st)
	}

	if st, _ := runStatus(t, scripts.Complete, rdb, k, keys.Group, msg.ID, "tokA", 0, completeSrc); st != "LEASE_LOST" {
		t.Fatalf("stale complete: %s, want LEASE_LOST", st)
	}
	j, _ := c.GetJob(ctx, q, id)
	if j.State != taskq.StateActive || j.LeaseOwner != "B" || j.Attempts != 2 {
		t.Fatalf("stale complete wrote state: %+v", j)
	}
	if p := rdb.XPending(ctx, k[1], keys.Group).Val(); p.Count != 1 {
		t.Fatalf("stale complete acked the entry: PEL=%d", p.Count)
	}

	if st, _ := runStatus(t, scripts.Complete, rdb, k, keys.Group, msg.ID, "tokB", 0, completeSrc); st != "OK" {
		t.Fatalf("current complete: %s", st)
	}
	// A terminal job can't be completed again.
	if st, _ := runStatus(t, scripts.Complete, rdb, k, keys.Group, msg.ID, "tokB", 0, completeSrc); st != "INVALID_STATE" {
		t.Fatalf("second complete: %s, want INVALID_STATE", st)
	}
	// retention 0 keeps the hash forever.
	if ttl := rdb.PTTL(ctx, k[0]).Val(); ttl != -1 {
		t.Fatalf("PTTL = %v, want -1 (no expiry)", ttl)
	}
	assertDrained(t, rdb, q)
}

// Only the entry a job's hash points at may start it; any other entry for
// the same job is acked and dropped.
func TestStartRejectsStaleEntry(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	s := keys.Stream(q, keys.DefaultPriority)
	// A second, rogue entry for the same job.
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: []string{"id", id}}).Err(); err != nil {
		t.Fatal(err)
	}
	first := deliver(t, rdb, q, "A")
	second := deliver(t, rdb, q, "A")
	k := []string{keys.Job(q, id), s}
	src := state.SourcesCSV(state.Start)

	if st, rest := runStatus(t, scripts.Start, rdb, k, keys.Group, second.ID, "A", "t2", src); st != "SKIP" || rest[0] != "stale_entry" {
		t.Fatalf("rogue entry: %s %v", st, rest)
	}
	if st, _ := runStatus(t, scripts.Start, rdb, k, keys.Group, first.ID, "A", "t1", src); st != "OK" {
		t.Fatalf("real entry: %s", st)
	}
	if n := rdb.XLen(ctx, s).Val(); n != 1 {
		t.Fatalf("XLEN = %d, want 1 (rogue entry deleted)", n)
	}
}

// Terminal jobs can't be started again; the entry is acked and dropped.
func TestStartSkipsTerminalJob(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	msg := deliver(t, rdb, q, "A")
	rdb.HSet(ctx, keys.Job(q, id), "state", "succeeded")
	k := []string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority)}
	st, rest := runStatus(t, scripts.Start, rdb, k, keys.Group, msg.ID, "A", "t", state.SourcesCSV(state.Start))
	if st != "SKIP" || rest[0] != "succeeded" {
		t.Fatalf("got %s %v", st, rest)
	}
	j, _ := c.GetJob(ctx, q, id)
	if j.Attempts != 0 {
		t.Fatalf("skipped start incremented attempts to %d", j.Attempts)
	}
	assertDrained(t, rdb, q)
}

// After SCRIPT FLUSH (what a Redis restart does to the script cache),
// EVALSHA fails with NOSCRIPT and go-redis must fall back to EVAL.
func TestScriptsSurviveScriptFlush(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)
	if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"}); err != nil {
		t.Fatalf("enqueue after SCRIPT FLUSH: %v", err)
	}
}

// A worker must refuse to start against a Redis that may evict keys.
func TestWorkerRefusesEvictingRedis(t *testing.T) {
	t.Parallel()
	r, err := testutil.StartRedis(context.Background(), "--maxmemory-policy", "allkeys-lru")
	if err != nil {
		t.Fatalf("integration tests need Docker: %v", err)
	}
	t.Cleanup(func() { _ = r.Terminate() })
	w := newWorker(t, r.Client(t), "q", 1, func(context.Context, *taskq.Job) error { return nil })
	err = w.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "noeviction") {
		t.Fatalf("Run = %v, want noeviction error", err)
	}
}

func TestEnqueueValidation(t *testing.T) {
	t.Parallel()
	c := taskq.NewClient(testutil.Shared(t))
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, "bad:queue", taskq.EnqueueOptions{Type: "x"}); err == nil {
		t.Error("accepted bad queue name")
	}
	if _, err := c.Enqueue(ctx, "q", taskq.EnqueueOptions{}); err == nil {
		t.Error("accepted empty type")
	}
	if _, err := c.Enqueue(ctx, "q", taskq.EnqueueOptions{Type: "x", MaxAttempts: -1}); err == nil {
		t.Error("accepted negative MaxAttempts")
	}
	if _, err := c.GetJob(ctx, "q", job.NewID()); !errors.Is(err, taskq.ErrNotFound) {
		t.Errorf("GetJob missing = %v, want ErrNotFound", err)
	}
}
