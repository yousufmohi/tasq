//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/keys"
	"github.com/yousufmohi/tasq/internal/scripts"
	"github.com/yousufmohi/tasq/internal/testutil"
	"github.com/yousufmohi/tasq/worker"
)

func shutdownOpts(q string) worker.Options {
	o := fastLeaseOpts(q, 2)
	o.DrainTimeout = 200 * time.Millisecond
	o.ReleaseGrace = 200 * time.Millisecond
	return o
}

// runUntil starts w, waits for started, cancels it and returns how long Run
// took to return after cancellation.
func runUntil(t *testing.T, w *worker.Worker, started <-chan struct{}) time.Duration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}
	t0 := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
	return time.Since(t0)
}

func consumers(t *testing.T, rdb redis.UniversalClient, q string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, s := range keys.Streams(q) {
		cs, err := rdb.XInfoConsumers(context.Background(), s, keys.Group).Result()
		if err != nil {
			continue
		}
		for _, c := range cs {
			out[c.Name] += c.Pending
		}
	}
	return out
}

// A cooperative handler interrupted by shutdown has its job released back
// to the stream with the attempt refunded, immediately runnable elsewhere.
func TestShutdownReleasesInterruptedJob(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	started := make(chan struct{})
	w := newWorkerOpts(t, rdb, shutdownOpts(q), func(ctx context.Context, _ *taskq.Job) error {
		close(started)
		<-ctx.Done()
		return context.Cause(ctx)
	})
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "long"})
	took := runUntil(t, w, started)
	if took > 2*time.Second {
		t.Fatalf("shutdown took %v", took)
	}

	j, _ := c.GetJob(ctx, q, id)
	if j.State != taskq.StatePending || j.Attempts != 0 || j.LeaseToken != "" || j.LastError != "" {
		t.Fatalf("released job: %+v", j)
	}
	if n := rdb.XLen(ctx, keys.Stream(q, keys.DefaultPriority)).Val(); n != 1 {
		t.Fatalf("stream has %d entries, want 1 (old acked+deleted, new added)", n)
	}
	if _, ok := consumers(t, rdb, q)[w.Consumer()]; ok {
		t.Fatal("idle consumer not deleted on shutdown")
	}

	// It is picked up right away by the next worker, not after the
	// visibility timeout.
	t0 := time.Now()
	runWorker(t, newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), func(context.Context, *taskq.Job) error { return nil }))
	j = waitState(t, c, q, id, taskq.StateSucceeded)
	if j.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (refunded)", j.Attempts)
	}
	if time.Since(t0) > testVT {
		t.Fatalf("released job waited %v, as if reclaimed", time.Since(t0))
	}
}

// A handler that ignores cancellation cannot hold shutdown hostage: its job
// is released after DrainTimeout + ReleaseGrace, and its late completion is
// rejected by the fencing token.
func TestShutdownReleasesUncooperativeHandler(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	var returned atomic.Bool
	w := newWorkerOpts(t, rdb, shutdownOpts(q), func(context.Context, *taskq.Job) error {
		close(started)
		<-release // ignores ctx
		returned.Store(true)
		return nil
	})
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "stubborn"})
	took := runUntil(t, w, started)
	if took > 2*time.Second {
		t.Fatalf("shutdown took %v; an uncooperative handler blocked it", took)
	}
	j, _ := c.GetJob(ctx, q, id)
	if j.State != taskq.StatePending {
		t.Fatalf("state %s, want pending (released)", j.State)
	}

	close(release) // the zombie handler finishes and tries to complete
	testutil.WaitFor(t, 5*time.Second, "zombie returned", returned.Load)
	time.Sleep(100 * time.Millisecond)
	j, _ = c.GetJob(ctx, q, id)
	if j.State != taskq.StatePending {
		t.Fatalf("zombie's complete was accepted: state %s", j.State)
	}
}

// A clean shutdown with nothing in flight returns promptly and removes
// the consumer from every stream's group.
func TestShutdownIdleDeletesConsumer(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	w := newWorkerOpts(t, rdb, shutdownOpts(q), func(context.Context, *taskq.Job) error { return nil })
	stop := runWorker(t, w)
	testutil.WaitFor(t, 5*time.Second, "consumer registered", func() bool {
		_, ok := consumers(t, rdb, q)[w.Consumer()]
		return ok
	})
	stop()
	if _, ok := consumers(t, rdb, q)[w.Consumer()]; ok {
		t.Fatal("consumer still registered after shutdown")
	}
}

// delconsumer.lua must refuse to delete a consumer that owns pending
// entries: XGROUP DELCONSUMER would silently drop them.
func TestDelConsumerRefusesWhilePending(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	deliver(t, rdb, q, "busy")
	s := keys.Stream(q, keys.DefaultPriority)
	if st, _ := runStatus(t, scripts.DelConsumer, rdb, []string{s}, keys.Group, "busy"); st != "BUSY" {
		t.Fatalf("status %s, want BUSY", st)
	}
	if p := consumers(t, rdb, q)["busy"]; p != 1 {
		t.Fatalf("busy consumer pending = %d, want 1 (intact)", p)
	}
}

// Consumers left behind by crashed workers are garbage collected once idle
// past ConsumerTTL, unless they still own pending entries.
func TestConsumerGC(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	s := keys.Stream(q, keys.DefaultPriority)
	if err := rdb.XGroupCreateMkStream(ctx, s, keys.Group, "0").Err(); err != nil {
		t.Fatal(err)
	}
	rdb.XGroupCreateConsumer(ctx, s, keys.Group, "crashed-idle")
	c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", Priority: taskq.PriorityDefault})
	deliver(t, rdb, q, "crashed-busy") // owns a pending entry
	time.Sleep(150 * time.Millisecond)

	o := shutdownOpts(q)
	o.VisibilityTimeout = time.Hour // keep crashed-busy's entry from being reclaimed
	o.HeartbeatInterval = time.Minute
	o.ConsumerTTL = 100 * time.Millisecond
	o.ConsumerGCInterval = 50 * time.Millisecond
	runWorker(t, newWorkerOpts(t, rdb, o, func(context.Context, *taskq.Job) error { return nil }))

	testutil.WaitFor(t, 5*time.Second, "idle consumer collected", func() bool {
		_, ok := consumers(t, rdb, q)["crashed-idle"]
		return !ok
	})
	if p, ok := consumers(t, rdb, q)["crashed-busy"]; !ok || p != 1 {
		t.Fatalf("consumer with pending entries was collected (ok=%v pending=%d)", ok, p)
	}
}
