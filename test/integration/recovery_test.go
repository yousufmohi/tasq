//go:build integration

package integration

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/testutil"
	"github.com/yousufmo27/taskq/worker"
)

const (
	testVT = 600 * time.Millisecond
	testHB = 150 * time.Millisecond
)

func fastLeaseOpts(q string, conc int) worker.Options {
	return worker.Options{
		Queue: q, Concurrency: conc,
		VisibilityTimeout: testVT, HeartbeatInterval: testHB, ReclaimInterval: 100 * time.Millisecond,
	}
}

// A job that runs for several visibility timeouts must not be reclaimed by
// an idle peer while its heartbeats succeed.
func TestHeartbeatKeepsLongJobAlive(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	var calls atomic.Int32
	h := func(ctx context.Context, _ *taskq.Job) error {
		calls.Add(1)
		select {
		case <-time.After(4 * testVT):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	runWorker(t, newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), h))
	runWorker(t, newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), h)) // idle peer that would reclaim
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "long"})
	j := waitState(t, c, q, id, taskq.StateSucceeded)
	if j.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("attempts=%d calls=%d, want 1/1 (no false reclaim)", j.Attempts, calls.Load())
	}
}

// A job abandoned mid-run by a consumer that stopped heartbeating (a
// crashed worker) is reclaimed after the visibility timeout and completed.
func TestReclaimAbandonedJob(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	msg := deliver(t, rdb, q, "crashed")
	if st, _ := startJob(t, rdb, q, id, msg.ID, "crashed", "dead-token"); st != "OK" {
		t.Fatal(st)
	}
	abandoned := time.Now()

	w := newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), func(context.Context, *taskq.Job) error { return nil })
	runWorker(t, w)
	j := waitState(t, c, q, id, taskq.StateSucceeded)
	if j.Attempts != 2 || j.LeaseOwner != w.Consumer() {
		t.Fatalf("attempts=%d owner=%s, want 2 and the reclaiming worker", j.Attempts, j.LeaseOwner)
	}
	if took := time.Since(abandoned); took < testVT {
		t.Fatalf("reclaimed after %v, before the visibility timeout %v", took, testVT)
	}
	assertDrained(t, rdb, q)
}

// A job that kills its worker on every attempt must end up dead once its
// attempts are used up, not crash-loop forever.
func TestPoisonPillIsDeadLettered(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "poison", MaxAttempts: 2})
	msg := deliver(t, rdb, q, "crash1")
	startJob(t, rdb, q, id, msg.ID, "crash1", "t1") // attempt 1, worker dies
	claimTo(t, rdb, q, msg.ID, "crash2")
	startJob(t, rdb, q, id, msg.ID, "crash2", "t2") // attempt 2, worker dies

	var called atomic.Bool
	runWorker(t, newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), func(context.Context, *taskq.Job) error {
		called.Store(true)
		return nil
	}))
	j := waitState(t, c, q, id, taskq.StateDead)
	if called.Load() {
		t.Fatal("handler ran a job whose attempts were exhausted")
	}
	if j.Attempts != 2 || !strings.Contains(j.LastError, "attempts exhausted") {
		t.Fatalf("got %+v", j)
	}
	if rdb.ZScore(ctx, keys.Dead(q), id).Err() != nil {
		t.Fatal("not indexed in the dead set")
	}
	assertDrained(t, rdb, q)
}

// The heartbeat must not steal back an entry another consumer reclaimed.
func TestHeartbeatDoesNotStealReclaimedEntry(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	msg := deliver(t, rdb, q, "A")
	startJob(t, rdb, q, id, msg.ID, "A", "tokA")
	claimTo(t, rdb, q, msg.ID, "B")
	k := []string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority)}

	st, rest := runStatus(t, scripts.Heartbeat, rdb, k, keys.Group, msg.ID, "A", "tokA", 1000)
	if st != "LEASE_LOST" || rest[0] != "not_owner" {
		t.Fatalf("heartbeat by old owner: %s %v", st, rest)
	}
	p := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: k[1], Group: keys.Group, Start: "-", End: "+", Count: 10}).Val()
	if len(p) != 1 || p[0].Consumer != "B" {
		t.Fatalf("PEL owner changed: %+v", p)
	}
	// B has claimed but not started: the token is still A's, yet A is not
	// the PEL owner, so A still may not renew.
	startJob(t, rdb, q, id, msg.ID, "B", "tokB")
	st, rest = runStatus(t, scripts.Heartbeat, rdb, k, keys.Group, msg.ID, "B", "tokA", 1000)
	if st != "LEASE_LOST" || rest[0] != "token" {
		t.Fatalf("heartbeat with stale token: %s %v", st, rest)
	}
	if st, _ := runStatus(t, scripts.Heartbeat, rdb, k, keys.Group, msg.ID, "B", "tokB", 1000); st != "OK" {
		t.Fatalf("heartbeat by owner: %s", st)
	}
}

// When another worker takes the job over, the old worker's heartbeat detects
// it and cancels the handler's context with a lease-lost cause, and the old
// worker writes nothing.
func TestLeaseLossCancelsHandler(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	started := make(chan string, 1)
	cancelled := make(chan error, 1)
	w := newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), func(ctx context.Context, j *taskq.Job) error {
		started <- j.StreamEntry
		<-ctx.Done()
		cancelled <- context.Cause(ctx)
		return ctx.Err()
	})
	runWorker(t, w)
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	eid := <-started

	claimTo(t, rdb, q, eid, "thief")
	startJob(t, rdb, q, id, eid, "thief", "thief-token")
	select {
	case cause := <-cancelled:
		if cause == nil || !strings.Contains(cause.Error(), "lease lost") {
			t.Fatalf("cause = %v", cause)
		}
	case <-time.After(5 * testHB):
		t.Fatal("handler not cancelled after lease loss")
	}
	time.Sleep(2 * testHB)
	j, _ := c.GetJob(ctx, q, id)
	if j.State != taskq.StateActive || j.LeaseOwner != "thief" || j.LastError != "" {
		t.Fatalf("old worker wrote after losing its lease: %+v", j)
	}
}

func TestInvalidLeaseOptionsRejected(t *testing.T) {
	_, err := worker.New(nil, worker.Options{Queue: "q", VisibilityTimeout: time.Second, HeartbeatInterval: 600 * time.Millisecond},
		func(context.Context, *taskq.Job) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "HeartbeatInterval") {
		t.Fatalf("got %v", err)
	}
}
