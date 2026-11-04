//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/keys"
	"github.com/yousufmohi/tasq/internal/testutil"
)

// A worker that crashed after XREADGROUP delivered an entry but before
// start.lua ran leaves the entry in its PEL while the hash still says
// pending. Recovery is driven by PEL idle time, so the entry is reclaimed
// and the job runs once, with its first attempt.
func TestReclaimUnstartedDelivery(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	deliver(t, rdb, q, "crashed-before-start") // in the PEL, never started
	if j, _ := c.GetJob(ctx, q, id); j.State != taskq.StatePending {
		t.Fatalf("state %s", j.State)
	}
	runWorker(t, newWorkerOpts(t, rdb, fastLeaseOpts(q, 1), func(context.Context, *taskq.Job) error { return nil }))
	j := waitState(t, c, q, id, taskq.StateSucceeded)
	if j.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (the crashed delivery never started)", j.Attempts)
	}
	assertDrained(t, rdb, q)
}

// With maxmemory reached under noeviction, enqueue fails loudly instead of
// Redis silently evicting jobs; every enqueue that returned success is
// still intact.
func TestEnqueueFailsLoudlyWhenRedisIsFull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r, err := testutil.StartRedis(ctx, "--maxmemory", "2mb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Terminate() })
	rdb := r.Client(t)
	c := taskq.NewClient(rdb)
	q := "full"

	payload := []byte(strings.Repeat("x", 1024))
	var acked []string
	var lastErr error
	for range 20000 {
		id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", Payload: payload})
		if err != nil {
			lastErr = err
			break
		}
		acked = append(acked, id)
	}
	if lastErr == nil || !strings.Contains(lastErr.Error(), "OOM") {
		t.Fatalf("expected an OOM error once memory is full, got %v after %d enqueues", lastErr, len(acked))
	}
	for _, id := range acked {
		if n := rdb.Exists(ctx, keys.Job(q, id)).Val(); n != 1 {
			t.Fatalf("acked job %s missing: Redis evicted data", id)
		}
	}
	if n := rdb.XLen(ctx, keys.Stream(q, keys.DefaultPriority)).Val(); int(n) != len(acked) {
		t.Fatalf("stream has %d entries for %d acked jobs (partial script write?)", n, len(acked))
	}
	// Redis raises OOM only on a script's first write (scriptVerifyOOM:
	// "otherwise we can't stop in the middle"), so the rejected enqueue
	// must have written nothing: no orphan job hash.
	var hashes int
	iter := rdb.Scan(ctx, 0, keys.Prefix(q)+"job:*", 1000).Iterator()
	for iter.Next(ctx) {
		hashes++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if hashes != len(acked) {
		t.Fatalf("%d job hashes for %d acked enqueues: the OOM-rejected script left a partial write", hashes, len(acked))
	}
	t.Logf("%d enqueues accepted, then: %v", len(acked), lastErr)
}

// Regression (audit): negative offsets and non-positive limits must be
// rejected, not passed to Redis where ZRANGE 0 -1 means "everything".
func TestDeadLetterBoundsValidated(t *testing.T) {
	t.Parallel()
	c := taskq.NewClient(testutil.Shared(t))
	ctx := context.Background()
	q := testutil.Queue(t)
	if _, err := c.ListDead(ctx, q, -1, 10); !errors.Is(err, taskq.ErrInvalidArgument) {
		t.Errorf("ListDead(offset -1) = %v", err)
	}
	if _, err := c.ListDead(ctx, q, 0, 0); !errors.Is(err, taskq.ErrInvalidArgument) {
		t.Errorf("ListDead(limit 0) = %v", err)
	}
	if _, err := c.ReplayDeadBatch(ctx, q, 0); !errors.Is(err, taskq.ErrInvalidArgument) {
		t.Errorf("ReplayDeadBatch(0) = %v", err)
	}
}
