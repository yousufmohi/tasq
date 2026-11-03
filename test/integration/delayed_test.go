//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/state"
	"github.com/yousufmo27/taskq/internal/testutil"
)

func TestDelayedJobRunsAfterDelay(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	const delay = 700 * time.Millisecond
	var ran atomic.Bool
	runWorker(t, newWorker(t, rdb, q, 1, func(context.Context, *taskq.Job) error {
		ran.Store(true)
		return nil
	}))
	id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "later", Delay: delay})
	if err != nil {
		t.Fatal(err)
	}
	j, _ := c.GetJob(ctx, q, id)
	if j.State != taskq.StateScheduled || j.StreamEntry != "" {
		t.Fatalf("delayed job: %+v", j)
	}
	if got := j.RunAt.Sub(j.CreatedAt); got != delay {
		t.Fatalf("run_at - created = %v, want %v", got, delay)
	}
	time.Sleep(delay / 2)
	if ran.Load() {
		t.Fatal("ran before its delay")
	}
	j = waitState(t, c, q, id, taskq.StateSucceeded)
	if j.StartedAt.Before(j.RunAt) {
		t.Fatalf("started %v before run_at %v", j.StartedAt, j.RunAt)
	}
	// Precision: run_at + promote interval (20ms in tests) + queue wait.
	if late := j.StartedAt.Sub(j.RunAt); late > 500*time.Millisecond {
		t.Fatalf("started %v after run_at", late)
	}
	assertDrained(t, rdb, q)
}

func TestEnqueueRunAtAndValidation(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if j, _ := c.GetJob(ctx, q, id); j.State != taskq.StateScheduled {
		t.Fatalf("state %s", j.State)
	}
	// A RunAt in the past runs immediately.
	id, _ = c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", RunAt: time.Now().Add(-time.Hour)})
	if j, _ := c.GetJob(ctx, q, id); j.State != taskq.StatePending {
		t.Fatalf("past RunAt: state %s", j.State)
	}
	if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", Delay: time.Second, RunAt: time.Now()}); err == nil {
		t.Fatal("accepted both Delay and RunAt")
	}
	if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", Delay: -time.Second}); err == nil {
		t.Fatal("accepted negative Delay")
	}
}

// Many promoters racing over a large backlog must move every job exactly
// once: one stream entry per job, and the hash points at it.
func TestConcurrentPromotersPromoteExactlyOnce(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	const n = 2000
	ids := make([]string, 0, n)
	for range n {
		id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", Delay: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	time.Sleep(20 * time.Millisecond) // all due

	var wg sync.WaitGroup
	var moved atomic.Int64
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := scripts.Promote.Run(ctx, rdb, []string{keys.Sched(q)},
					keys.Prefix(q), 100, state.SourcesCSV(state.Promote)).Slice()
				if err != nil {
					t.Error(err)
					return
				}
				m, _ := scripts.Int(res[0])
				s, _ := scripts.Int(res[1])
				moved.Add(m)
				if s == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if moved.Load() != n {
		t.Fatalf("moved %d, want %d", moved.Load(), n)
	}
	entries, err := rdb.XRange(ctx, keys.Stream(q, keys.DefaultPriority), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, e := range entries {
		id := e.Values["id"].(string)
		if prev, dup := seen[id]; dup {
			t.Fatalf("job %s has two entries: %s and %s", id, prev, e.ID)
		}
		seen[id] = e.ID
	}
	if len(seen) != n {
		t.Fatalf("%d jobs in stream, want %d", len(seen), n)
	}
	pipe := rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, n)
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, keys.Job(q, id), "state", "stream_entry")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		v := cmds[i].Val()
		if v[0] != "pending" || v[1] != seen[id] {
			t.Fatalf("job %s: state=%v entry=%v, stream entry %s", id, v[0], v[1], seen[id])
		}
	}
	if rdb.ZCard(ctx, keys.Sched(q)).Val() != 0 {
		t.Fatal("sched not empty")
	}
}

// Jobs scheduled for the future are untouched by promotion.
func TestPromoteLeavesFutureJobs(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	for i := range 10 {
		if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: fmt.Sprint(i), Delay: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := scripts.Promote.Run(ctx, rdb, []string{keys.Sched(q)},
		keys.Prefix(q), 100, state.SourcesCSV(state.Promote)).Slice()
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := scripts.Int(res[1]); s != 0 {
		t.Fatalf("scanned %d future jobs", s)
	}
	if rdb.ZCard(ctx, keys.Sched(q)).Val() != 10 {
		t.Fatal("future jobs removed")
	}
}
