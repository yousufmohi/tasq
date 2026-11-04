//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/keys"
	"github.com/yousufmohi/tasq/internal/scripts"
	"github.com/yousufmohi/tasq/internal/state"
	"github.com/yousufmohi/tasq/internal/testutil"
	"github.com/yousufmohi/tasq/worker"
)

func waitState(t *testing.T, c *taskq.Client, q, id string, want taskq.State) *taskq.Job {
	t.Helper()
	var j *taskq.Job
	testutil.WaitFor(t, 15*time.Second, fmt.Sprintf("job %s %s", id, want), func() bool {
		var err error
		j, err = c.GetJob(context.Background(), q, id)
		return err == nil && j.State == want
	})
	return j
}

func TestRetryThenSucceed(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	var calls atomic.Int32
	runWorker(t, newWorker(t, rdb, q, 2, func(context.Context, *taskq.Job) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}))
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "flaky", MaxAttempts: 5})
	j := waitState(t, c, q, id, taskq.StateSucceeded)
	if j.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3/3", j.Attempts, calls.Load())
	}
	if j.LastError != "transient" {
		t.Fatalf("last_error = %q", j.LastError)
	}
	if n := rdb.ZCard(ctx, keys.Sched(q)).Val(); n != 0 {
		t.Fatalf("sched still holds %d", n)
	}
	assertDrained(t, rdb, q)
}

func TestExhaustedAttemptsDeadLetter(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	var calls atomic.Int32
	runWorker(t, newWorker(t, rdb, q, 1, func(context.Context, *taskq.Job) error {
		calls.Add(1)
		return errors.New("always")
	}))
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "bad", MaxAttempts: 3})
	j := waitState(t, c, q, id, taskq.StateDead)
	if j.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3/3", j.Attempts, calls.Load())
	}
	if j.FinishedAt.IsZero() || j.LastError != "always" {
		t.Fatalf("bad dead record: %+v", j)
	}
	dead, err := c.ListDead(ctx, q, 0, 10)
	if err != nil || len(dead) != 1 || dead[0].ID != id {
		t.Fatalf("ListDead = %v, %v", dead, err)
	}
	if ttl := rdb.PTTL(ctx, keys.Job(q, id)).Val(); ttl != -1 {
		t.Fatalf("dead job must not expire, PTTL=%v", ttl)
	}
	assertDrained(t, rdb, q)
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	runWorker(t, newWorker(t, rdb, q, 1, func(context.Context, *taskq.Job) error {
		return fmt.Errorf("decode: %w", taskq.Permanent(errors.New("bad json")))
	}))
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", MaxAttempts: 10})
	if j := waitState(t, c, q, id, taskq.StateDead); j.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", j.Attempts)
	}
}

func TestJobTimeoutCountsAsFailure(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	runWorker(t, newWorker(t, rdb, q, 1, func(ctx context.Context, _ *taskq.Job) error {
		<-ctx.Done() // cooperative handler
		return ctx.Err()
	}))
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", MaxAttempts: 2, Timeout: 50 * time.Millisecond})
	j := waitState(t, c, q, id, taskq.StateDead)
	if !strings.Contains(j.LastError, "deadline exceeded") || j.Timeout != 50*time.Millisecond {
		t.Fatalf("got %+v", j)
	}
}

func TestRetryIsScheduledWithBackoff(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	w := newWorkerOpts(t, rdb, worker.Options{Queue: q, RetryBase: time.Hour, RetryMax: time.Hour},
		func(context.Context, *taskq.Job) error { return errors.New("x") })
	runWorker(t, w)
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	j := waitState(t, c, q, id, taskq.StateRetry)
	score, err := rdb.ZScore(ctx, keys.Sched(q), id).Result()
	if err != nil {
		t.Fatal(err)
	}
	if int64(score) != j.RunAt.UnixMilli() {
		t.Fatalf("sched score %v != run_at %v", int64(score), j.RunAt.UnixMilli())
	}
	// Full jitter over [0, 1h] for attempt 1.
	if d := j.RunAt.Sub(j.StartedAt); d < 0 || d > time.Hour+time.Second {
		t.Fatalf("retry delay %v outside [0,1h]", d)
	}
	assertDrained(t, rdb, q)
}

func TestReplayDead(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var calls atomic.Int32
	runWorker(t, newWorker(t, rdb, q, 1, func(context.Context, *taskq.Job) error {
		calls.Add(1)
		if fail.Load() {
			return taskq.Permanent(errors.New("downstream down"))
		}
		return nil
	}))
	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	waitState(t, c, q, id, taskq.StateDead)
	fail.Store(false)

	// Concurrent replays: exactly one may win.
	var ok, invalid atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := c.ReplayDead(ctx, q, id); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, taskq.ErrInvalidState):
				invalid.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || invalid.Load() != 7 {
		t.Fatalf("replays ok=%d invalid=%d, want 1/7", ok.Load(), invalid.Load())
	}
	j := waitState(t, c, q, id, taskq.StateSucceeded)
	if j.Attempts != 1 || calls.Load() != 2 {
		t.Fatalf("attempts=%d (reset on replay) calls=%d", j.Attempts, calls.Load())
	}
	if n := rdb.ZCard(ctx, keys.Dead(q)).Val(); n != 0 {
		t.Fatalf("dead set still holds %d", n)
	}
	if err := c.ReplayDead(ctx, q, "nope"); !errors.Is(err, taskq.ErrNotFound) {
		t.Fatalf("replay missing = %v", err)
	}
	assertDrained(t, rdb, q)
}

func TestReplayBatchAndPurge(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	// Build dead jobs directly through the scripts, without a worker.
	var ids []string
	for i := range 250 {
		id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", MaxAttempts: 1})
		ids = append(ids, id)
		msg := deliver(t, rdb, q, "A")
		k := []string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority)}
		tok := fmt.Sprint("t", i)
		if st, _ := startJob(t, rdb, q, id, msg.ID, "A", tok); st != "OK" {
			t.Fatal(st)
		}
		k = append(k, keys.Sched(q), keys.Dead(q))
		if st, _ := runStatus(t, scripts.Fail, rdb, k, keys.Group, msg.ID, tok, "e", 0, "0", state.SourcesCSV(state.Fail)); st != "DEAD" {
			t.Fatal(st)
		}
	}
	n, err := c.ReplayDeadBatch(ctx, q, 50)
	if err != nil || n != 50 {
		t.Fatalf("replayed %d, %v", n, err)
	}
	if got := rdb.XLen(ctx, keys.Stream(q, keys.DefaultPriority)).Val(); got != 50 {
		t.Fatalf("stream has %d entries, want 50", got)
	}
	// Purge everything left (more than one 100-item batch).
	purged, err := c.PurgeDead(ctx, q, time.Now().Add(time.Hour))
	if err != nil || purged != 200 {
		t.Fatalf("purged %d, %v", purged, err)
	}
	if rdb.ZCard(ctx, keys.Dead(q)).Val() != 0 {
		t.Fatal("dead set not empty")
	}
	if rdb.Exists(ctx, keys.Job(q, ids[249])).Val() != 0 {
		t.Fatal("purged hash still exists")
	}
	// Replayed jobs survive the purge.
	if j, err := c.GetJob(ctx, q, ids[0]); err != nil || j.State != taskq.StatePending {
		t.Fatalf("replayed job: %+v %v", j, err)
	}
}

// snapshot captures every key a script could write for one job.
type snapshot struct {
	Hash              map[string]string
	XLen, PEL         int64
	SchedLen, DeadLen int64
}

func snap(t *testing.T, rdb redis.UniversalClient, q, id string) snapshot {
	t.Helper()
	ctx := context.Background()
	s := keys.Stream(q, keys.DefaultPriority)
	p, _ := rdb.XPending(ctx, s, keys.Group).Result()
	var pel int64
	if p != nil {
		pel = p.Count
	}
	return snapshot{
		Hash:     rdb.HGetAll(ctx, keys.Job(q, id)).Val(),
		XLen:     rdb.XLen(ctx, s).Val(),
		PEL:      pel,
		SchedLen: rdb.ZCard(ctx, keys.Sched(q)).Val(),
		DeadLen:  rdb.ZCard(ctx, keys.Dead(q)).Val(),
	}
}

// Every script must reject every state the transition table does not allow,
// and must write nothing when it does.
func TestIllegalTransitionsWriteNothing(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	ctx := context.Background()

	type call func(q, id, eid string) string
	scriptsUnderTest := map[state.Event]call{
		state.Complete: func(q, id, eid string) string {
			st, _ := runStatus(t, scripts.Complete, rdb,
				[]string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority)},
				keys.Group, eid, "tok", 0, state.SourcesCSV(state.Complete))
			return st
		},
		state.Fail: func(q, id, eid string) string {
			st, _ := runStatus(t, scripts.Fail, rdb,
				[]string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority), keys.Sched(q), keys.Dead(q)},
				keys.Group, eid, "tok", "e", 0, "0", state.SourcesCSV(state.Fail))
			return st
		},
		state.Replay: func(q, id, _ string) string {
			st, _ := runStatus(t, scripts.Replay, rdb,
				[]string{keys.Job(q, id), keys.Dead(q)},
				keys.Prefix(q), state.SourcesCSV(state.Replay), id)
			return st
		},
	}
	for ev, run := range scriptsUnderTest {
		for _, from := range state.All {
			if state.Allowed(from, ev) {
				continue
			}
			t.Run(fmt.Sprintf("%s_from_%s", ev, from), func(t *testing.T) {
				q := testutil.Queue(t)
				c := taskq.NewClient(rdb)
				id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
				msg := deliver(t, rdb, q, "A")
				rdb.HSet(ctx, keys.Job(q, id), "state", string(from), "lease_token", "tok")
				before := snap(t, rdb, q, id)
				st := run(q, id, msg.ID)
				if st != "INVALID_STATE" {
					t.Fatalf("status %s, want INVALID_STATE", st)
				}
				if after := snap(t, rdb, q, id); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected call wrote:\nbefore %+v\nafter  %+v", before, after)
				}
			})
		}
	}
}

// promote must drop a sched member whose job is not scheduled/retry, without
// creating a stream entry.
func TestPromoteIgnoresIllegalStates(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	rdb.HSet(ctx, keys.Job(q, id), "state", "active")
	rdb.ZAdd(ctx, keys.Sched(q), redis.Z{Score: 1, Member: id})
	xlen := rdb.XLen(ctx, keys.Stream(q, keys.DefaultPriority)).Val()
	res, err := scripts.Promote.Run(ctx, rdb, []string{keys.Sched(q)},
		keys.Prefix(q), 100, state.SourcesCSV(state.Promote)).Slice()
	if err != nil {
		t.Fatal(err)
	}
	if moved, _ := scripts.Int(res[0]); moved != 0 {
		t.Fatalf("moved %d", moved)
	}
	if rdb.ZCard(ctx, keys.Sched(q)).Val() != 0 || rdb.XLen(ctx, keys.Stream(q, keys.DefaultPriority)).Val() != xlen {
		t.Fatal("promote of an active job changed the stream or kept the member")
	}
	if s := rdb.HGet(ctx, keys.Job(q, id), "state").Val(); s != "active" {
		t.Fatalf("state changed to %s", s)
	}
}

// A stale lease holder's fail must not reschedule or dead-letter the job.
func TestFailIsFencedByLeaseToken(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	c := taskq.NewClient(rdb)

	id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	msg := deliver(t, rdb, q, "A")
	k := []string{keys.Job(q, id), keys.Stream(q, keys.DefaultPriority)}
	startJob(t, rdb, q, id, msg.ID, "A", "old")
	claimTo(t, rdb, q, msg.ID, "B")
	startJob(t, rdb, q, id, msg.ID, "B", "new")
	before := snap(t, rdb, q, id)
	st, _ := runStatus(t, scripts.Fail, rdb, append(k, keys.Sched(q), keys.Dead(q)),
		keys.Group, msg.ID, "old", "e", 0, "1", state.SourcesCSV(state.Fail))
	if st != "LEASE_LOST" {
		t.Fatalf("status %s", st)
	}
	if after := snap(t, rdb, q, id); !reflect.DeepEqual(before, after) {
		t.Fatalf("stale fail wrote:\nbefore %+v\nafter  %+v", before, after)
	}
}
