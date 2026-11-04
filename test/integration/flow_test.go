//go:build integration

package integration

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/testutil"
	"github.com/yousufmo27/taskq/worker"
)

// backlog enqueues n jobs at each priority before any worker runs.
func backlog(t *testing.T, c *taskq.Client, q string, n int) {
	t.Helper()
	ctx := context.Background()
	for _, p := range []taskq.Priority{taskq.PriorityHigh, taskq.PriorityDefault, taskq.PriorityLow} {
		for range n {
			if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: string(p), Priority: p}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// processOrder runs one single-slot worker until `take` jobs have run and
// returns their types in processing order.
func processOrder(t *testing.T, q string, opts worker.Options, take int) []string {
	t.Helper()
	rdb := testutil.Shared(t)
	var mu sync.Mutex
	var order []string
	done := make(chan struct{})
	opts.Queue, opts.Concurrency = q, 1
	stop := runWorker(t, newWorkerOpts(t, rdb, opts, func(_ context.Context, j *taskq.Job) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, j.Type)
		if len(order) == take {
			close(done)
		}
		return nil
	}))
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out")
	}
	stop()
	mu.Lock()
	defer mu.Unlock()
	return order[:take]
}

// With every level backlogged, weighted fetching gives each level roughly
// its weight's share; the low level is not starved.
func TestPriorityWeightsPreventStarvation(t *testing.T) {
	t.Parallel()
	q := testutil.Queue(t)
	backlog(t, taskq.NewClient(testutil.Shared(t)), q, 400)
	order := processOrder(t, q, worker.Options{}, 400)
	count := map[string]int{}
	for _, typ := range order {
		count[typ]++
	}
	t.Logf("first 400 jobs: %v (weights 6:3:1)", count)
	// Expected 240/120/40. Binomial sd for low ≈ 6, so these bounds are
	// ~3 sd and the test is not flaky, yet strict priority would give 0.
	if count["low"] < 20 || count["low"] > 65 {
		t.Errorf("low got %d of 400, want ≈40", count["low"])
	}
	if count["high"] < 200 || count["high"] > 280 {
		t.Errorf("high got %d of 400, want ≈240", count["high"])
	}
}

// Strict priority drains high before anything else (and so can starve).
func TestStrictPriority(t *testing.T) {
	t.Parallel()
	q := testutil.Queue(t)
	backlog(t, taskq.NewClient(testutil.Shared(t)), q, 50)
	order := processOrder(t, q, worker.Options{StrictPriority: true}, 150)
	for i, typ := range order {
		want := []string{"high", "default", "low"}[i/50]
		if typ != want {
			t.Fatalf("job %d was %s, want %s", i, typ, want)
		}
	}
}

// The GCRA limiter allows a burst, then spaces tokens at 1/rate.
func TestRateLimitScriptBurstThenWait(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()
	if err := c.SetQueueConfig(ctx, q, taskq.QueueConfig{Rate: 10, Burst: 5}); err != nil {
		t.Fatal(err)
	}
	k := []string{keys.Config(q), keys.RateLimit(q)}
	for i := range 5 {
		if st, _ := runStatus(t, scripts.RateLimit, rdb, k); st != "OK" {
			t.Fatalf("burst token %d: %s", i, st)
		}
	}
	st, rest := runStatus(t, scripts.RateLimit, rdb, k)
	if st != "WAIT" {
		t.Fatalf("6th token: %s, want WAIT", st)
	}
	if ms, _ := scripts.Int(rest[0]); ms <= 0 || ms > 100 {
		t.Fatalf("wait %dms, want (0,100]", ms)
	}
	time.Sleep(110 * time.Millisecond)
	if st, _ := runStatus(t, scripts.RateLimit, rdb, k); st != "OK" {
		t.Fatalf("after waiting 1/rate: %s", st)
	}
}

// The rate applies across all workers of the queue, not per worker.
func TestRateLimitIsGlobalAcrossWorkers(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	const rate, n = 20, 40
	if err := c.SetQueueConfig(ctx, q, taskq.QueueConfig{Rate: rate, Burst: 1}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for range n {
		id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
		ids = append(ids, id)
	}
	for range 4 {
		runWorker(t, newWorker(t, rdb, q, 4, func(context.Context, *taskq.Job) error { return nil }))
	}
	var starts []time.Time
	for _, id := range ids {
		starts = append(starts, waitState(t, c, q, id, taskq.StateSucceeded).StartedAt)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	span := starts[n-1].Sub(starts[0])
	minSpan := time.Duration(float64(n-1) / rate * 0.9 * float64(time.Second))
	t.Logf("%d starts over %v with 16 slots at %d/s", n, span, rate)
	if span < minSpan {
		t.Fatalf("%d starts in %v; a %d/s limit needs at least %v", n, span, rate, minSpan)
	}
	// No 1s window may hold more than rate + burst starts.
	for i := range starts {
		in := 0
		for j := i; j < n && starts[j].Sub(starts[i]) < time.Second; j++ {
			in++
		}
		if in > rate+1 {
			t.Fatalf("%d starts within 1s of %v", in, starts[i])
		}
	}
}

func TestBackpressureMaxDepth(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	if err := c.SetQueueConfig(ctx, q, taskq.QueueConfig{MaxDepth: 10}); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		opts := taskq.EnqueueOptions{Type: "x", Priority: []taskq.Priority{"high", "default", "low"}[i%3]}
		if i == 9 {
			opts.Delay = time.Hour // scheduled jobs count too
		}
		if _, err := c.Enqueue(ctx, q, opts); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"}); !errors.Is(err, taskq.ErrQueueFull) {
		t.Fatalf("11th enqueue: %v, want ErrQueueFull", err)
	}
	// Draining frees capacity.
	runWorker(t, newWorker(t, rdb, q, 4, func(context.Context, *taskq.Job) error { return nil }))
	testutil.WaitFor(t, 10*time.Second, "capacity", func() bool {
		_, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
		return err == nil
	})
	cfg, _ := c.GetQueueConfig(ctx, q)
	if cfg.MaxDepth != 10 {
		t.Fatalf("config round trip: %+v", cfg)
	}
}

// Concurrent producers cannot overshoot the depth limit: the check and the
// write are one script.
func TestBackpressureUnderConcurrentProducers(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()
	if err := c.SetQueueConfig(ctx, q, taskq.QueueConfig{MaxDepth: 100}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, full := 0, 0
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
				mu.Lock()
				if err == nil {
					ok++
				} else if errors.Is(err, taskq.ErrQueueFull) {
					full++
				} else {
					t.Error(err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ok != 100 || full != 300 {
		t.Fatalf("accepted %d, rejected %d; want exactly 100/300", ok, full)
	}
}

func TestInvalidPriorityAndConfig(t *testing.T) {
	t.Parallel()
	c := taskq.NewClient(testutil.Shared(t))
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, "q", taskq.EnqueueOptions{Type: "x", Priority: "urgent"}); err == nil {
		t.Fatal("accepted unknown priority")
	}
	if err := c.SetQueueConfig(ctx, "q", taskq.QueueConfig{Rate: -1}); err == nil {
		t.Fatal("accepted negative rate")
	}
}
