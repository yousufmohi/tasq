//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/idem"
	"github.com/yousufmohi/tasq/internal/keys"
	"github.com/yousufmohi/tasq/internal/testutil"
)

func TestEnqueueIdempotencyKeyDeduplicates(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	id1, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "charge", IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "charge", IdempotencyKey: "order-42"})
	if !errors.Is(err, taskq.ErrDuplicate) || id2 != id1 {
		t.Fatalf("retry: id=%s err=%v, want %s + ErrDuplicate", id2, err, id1)
	}
	if n := rdb.XLen(ctx, keys.Stream(q, keys.DefaultPriority)).Val(); n != 1 {
		t.Fatalf("%d stream entries, want 1", n)
	}
	j, _ := c.GetJob(ctx, q, id1)
	if j.IdempotencyKey() != "order-42" {
		t.Fatalf("Job.IdempotencyKey() = %q", j.IdempotencyKey())
	}
	// Another key, or no key, creates a new job; the ID is the fallback key.
	id3, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "charge"})
	if err != nil || id3 == id1 {
		t.Fatal(err)
	}
	if j, _ := c.GetJob(ctx, q, id3); j.IdempotencyKey() != id3 {
		t.Fatalf("keyless job IdempotencyKey = %q, want its ID", j.IdempotencyKey())
	}
}

// A retried enqueue must find the original even when the queue is full.
func TestDuplicateBeatsQueueFull(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()
	c.SetQueueConfig(ctx, q, taskq.QueueConfig{MaxDepth: 1})
	id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", IdempotencyKey: "k"}); got != id || !errors.Is(err, taskq.ErrDuplicate) {
		t.Fatalf("got %s %v", got, err)
	}
}

func TestConcurrentEnqueueSameKeyCreatesOneJob(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	var created, dup atomic.Int32
	ids := sync.Map{}
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", IdempotencyKey: "same"})
			switch {
			case err == nil:
				created.Add(1)
			case errors.Is(err, taskq.ErrDuplicate):
				dup.Add(1)
			default:
				t.Error(err)
			}
			ids.Store(id, true)
		}()
	}
	wg.Wait()
	n := 0
	ids.Range(func(any, any) bool { n++; return true })
	if created.Load() != 1 || dup.Load() != 49 || n != 1 {
		t.Fatalf("created=%d dup=%d distinct ids=%d", created.Load(), dup.Load(), n)
	}
}

func TestIdempotencyKeyExpires(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()
	o := taskq.EnqueueOptions{Type: "x", IdempotencyKey: "short", IdempotencyTTL: 100 * time.Millisecond}
	id1, _ := c.Enqueue(ctx, q, o)
	time.Sleep(150 * time.Millisecond)
	id2, err := c.Enqueue(ctx, q, o)
	if err != nil || id2 == id1 {
		t.Fatalf("after TTL: id=%s err=%v; a new job was expected", id2, err)
	}
	if _, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x", IdempotencyKey: strings.Repeat("k", 257)}); err == nil {
		t.Fatal("accepted oversized key")
	}
}

func TestOnceRunsOnce(t *testing.T) {
	t.Parallel()
	s := idem.New(testutil.Shared(t))
	ctx := context.Background()
	key := testutil.Queue(t)

	var runs atomic.Int32
	fn := func(context.Context) error { runs.Add(1); return nil }
	if ran, err := s.Once(ctx, key, fn); !ran || err != nil {
		t.Fatalf("first: ran=%v err=%v", ran, err)
	}
	if ran, err := s.Once(ctx, key, fn); ran || err != nil {
		t.Fatalf("second: ran=%v err=%v", ran, err)
	}
	if done, _ := s.Done(ctx, key); !done || runs.Load() != 1 {
		t.Fatalf("done=%v runs=%d", done, runs.Load())
	}
}

func TestOnceConcurrentCallersRunOnce(t *testing.T) {
	t.Parallel()
	s := idem.New(testutil.Shared(t))
	ctx := context.Background()
	key := testutil.Queue(t)

	var runs, inProgress atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Once(ctx, key, func(context.Context) error {
				runs.Add(1)
				time.Sleep(50 * time.Millisecond)
				return nil
			})
			if errors.Is(err, idem.ErrInProgress) {
				inProgress.Add(1)
			} else if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if runs.Load() != 1 {
		t.Fatalf("fn ran %d times", runs.Load())
	}
	t.Logf("%d callers saw ErrInProgress", inProgress.Load())
}

func TestOnceFailureAllowsRetry(t *testing.T) {
	t.Parallel()
	s := idem.New(testutil.Shared(t))
	ctx := context.Background()
	key := testutil.Queue(t)

	boom := errors.New("boom")
	if ran, err := s.Once(ctx, key, func(context.Context) error { return boom }); !ran || !errors.Is(err, boom) {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	if ran, err := s.Once(ctx, key, func(context.Context) error { return nil }); !ran || err != nil {
		t.Fatalf("retry after failure: ran=%v err=%v", ran, err)
	}
}

// Documents the limit: once the lock TTL passes, a second caller may run fn
// while the first is still running.
func TestOnceLockExpiryAllowsConcurrentRun(t *testing.T) {
	t.Parallel()
	s := idem.New(testutil.Shared(t))
	s.LockTTL = 50 * time.Millisecond
	ctx := context.Background()
	key := testutil.Queue(t)

	var runs atomic.Int32
	slow := make(chan struct{})
	go s.Once(ctx, key, func(context.Context) error { runs.Add(1); <-slow; return nil })
	time.Sleep(100 * time.Millisecond)
	ran, err := s.Once(ctx, key, func(context.Context) error { runs.Add(1); return nil })
	close(slow)
	if !ran || err != nil || runs.Load() != 2 {
		t.Fatalf("ran=%v err=%v runs=%d; expected the documented double run", ran, err, runs.Load())
	}
}
