//go:build failure

package failure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/demo"
	"github.com/yousufmo27/taskq/internal/testutil"
	"github.com/yousufmo27/taskq/worker"
)

// producer enqueues continuously from several goroutines and records which
// enqueues Redis acknowledged. Only those count as "acked": the guarantee
// under test is about writes the producer was told succeeded.
type producer struct {
	mu        sync.Mutex
	acked     []string
	uncertain []string // ErrDurabilityUncertain: on the primary, unconfirmed
	failed    int      // errors (e.g. Redis down): not acked, nothing promised
	stop      chan struct{}
	wg        sync.WaitGroup
	started   time.Time
	stopped   time.Time
}

func startProducer(t *testing.T, c *taskq.Client, q string, goroutines int, every time.Duration, o taskq.EnqueueOptions) *producer {
	t.Helper()
	p := &producer{stop: make(chan struct{}), started: time.Now()}
	for range goroutines {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-p.stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				id, err := c.Enqueue(ctx, q, o)
				cancel()
				p.mu.Lock()
				switch {
				case err == nil:
					p.acked = append(p.acked, id)
				case errors.Is(err, taskq.ErrDurabilityUncertain):
					p.uncertain = append(p.uncertain, id)
				default:
					p.failed++
				}
				p.mu.Unlock()
				if every > 0 {
					time.Sleep(every)
				}
			}
		}()
	}
	return p
}

func (p *producer) Stop() {
	close(p.stop)
	p.wg.Wait()
	p.stopped = time.Now()
}

func (p *producer) Acked() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.acked...)
}

// inprocWorker runs a worker goroutine against addr with the demo handler
// recording into rec. Returned stop waits for Run to return.
func inprocWorker(t *testing.T, addr string, rec *redis.Client, q string, o worker.Options) (*worker.Worker, func()) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	o.Queue = q
	if o.Concurrency == 0 {
		o.Concurrency = 8
	}
	if o.RetryBase == 0 {
		o.RetryBase, o.RetryMax = 10*time.Millisecond, 200*time.Millisecond
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout, o.ReleaseGrace = 2*time.Second, time.Second
	}
	w, err := worker.New(rdb, o, demo.Handler(demo.NewRecorder(rec, q, o.VisibilityTimeout)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("in-process worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return w, stop
}

// recorder returns a client for the shared container, used as the
// independent recorder: it never has failures injected.
func recorder(t *testing.T) *redis.Client {
	t.Helper()
	return redis.NewClient(&redis.Options{Addr: testutil.SharedAddr(t)})
}
