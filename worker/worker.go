// Package worker runs job handlers against a queue.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
)

// Handler processes one job. Returning nil completes it; returning an error
// schedules a retry with backoff, or dead-letters the job when attempts are
// exhausted or the error is wrapped with taskq.Permanent.
//
// The handler may run more than once for the same job (at-least-once
// delivery), and in rare cases concurrently with itself (docs/DESIGN.md §3),
// so side effects must be idempotent: pass j.IdempotencyKey() downstream.
// Handlers must return promptly when ctx is cancelled.
type Handler func(ctx context.Context, j *taskq.Job) error

// Options configures a Worker. Zero values select the documented defaults.
type Options struct {
	Queue       string
	Concurrency int           // max jobs in flight; default 1
	Consumer    string        // consumer name; default host-pid-random. Must be unique per process.
	Block       time.Duration // XREADGROUP block time; default 1s
	Retention   time.Duration // how long succeeded jobs are kept; default 24h, <0 keeps forever
	Logger      *slog.Logger  // default slog.Default()

	// JobTimeout bounds each handler call unless the job sets its own
	// Timeout. Default 0: no timeout.
	JobTimeout time.Duration
	// RetryBase and RetryMax shape the full-jitter exponential backoff:
	// delay ∈ [0, min(RetryMax, RetryBase·2^(attempt-1))]. Defaults 1s, 10m.
	RetryBase time.Duration
	RetryMax  time.Duration
	// PromoteInterval is how often due scheduled/retry jobs are moved into
	// their stream. Default 250ms (jittered ±20%).
	PromoteInterval time.Duration

	// SkipPolicyCheck disables the startup check that Redis uses
	// maxmemory-policy noeviction. Only for Redis deployments that block
	// CONFIG GET and are known to be configured correctly.
	SkipPolicyCheck bool
}

func (o *Options) setDefaults() {
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.Block <= 0 {
		o.Block = time.Second
	}
	if o.Retention == 0 {
		o.Retention = 24 * time.Hour
	}
	if o.Consumer == "" {
		host, _ := os.Hostname()
		o.Consumer = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), job.NewID()[:8])
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.RetryBase <= 0 {
		o.RetryBase = time.Second
	}
	if o.RetryMax <= 0 {
		o.RetryMax = 10 * time.Minute
	}
	if o.PromoteInterval <= 0 {
		o.PromoteInterval = 250 * time.Millisecond
	}
}

// Worker consumes one queue.
type Worker struct {
	rdb    redis.UniversalClient
	opts   Options
	h      Handler
	log    *slog.Logger
	stream string
}

// New validates options and returns a Worker. It does not touch Redis.
func New(rdb redis.UniversalClient, opts Options, h Handler) (*Worker, error) {
	if err := keys.ValidateQueue(opts.Queue); err != nil {
		return nil, err
	}
	if h == nil {
		return nil, errors.New("worker: nil handler")
	}
	opts.setDefaults()
	return &Worker{
		rdb:    rdb,
		opts:   opts,
		h:      h,
		log:    opts.Logger.With("queue", opts.Queue, "consumer", opts.Consumer),
		stream: keys.Stream(opts.Queue, keys.DefaultPriority),
	}, nil
}

// Consumer returns the consumer name this worker registers with.
func (w *Worker) Consumer() string { return w.opts.Consumer }

// Run fetches and processes jobs until ctx is cancelled, then waits for
// in-flight handlers to return.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.checkPolicy(ctx); err != nil {
		return err
	}
	if err := w.ensureGroup(ctx); err != nil {
		return err
	}
	w.log.Info("worker started", "concurrency", w.opts.Concurrency)

	var bg sync.WaitGroup
	bg.Add(1)
	go func() { defer bg.Done(); w.promoteLoop(ctx) }()

	// jobCtx outlives ctx so that cancelling Run lets in-flight jobs finish
	// and be acknowledged instead of being abandoned mid-way.
	jobCtx := context.WithoutCancel(ctx)
	sem := make(chan struct{}, w.opts.Concurrency)
	var wg sync.WaitGroup
	backoff := 50 * time.Millisecond

	for {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			bg.Wait()
			w.log.Info("worker stopped")
			return nil
		}
		msg, err := w.fetch(ctx)
		if err != nil {
			<-sem
			if ctx.Err() != nil {
				continue // loop exits via ctx.Done above
			}
			w.log.Error("fetch failed", "err", err)
			sleep(ctx, backoff)
			backoff = min(backoff*2, 2*time.Second)
			continue
		}
		backoff = 50 * time.Millisecond
		if msg == nil {
			<-sem
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.process(jobCtx, delivery{stream: w.stream, msg: *msg})
		}()
	}
}

// fetch reads at most one new entry. It returns (nil, nil) when the block
// timeout expires with nothing to read.
//
// If ctx is cancelled after Redis delivered an entry but before the reply
// was read, the entry sits in this consumer's PEL; crash recovery reclaims
// it.
func (w *Worker) fetch(ctx context.Context) (*redis.XMessage, error) {
	res, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    keys.Group,
		Consumer: w.opts.Consumer,
		Streams:  []string{w.stream, ">"},
		Count:    1,
		Block:    w.opts.Block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, s := range res {
		for i := range s.Messages {
			return &s.Messages[i], nil
		}
	}
	return nil, nil
}

// ensureGroup creates the consumer group at ID 0, not "$": jobs enqueued
// before the first worker ever started must still be delivered.
func (w *Worker) ensureGroup(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, w.stream, keys.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("worker: create group: %w", err)
	}
	return nil
}

// checkPolicy refuses to run against a Redis that may evict keys: eviction
// would silently delete job hashes or streams, breaking at-least-once.
func (w *Worker) checkPolicy(ctx context.Context) error {
	if w.opts.SkipPolicyCheck {
		return nil
	}
	v, err := w.rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return fmt.Errorf("worker: read maxmemory-policy (set SkipPolicyCheck if CONFIG is disabled): %w", err)
	}
	if p := v["maxmemory-policy"]; p != "noeviction" {
		return fmt.Errorf("worker: Redis maxmemory-policy is %q; taskq requires noeviction", p)
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
