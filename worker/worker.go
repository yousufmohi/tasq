// Package worker runs job handlers against a queue.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

	// VisibilityTimeout is how long a delivered job may go without a
	// heartbeat before another worker reclaims it. It bounds crash recovery
	// time; too short causes false reclaims (duplicates) during GC pauses
	// or network blips. Default 30s.
	VisibilityTimeout time.Duration
	// HeartbeatInterval is how often leases are renewed. Default
	// VisibilityTimeout/3; must be < VisibilityTimeout/2.
	HeartbeatInterval time.Duration
	// ReclaimInterval is how often this worker scans for jobs whose lease
	// expired. Default VisibilityTimeout/4 (jittered ±20%).
	ReclaimInterval time.Duration

	// Weights sets each priority level's share when all are backlogged.
	// Default DefaultWeights (6:3:1). StrictPriority instead always drains
	// higher levels first, which can starve lower ones indefinitely.
	Weights        Weights
	StrictPriority bool

	// DrainTimeout is how long shutdown waits for in-flight handlers to
	// finish before cancelling them. Keep it below the orchestrator's kill
	// grace period (Compose stop_grace_period, Kubernetes
	// terminationGracePeriodSeconds). Default 25s.
	DrainTimeout time.Duration
	// ReleaseGrace is how long cancelled handlers get to return before their
	// jobs are released regardless. Default 2s.
	ReleaseGrace time.Duration
	// ConsumerTTL: consumers idle this long with no pending entries are
	// removed by the consumer GC. Default 1h. ConsumerGCInterval default 5m.
	ConsumerTTL        time.Duration
	ConsumerGCInterval time.Duration

	// SkipPolicyCheck disables the startup check that Redis uses
	// maxmemory-policy noeviction. Only for Redis deployments that block
	// CONFIG GET and are known to be configured correctly.
	SkipPolicyCheck bool
}

func (o *Options) setDefaults() error {
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
	if o.VisibilityTimeout <= 0 {
		o.VisibilityTimeout = 30 * time.Second
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = o.VisibilityTimeout / 3
	}
	if o.ReclaimInterval <= 0 {
		o.ReclaimInterval = o.VisibilityTimeout / 4
	}
	if o.HeartbeatInterval*2 >= o.VisibilityTimeout {
		return fmt.Errorf("worker: HeartbeatInterval (%v) must be < VisibilityTimeout/2 (%v)",
			o.HeartbeatInterval, o.VisibilityTimeout)
	}
	if o.Weights.High < 0 || o.Weights.Default < 0 || o.Weights.Low < 0 {
		return fmt.Errorf("worker: negative priority weight %+v", o.Weights)
	}
	if o.Weights == (Weights{}) {
		o.Weights = DefaultWeights
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 25 * time.Second
	}
	if o.ReleaseGrace <= 0 {
		o.ReleaseGrace = 2 * time.Second
	}
	if o.ConsumerTTL <= 0 {
		o.ConsumerTTL = time.Hour
	}
	if o.ConsumerGCInterval <= 0 {
		o.ConsumerGCInterval = 5 * time.Minute
	}
	return nil
}

// Worker consumes one queue.
type Worker struct {
	rdb     redis.UniversalClient
	opts    Options
	h       Handler
	log     *slog.Logger
	streams []string // priority streams, highest first
	leases  *leases

	// Fetch-loop state; touched only by the goroutine running Run.
	buffer        []delivery // extra entries from a multi-stream read
	reclaimCursor []string   // XAUTOCLAIM cursor per stream
	reclaimIdx    int        // stream currently being scanned
	nextReclaim   time.Time
	rateLimited   bool
	rateCheckedAt time.Time
}

// New validates options and returns a Worker. It does not touch Redis.
func New(rdb redis.UniversalClient, opts Options, h Handler) (*Worker, error) {
	if err := keys.ValidateQueue(opts.Queue); err != nil {
		return nil, err
	}
	if h == nil {
		return nil, errors.New("worker: nil handler")
	}
	if err := opts.setDefaults(); err != nil {
		return nil, err
	}
	return &Worker{
		rdb:           rdb,
		opts:          opts,
		h:             h,
		log:           opts.Logger.With("queue", opts.Queue, "consumer", opts.Consumer),
		streams:       keys.Streams(opts.Queue),
		leases:        newLeases(),
		reclaimCursor: []string{"0-0", "0-0", "0-0"},
	}, nil
}

// Consumer returns the consumer name this worker registers with.
func (w *Worker) Consumer() string { return w.opts.Consumer }

// InFlight returns the number of jobs currently running.
func (w *Worker) InFlight() int { return w.leases.len() }

// ensureGroup creates the consumer group on every priority stream at ID 0,
// not "$": jobs enqueued before the first worker ever started must still be
// delivered.
func (w *Worker) ensureGroup(ctx context.Context) error {
	for _, s := range w.streams {
		err := w.rdb.XGroupCreateMkStream(ctx, s, keys.Group, "0").Err()
		if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("worker: create group on %s: %w", s, err)
		}
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
