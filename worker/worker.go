// Package worker runs job handlers against a queue.
//
// Milestone 1 scope: fetch, start, run, complete. A handler error leaves the
// job active with its entry in the PEL; retries (M2) and crash recovery via
// XAUTOCLAIM (M3) are not implemented yet.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/state"
)

// Handler processes one job. Returning nil completes it. The handler may be
// invoked more than once for the same job (at-least-once delivery), so its
// side effects must be idempotent; use job.ID as the idempotency key.
type Handler func(ctx context.Context, j *taskq.Job) error

// Options configures a Worker.
type Options struct {
	Queue       string
	Concurrency int           // max jobs in flight; default 1
	Consumer    string        // consumer name; default host-pid-random. Must be unique per process.
	Block       time.Duration // XREADGROUP block time; default 1s
	Retention   time.Duration // how long succeeded jobs are kept; default 24h, <0 keeps forever
	Logger      *slog.Logger  // default slog.Default()

	// SkipPolicyCheck disables the startup check that Redis uses
	// maxmemory-policy noeviction. Only for Redis deployments that block
	// CONFIG GET and are known to be configured correctly.
	SkipPolicyCheck bool
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
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.Block <= 0 {
		opts.Block = time.Second
	}
	if opts.Retention == 0 {
		opts.Retention = 24 * time.Hour
	}
	if opts.Consumer == "" {
		host, _ := os.Hostname()
		opts.Consumer = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), job.NewID()[:8])
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
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
// in-flight handlers to return. Handlers do not see ctx's cancellation in
// M1; bounded draining arrives with graceful shutdown in M6.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.checkPolicy(ctx); err != nil {
		return err
	}
	if err := w.ensureGroup(ctx); err != nil {
		return err
	}
	w.log.Info("worker started", "concurrency", w.opts.Concurrency)

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
			w.process(jobCtx, *msg)
		}()
	}
}

// fetch reads at most one new entry. It returns (nil, nil) when the block
// timeout expires with nothing to read.
//
// If ctx is cancelled after Redis delivered an entry but before the reply
// was read, the entry sits in this consumer's PEL; crash recovery (M3)
// reclaims it.
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

func (w *Worker) process(ctx context.Context, msg redis.XMessage) {
	id, _ := msg.Values["id"].(string)
	log := w.log.With("job_id", id, "entry", msg.ID)
	if id == "" {
		// Not written by taskq. Leave it in the PEL for inspection rather
		// than silently deleting data we don't understand.
		log.Error("stream entry without job id")
		return
	}
	jkey := keys.Job(w.opts.Queue, id)
	token := job.NewID()

	res, err := scripts.Start.Run(ctx, w.rdb, []string{jkey, w.stream},
		keys.Group, msg.ID, w.opts.Consumer, token, state.SourcesCSV(state.Start)).Result()
	if err != nil {
		log.Error("start failed", "err", err)
		return
	}
	status, rest, err := scripts.Status(res)
	if err != nil {
		log.Error("start reply", "err", err)
		return
	}
	if status == "SKIP" {
		log.Warn("delivery skipped", "reason", rest[0])
		return
	}
	flat, _ := rest[0].([]any)
	j, err := job.FromFlat(flat)
	if err != nil {
		log.Error("decode job", "err", err)
		return
	}
	log = log.With("type", j.Type, "attempt", j.Attempts)

	if herr := w.call(ctx, j); herr != nil {
		log.Warn("handler failed (retries arrive in M2; job stays active)", "err", herr)
		return
	}

	retention := w.opts.Retention.Milliseconds()
	if w.opts.Retention < 0 {
		retention = 0
	}
	res, err = scripts.Complete.Run(ctx, w.rdb, []string{jkey, w.stream},
		keys.Group, msg.ID, token, retention, state.SourcesCSV(state.Complete)).Result()
	if err != nil {
		// The job is still in the PEL and will be re-delivered: a duplicate,
		// not a loss.
		log.Error("complete failed", "err", err)
		return
	}
	if status, rest, _ = scripts.Status(res); status != "OK" {
		log.Warn("complete rejected", "status", status, "detail", rest)
		return
	}
	log.Debug("job succeeded")
}

// call runs the handler, converting a panic into an error so one bad job
// cannot take down the worker.
func (w *Worker) call(ctx context.Context, j *taskq.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return w.h(ctx, j)
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
