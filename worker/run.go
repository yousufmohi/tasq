package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/yousufmohi/tasq/internal/keys"
	"github.com/yousufmohi/tasq/internal/scripts"
	"github.com/yousufmohi/tasq/internal/state"
)

// errShutdown is the cancellation cause handlers see when the drain timeout
// expires during graceful shutdown.
var errShutdown = errors.New("taskq: worker shutting down")

// Run fetches and processes jobs until ctx is cancelled, then shuts down
// gracefully:
//
//  1. stop fetching, reclaiming and promoting;
//  2. keep renewing leases and wait up to DrainTimeout for in-flight
//     handlers to finish normally;
//  3. cancel the remaining handlers' contexts and wait up to ReleaseGrace;
//     a handler that returns an error now has its job released back to the
//     queue (attempt refunded) instead of failed;
//  4. release any job whose handler still has not returned, and return;
//  5. remove this consumer from the group, only if it owns no pending
//     entries.
//
// Run returns nil after a graceful shutdown. A handler that ignores
// cancellation may still be running when Run returns; its later
// complete/fail is rejected because the release cleared its lease token.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.checkPolicy(ctx); err != nil {
		return err
	}
	if err := w.ensureGroup(ctx); err != nil {
		return err
	}
	w.log.Info("worker started", "concurrency", w.opts.Concurrency,
		"visibility_timeout", w.opts.VisibilityTimeout)

	// Handlers and Redis calls made on their behalf must outlive ctx, so
	// that cancelling Run drains instead of abandoning work mid-way.
	detached := context.WithoutCancel(ctx)
	jobCtx, cancelJobs := context.WithCancelCause(detached)
	defer cancelJobs(nil)
	hbCtx, stopHeartbeats := context.WithCancel(detached)
	defer stopHeartbeats()

	var bg sync.WaitGroup
	bg.Add(3)
	go func() { defer bg.Done(); w.promoteLoop(ctx) }()
	go func() { defer bg.Done(); w.consumerGCLoop(ctx) }()
	go func() { defer bg.Done(); w.heartbeatLoop(hbCtx) }()

	sem := make(chan struct{}, w.opts.Concurrency)
	var wg sync.WaitGroup
	backoff := 50 * time.Millisecond

	// release returns n unused slots.
	release := func(n int) {
		for range n {
			<-sem
		}
	}

loop:
	for {
		// Wait for one free slot, then claim every other free slot without
		// waiting: a fetch takes up to that many jobs in one round trip.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		slots := 1
	grab:
		for slots < w.opts.Concurrency {
			select {
			case sem <- struct{}{}:
				slots++
			default:
				break grab
			}
		}
		ds, err := w.fetch(ctx, slots)
		if err != nil {
			release(slots)
			if ctx.Err() != nil {
				break loop
			}
			w.log.Error("fetch failed", "err", err)
			if strings.Contains(err.Error(), "NOGROUP") {
				// Redis lost its data (restart without persistence, or
				// failover to an empty replica). Recreate the groups or every
				// read would fail forever.
				if gerr := w.ensureGroup(ctx); gerr == nil {
					w.log.Warn("consumer group was missing and has been recreated")
				}
			}
			sleep(ctx, backoff)
			backoff = min(backoff*2, 2*time.Second)
			continue
		}
		backoff = 50 * time.Millisecond
		release(slots - len(ds))
		for _, d := range ds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				w.process(detached, jobCtx, d)
			}()
		}
	}

	w.drain(detached, &wg, cancelJobs)
	stopHeartbeats()
	bg.Wait()
	w.deleteConsumer(detached)
	w.log.Info("worker stopped")
	return nil
}

// drain implements steps 2-4 of the shutdown sequence.
func (w *Worker) drain(ctx context.Context, wg *sync.WaitGroup, cancelJobs context.CancelCauseFunc) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	n := w.leases.len()
	if n > 0 {
		w.log.Info("draining in-flight jobs", "in_flight", n, "timeout", w.opts.DrainTimeout)
	}
	select {
	case <-done:
		return
	case <-time.After(w.opts.DrainTimeout):
	}

	w.log.Warn("drain timeout; cancelling handlers", "in_flight", w.leases.len())
	cancelJobs(errShutdown)
	select {
	case <-done:
		return
	case <-time.After(w.opts.ReleaseGrace):
	}

	stuck := w.leases.snapshot()
	for _, f := range stuck {
		if w.release(ctx, f.jkey, f.stream, f.entry, f.token) {
			w.metrics.outcome(w.opts.Queue, OutcomeReleased, 0)
		}
		w.leases.remove(f.entry)
	}
	w.log.Error("handlers ignored cancellation; their jobs were released and may run twice",
		"count", len(stuck))
}

// release returns an active job to its stream with the attempt refunded and
// reports whether it did.
func (w *Worker) release(ctx context.Context, jkey, stream, entry, token string) bool {
	done := w.metrics.scriptTimer("release")
	defer done()
	res, err := scripts.Release.Run(ctx, w.rdb, []string{jkey, stream},
		keys.Group, entry, token, state.SourcesCSV(state.Release), "1").Result()
	if err != nil {
		// Still in our PEL; another worker reclaims it after the
		// visibility timeout.
		w.log.Error("release failed; job will be reclaimed", "entry", entry, "err", err)
		return false
	}
	status, rest, _ := scripts.Status(res)
	if status != "OK" {
		w.log.Warn("release rejected", "entry", entry, "status", status, "detail", rest)
		return false
	}
	w.log.Info("job released for another worker", "entry", entry)
	return true
}

// deleteConsumer removes this consumer from every stream's group if it owns
// no pending entries there. Entries still pending (released failed, or
// buffered entries never started) stay and are reclaimed by other workers.
func (w *Worker) deleteConsumer(ctx context.Context) {
	for _, s := range w.streams {
		res, err := scripts.DelConsumer.Run(ctx, w.rdb, []string{s}, keys.Group, w.opts.Consumer).Result()
		if err != nil {
			w.log.Warn("delete consumer failed", "stream", s, "err", err)
			continue
		}
		if status, _, _ := scripts.Status(res); status != "OK" {
			w.log.Info("consumer kept: it still owns pending entries", "stream", s)
		}
	}
}

// consumerGCLoop periodically deletes consumers left behind by workers that
// crashed (and so never ran deleteConsumer): those idle for longer than
// ConsumerTTL that own no pending entries. Without it, every crashed
// process leaves a consumer in the group forever.
func (w *Worker) consumerGCLoop(ctx context.Context) {
	for {
		sleep(ctx, jitter(w.opts.ConsumerGCInterval))
		if ctx.Err() != nil {
			return
		}
		for _, s := range w.streams {
			cs, err := w.rdb.XInfoConsumers(ctx, s, keys.Group).Result()
			if err != nil {
				if ctx.Err() == nil {
					w.log.Warn("consumer gc: list consumers", "stream", s, "err", err)
				}
				continue
			}
			for _, c := range cs {
				if c.Name == w.opts.Consumer || c.Pending > 0 || c.Idle < w.opts.ConsumerTTL {
					continue
				}
				res, err := scripts.DelConsumer.Run(ctx, w.rdb, []string{s}, keys.Group, c.Name).Result()
				if err != nil {
					continue
				}
				if status, _, _ := scripts.Status(res); status == "OK" {
					w.log.Info("consumer gc: removed idle consumer", "stream", s, "consumer", c.Name, "idle", c.Idle)
				}
			}
		}
	}
}
