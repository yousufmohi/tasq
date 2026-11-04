package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
)

var (
	// errLeaseLost: Redis says another worker owns the job now.
	errLeaseLost = errors.New("taskq: lease lost to another worker")
	// errLeaseExpired: we could not renew the lease for long enough that
	// another worker may have reclaimed the job (self-fencing).
	errLeaseExpired = errors.New("taskq: lease could not be renewed before expiry")
)

// inflight is one started job whose lease this worker keeps alive.
type inflight struct {
	jkey, stream, entry, token string
	cancel                     context.CancelCauseFunc
	lastRenewed                time.Time // local monotonic clock
}

// leases tracks in-flight jobs for the heartbeat loop.
type leases struct {
	mu sync.Mutex
	m  map[string]*inflight // by entry id
}

func newLeases() *leases { return &leases{m: map[string]*inflight{}} }

func (l *leases) add(f *inflight) {
	l.mu.Lock()
	l.m[f.entry] = f
	l.mu.Unlock()
}

func (l *leases) remove(entry string) {
	l.mu.Lock()
	delete(l.m, entry)
	l.mu.Unlock()
}

func (l *leases) has(entry string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.m[entry]
	return ok
}

func (l *leases) snapshot() []*inflight {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*inflight, 0, len(l.m))
	for _, f := range l.m {
		out = append(out, f)
	}
	return out
}

func (l *leases) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.m)
}

// heartbeatLoop renews the lease of every in-flight job each
// HeartbeatInterval until ctx is done.
//
// Self-fencing: if a job's lease has not been renewed for
// VisibilityTimeout - HeartbeatInterval, another worker may reclaim it at any
// moment, so its handler is cancelled. This narrows, but cannot eliminate,
// the window in which two workers run the same job: the handler may ignore
// cancellation, and our local clock and Redis's idle clock are not the same
// clock.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.opts.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		w.heartbeatOnce(ctx)
	}
}

func (w *Worker) heartbeatOnce(ctx context.Context) {
	deadline := w.opts.VisibilityTimeout - w.opts.HeartbeatInterval
	for _, f := range w.leases.snapshot() {
		// Bound each call so that a partition cannot stall the loop past the
		// self-fencing deadline of the other jobs.
		hctx, cancel := context.WithTimeout(ctx, w.opts.HeartbeatInterval/2)
		done := w.metrics.scriptTimer("heartbeat")
		res, err := scripts.Heartbeat.Run(hctx, w.rdb, []string{f.jkey, f.stream},
			keys.Group, f.entry, w.opts.Consumer, f.token,
			w.opts.VisibilityTimeout.Milliseconds()).Result()
		cancel()
		done()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if time.Since(f.lastRenewed) > deadline {
				w.log.Warn("lease renewal failing past safety margin; cancelling handler",
					"entry", f.entry, "err", err)
				w.metrics.lostLease(w.opts.Queue, "renewal_failed")
				f.cancel(errLeaseExpired)
				w.leases.remove(f.entry)
			} else {
				w.log.Warn("heartbeat failed", "entry", f.entry, "err", err)
			}
			continue
		}
		status, rest, _ := scripts.Status(res)
		if status == "OK" {
			f.lastRenewed = time.Now()
			continue
		}
		w.log.Warn("lease lost; cancelling handler", "entry", f.entry, "reason", rest)
		w.metrics.lostLease(w.opts.Queue, "taken_over")
		f.cancel(errLeaseLost)
		w.leases.remove(f.entry)
	}
}
