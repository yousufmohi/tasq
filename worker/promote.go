package worker

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/state"
)

// promoteBatch bounds how many jobs one promote.lua call moves, and so how
// long it blocks Redis.
const promoteBatch = 100

// promoteLoop moves due scheduled/retry jobs into their streams. Every worker
// runs one; promote.lua is atomic, so no leader election is needed.
func (w *Worker) promoteLoop(ctx context.Context) {
	for {
		full, err := w.promoteOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("promote failed", "err", err)
		}
		if full {
			continue // more due jobs are waiting
		}
		sleep(ctx, jitter(w.opts.PromoteInterval))
		if ctx.Err() != nil {
			return
		}
	}
}

// promoteOnce runs one bounded promotion and reports whether it hit the
// batch limit.
func (w *Worker) promoteOnce(ctx context.Context) (full bool, err error) {
	q := w.opts.Queue
	res, err := scripts.Promote.Run(ctx, w.rdb, []string{keys.Sched(q)},
		keys.Prefix(q), promoteBatch, state.SourcesCSV(state.Promote)).Slice()
	if err != nil {
		return false, err
	}
	scanned, err := scripts.Int(res[1])
	if err != nil {
		return false, err
	}
	return scanned >= promoteBatch, nil
}

// jitter returns d ± 20%, so that workers started together do not hit Redis
// in lockstep.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d*8/10 + time.Duration(rand.Int64N(int64(d)*4/10+1))
}
