package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
)

// Weights sets the relative share of fetches each priority level is tried
// first. With every level backlogged, level i receives about
// weight_i / sum(weights) of throughput, so no level with a non-zero weight
// can starve. A zero weight means "only when the others are empty".
type Weights struct{ High, Default, Low int }

// DefaultWeights favours high priority 6:3:1.
var DefaultWeights = Weights{High: 6, Default: 3, Low: 1}

// weightedOrder returns the priority indices (0 high, 1 default, 2 low) in
// the order to try them: weighted random sampling without replacement, or
// fixed order when strict.
func weightedOrder(w Weights, strict bool, rnd func(n int) int) [3]int {
	if strict {
		return [3]int{0, 1, 2}
	}
	ws := [3]int{max(w.High, 0), max(w.Default, 0), max(w.Low, 0)}
	idx := []int{0, 1, 2}
	var out [3]int
	for k := range out {
		total := 0
		for _, i := range idx {
			total += ws[i]
		}
		pick := 0 // all remaining weights zero: keep priority order
		if total > 0 {
			r := rnd(total)
			for p, i := range idx {
				if r < ws[i] {
					pick = p
					break
				}
				r -= ws[i]
			}
		}
		out[k] = idx[pick]
		idx = slices.Delete(idx, pick, pick+1)
	}
	return out
}

// fetch returns up to max deliveries, or none if nothing arrived within the
// block timeout. Order of sources:
//
//  1. entries buffered by an earlier multi-stream read;
//  2. a reclaimed entry whose lease expired, if a reclaim scan is due;
//  3. up to max new entries from the first non-empty priority stream, tried
//     in weighted order without blocking, in one round trip (fetch.lua);
//  4. a blocking read on all priority streams.
//
// max is the number of free slots, so a worker never claims more than it
// can start. When the queue is rate limited, max is forced to 1 and a token
// is taken first, so a rate-limited worker never pulls a job into its PEL
// just to let it sit there.
func (w *Worker) fetch(ctx context.Context, max int) ([]delivery, error) {
	limited, err := w.acquireToken(ctx)
	if err != nil {
		return nil, err
	}
	if limited {
		max = 1
	}
	if len(w.buffer) > 0 {
		n := min(max, len(w.buffer))
		ds := append([]delivery(nil), w.buffer[:n]...)
		w.buffer = w.buffer[n:]
		return ds, nil
	}
	if d, err := w.reclaim(ctx); d != nil || err != nil {
		if d == nil {
			return nil, err
		}
		return []delivery{*d}, nil
	}
	if ds, err := w.fetchWeighted(ctx, max); len(ds) > 0 || err != nil {
		return ds, err
	}
	ds, err := w.read(ctx, w.streams, w.opts.Block)
	if err != nil || len(ds) == 0 {
		return nil, err
	}
	// XREADGROUP COUNT applies per stream, so a multi-stream read can return
	// up to one entry per stream. Keep the extras rather than exceeding our
	// concurrency; they are already in our PEL. If they wait longer than the
	// visibility timeout another worker may reclaim them, and start.lua's
	// ownership check then makes our later start a no-op.
	n := min(max, len(ds))
	w.buffer = append(w.buffer, ds[n:]...)
	return ds[:n], nil
}

// fetchWeighted reads up to max entries from the first non-empty priority
// stream, trying them in weighted-random order (fetch.lua).
func (w *Worker) fetchWeighted(ctx context.Context, max int) ([]delivery, error) {
	order := weightedOrder(w.opts.Weights, w.opts.StrictPriority, rand.IntN)
	streams := make([]string, len(order))
	for k, i := range order {
		streams[k] = w.streams[i]
	}
	done := w.metrics.scriptTimer("fetch")
	res, err := scripts.Fetch.Run(ctx, w.rdb, streams, keys.Group, w.opts.Consumer, max).Slice()
	done()
	if errors.Is(err, redis.Nil) {
		return nil, nil // every stream empty
	}
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, fmt.Errorf("fetch: unexpected reply %v", res)
	}
	stream, _ := res[0].(string)
	entries, _ := res[1].([]any)
	out := make([]delivery, 0, len(entries))
	for _, e := range entries {
		pair, _ := e.([]any)
		if len(pair) != 2 {
			return nil, fmt.Errorf("fetch: malformed entry %v", e)
		}
		id, _ := pair[0].(string)
		flat, _ := pair[1].([]any)
		values := make(map[string]any, len(flat)/2)
		for i := 0; i+1 < len(flat); i += 2 {
			k, _ := flat[i].(string)
			values[k] = flat[i+1]
		}
		out = append(out, delivery{stream: stream, msg: redis.XMessage{ID: id, Values: values}})
	}
	return out, nil
}

// read issues XREADGROUP COUNT 1 over streams; block < 0 means do not block.
// Results are ordered by priority.
func (w *Worker) read(ctx context.Context, streams []string, block time.Duration) ([]delivery, error) {
	args := make([]string, 0, 2*len(streams))
	args = append(args, streams...)
	for range streams {
		args = append(args, ">")
	}
	res, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    keys.Group,
		Consumer: w.opts.Consumer,
		Streams:  args,
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []delivery
	for _, s := range w.streams { // priority order
		for _, r := range res {
			if r.Stream != s {
				continue
			}
			for _, m := range r.Messages {
				out = append(out, delivery{stream: r.Stream, msg: m})
			}
		}
	}
	return out, nil
}

// reclaim claims at most one entry whose lease expired, using one bounded
// XAUTOCLAIM call per fetch. It walks each priority stream's PEL with a
// cursor; when all three passes complete, it waits ReclaimInterval before
// scanning again. Reclaimed entries go through start.lua like new ones,
// which rotates the lease token and counts the attempt.
func (w *Worker) reclaim(ctx context.Context) (*delivery, error) {
	if time.Now().Before(w.nextReclaim) {
		return nil, nil
	}
	s := w.streams[w.reclaimIdx]
	msgs, next, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s,
		Group:    keys.Group,
		Consumer: w.opts.Consumer,
		MinIdle:  w.opts.VisibilityTimeout,
		Start:    w.reclaimCursor[w.reclaimIdx],
		Count:    1,
	}).Result()
	if err != nil {
		w.nextReclaim = time.Now().Add(jitter(w.opts.ReclaimInterval))
		return nil, fmt.Errorf("reclaim %s: %w", s, err)
	}
	w.reclaimCursor[w.reclaimIdx] = next
	if next == "0-0" {
		w.reclaimIdx = (w.reclaimIdx + 1) % len(w.streams)
		if w.reclaimIdx == 0 {
			// Full pass over all streams done.
			w.nextReclaim = time.Now().Add(jitter(w.opts.ReclaimInterval))
		}
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	if w.leases.has(msgs[0].ID) {
		// Our own in-flight job went idle past the timeout (heartbeats were
		// failing). The heartbeat loop owns that decision; starting it again
		// here would run it twice in this process.
		return nil, nil
	}
	w.log.Info("reclaimed expired lease", "entry", msgs[0].ID, "stream", s)
	return &delivery{stream: s, msg: msgs[0], reclaimed: true}, nil
}

// acquireToken blocks until the queue's rate limiter grants a start and
// reports whether the queue is rate limited at all. Whether a rate is set
// is cached for a second so an unlimited queue pays no extra round trip.
func (w *Worker) acquireToken(ctx context.Context) (limited bool, err error) {
	if time.Since(w.rateCheckedAt) > time.Second {
		v, err := w.rdb.HGet(ctx, keys.Config(w.opts.Queue), "rate").Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return false, fmt.Errorf("read rate: %w", err)
		}
		r, _ := strconv.ParseFloat(v, 64)
		w.rateLimited = r > 0
		w.rateCheckedAt = time.Now()
	}
	if !w.rateLimited {
		return false, nil
	}
	q := w.opts.Queue
	for {
		res, err := scripts.RateLimit.Run(ctx, w.rdb, []string{keys.Config(q), keys.RateLimit(q)}).Result()
		if err != nil {
			return true, fmt.Errorf("rate limit: %w", err)
		}
		status, rest, err := scripts.Status(res)
		if err != nil {
			return true, err
		}
		if status == "OK" {
			return true, nil
		}
		w.metrics.rateWait(q)
		ms, _ := scripts.Int(rest[0])
		sleep(ctx, min(time.Duration(ms)*time.Millisecond, time.Second))
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
	}
}
