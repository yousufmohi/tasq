// Package backoff computes retry delays.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Full returns a "full jitter" delay for the given attempt (1-based):
// a uniformly random duration in [0, min(cap, base·2^(attempt-1))].
//
// Full jitter spreads retries of many jobs that failed together (e.g. on a
// downstream outage) across the whole window instead of synchronising them
// into waves. See "Exponential Backoff And Jitter", AWS Architecture Blog.
//
// rnd returns a uniform value in [0, n); nil uses math/rand/v2.
func Full(attempt int, base, cap time.Duration, rnd func(n int64) int64) time.Duration {
	if rnd == nil {
		rnd = rand.Int64N
	}
	ceil := Ceiling(attempt, base, cap)
	if ceil <= 0 {
		return 0
	}
	return time.Duration(rnd(int64(ceil) + 1))
}

// Ceiling is the upper bound of the jitter window for attempt:
// min(cap, base·2^(attempt-1)), safe against overflow.
func Ceiling(attempt int, base, cap time.Duration) time.Duration {
	if base <= 0 || cap <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= cap || d > cap/2 {
			return cap
		}
		d *= 2
	}
	return min(d, cap)
}
