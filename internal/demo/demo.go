// Package demo provides the job handler used by cmd/taskq-worker, the
// failure tests and the benchmarks.
//
// A demo job's payload is JSON:
//
//	{"sleep_ms": 100, "fail_pct": 10, "crash": false}
//
// With a Recorder, the handler also writes execution and side-effect
// counters to Redis so that tests can count duplicates:
//
//	exec        HINCRBY per handler invocation (before the work)
//	effect      HINCRBY per completed side effect (naive, not idempotent)
//	once_effect HINCRBY guarded by idem.Once
//	applied     SADD of the job's idempotency key (an idempotent downstream)
package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/idem"
	"github.com/yousufmohi/tasq/worker"
)

// Payload is the demo job's input.
type Payload struct {
	SleepMS int  `json:"sleep_ms,omitempty"`
	FailPct int  `json:"fail_pct,omitempty"` // 0-100: chance of a retryable failure
	Crash   bool `json:"crash,omitempty"`    // exit the process mid-job (poison pill)
}

// Encode marshals p.
func (p Payload) Encode() []byte {
	b, _ := json.Marshal(p)
	return b
}

// Recorder writes duplicate-detection counters. Use a Redis connection that
// does not share fate with the queue's connection when testing partitions:
// it stands in for an external downstream.
type Recorder struct {
	rdb  redis.UniversalClient
	q    string
	once *idem.Store
}

// NewRecorder records into rdb under keys for queue q. lockTTL is the
// idem.Once lock TTL; match it to the worker's visibility timeout so that a
// lock left by a killed worker expires by the time its job is reclaimed.
func NewRecorder(rdb redis.UniversalClient, q string, lockTTL time.Duration) *Recorder {
	s := idem.New(rdb)
	s.LockTTL = lockTTL
	return &Recorder{rdb: rdb, q: q, once: s}
}

// Key names; exported for the verifiers.
func ExecKey(q string) string       { return "taskqtest:{" + q + "}:exec" }
func EffectKey(q string) string     { return "taskqtest:{" + q + "}:effect" }
func AppliedKey(q string) string    { return "taskqtest:{" + q + "}:applied" }
func OnceEffectKey(q string) string { return "taskqtest:{" + q + "}:once_effect" }

// Handler returns the demo handler. rec may be nil.
func Handler(rec *Recorder) worker.Handler {
	return func(ctx context.Context, j *taskq.Job) error {
		var p Payload
		if len(j.Payload) > 0 {
			if err := json.Unmarshal(j.Payload, &p); err != nil {
				return taskq.Permanent(fmt.Errorf("decode payload: %w", err))
			}
		}
		if rec != nil {
			if err := rec.rdb.HIncrBy(ctx, ExecKey(rec.q), j.ID, 1).Err(); err != nil {
				return fmt.Errorf("record exec: %w", err)
			}
		}
		if p.Crash {
			// Die without running deferred functions or reporting anything,
			// exactly as an OOM kill or segfault would look to Redis.
			os.Exit(3)
		}
		if p.SleepMS > 0 {
			t := time.NewTimer(time.Duration(p.SleepMS) * time.Millisecond)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
		if p.FailPct > 0 && rand.IntN(100) < p.FailPct {
			return errors.New("demo: injected failure")
		}
		if rec == nil {
			return nil
		}
		// Three versions of the same side effect, to compare in reports:
		// 1. naive: repeats on every duplicate delivery;
		if err := rec.rdb.HIncrBy(ctx, EffectKey(rec.q), j.ID, 1).Err(); err != nil {
			return fmt.Errorf("record effect: %w", err)
		}
		// 2. guarded by idem.Once: repeats only inside its D1 window;
		_, err := rec.once.Once(ctx, "demo:"+rec.q+":"+j.IdempotencyKey(), func(ctx context.Context) error {
			return rec.rdb.HIncrBy(ctx, OnceEffectKey(rec.q), j.ID, 1).Err()
		})
		if err != nil {
			return fmt.Errorf("record once-effect: %w", err)
		}
		// 3. an idempotent downstream keyed by the idempotency key: never
		// repeats.
		if err := rec.rdb.SAdd(ctx, AppliedKey(rec.q), j.IdempotencyKey()).Err(); err != nil {
			return fmt.Errorf("record applied: %w", err)
		}
		return nil
	}
}
