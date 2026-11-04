// Package idem guards side effects against re-execution when a job is
// delivered more than once.
//
// # What it does and does not guarantee
//
// Once(key, fn) runs fn unless a previous call with the same key already
// completed it. That suppresses re-execution for duplicates that arrive
// after the first run recorded completion: a redelivery after a lost ack
// (D3 in docs/DESIGN.md), a released job whose zombie handler finished (D5),
// or a reclaimed job whose original run finished first (D2).
//
// It cannot close D1: if the process dies after fn's side effect but before
// the completion marker is written, the next delivery runs fn again. The
// side effect and the marker are in different systems and cannot be written
// atomically. The only complete fix is a downstream that deduplicates by
// key itself (a payment API's idempotency key, INSERT ... ON CONFLICT DO
// NOTHING, a unique constraint), or writing the side effect and the marker
// in one database transaction.
//
// A lock with a TTL prevents concurrent runs of the same key in the common
// case. If fn outlives the lock TTL, a second caller can acquire it and run
// fn concurrently; choose LockTTL above fn's worst-case duration.
package idem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq/internal/job"
)

// ErrInProgress means another caller holds the lock for this key and has
// not finished. Return it from a handler to retry later.
var ErrInProgress = errors.New("idem: operation in progress elsewhere")

// acquire: KEYS[1] done marker, KEYS[2] lock; ARGV[1] token, ARGV[2] lock TTL ms.
// Atomic so that "not done yet" and "lock taken" are observed together.
var acquire = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 'DONE' end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) then return 'OK' end
return 'BUSY'
`)

// finish: KEYS[1] done marker, KEYS[2] lock; ARGV[1] token, ARGV[2] done TTL ms.
// The marker is written even if our lock expired: the effect did happen.
var finish = redis.NewScript(`
redis.call('SET', KEYS[1], '1', 'PX', ARGV[2])
if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
return 'OK'
`)

// abort: KEYS[1] lock; ARGV[1] token. Compare-and-delete, so we never
// release a lock another caller acquired after ours expired.
var abort = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

// Store records completed operations in Redis.
type Store struct {
	rdb redis.UniversalClient
	// LockTTL bounds how long a crashed caller blocks others. Default 5m.
	LockTTL time.Duration
	// DoneTTL is how long completion is remembered. It must exceed the
	// window in which duplicates can arrive (max retry span). Default 7 days.
	DoneTTL time.Duration
}

// New returns a Store with default TTLs.
func New(rdb redis.UniversalClient) *Store {
	return &Store{rdb: rdb, LockTTL: 5 * time.Minute, DoneTTL: 7 * 24 * time.Hour}
}

func doneKey(k string) string { return "taskq:once:{" + k + "}:done" }
func lockKey(k string) string { return "taskq:once:{" + k + "}:lock" }

// Once runs fn unless an earlier call with key completed. It reports whether
// fn ran. If fn returns an error the lock is released and nothing is
// recorded, so a later call runs fn again.
func (s *Store) Once(ctx context.Context, key string, fn func(context.Context) error) (ran bool, err error) {
	if key == "" {
		return false, errors.New("idem: empty key")
	}
	token := job.NewID()
	res, err := acquire.Run(ctx, s.rdb, []string{doneKey(key), lockKey(key)},
		token, s.LockTTL.Milliseconds()).Text()
	if err != nil {
		return false, fmt.Errorf("idem: acquire: %w", err)
	}
	switch res {
	case "DONE":
		return false, nil
	case "BUSY":
		return false, ErrInProgress
	}
	if err := fn(ctx); err != nil {
		// Use a fresh context: ctx may be the reason fn failed.
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = abort.Run(actx, s.rdb, []string{lockKey(key)}, token).Err()
		return true, err
	}
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := finish.Run(fctx, s.rdb, []string{doneKey(key), lockKey(key)},
		token, s.DoneTTL.Milliseconds()).Err(); err != nil {
		// fn's effect happened but is not recorded: the D1 window. Report
		// it so the caller can decide; a retry will run fn again.
		return true, fmt.Errorf("idem: effect applied but not recorded: %w", err)
	}
	return true, nil
}

// Done reports whether key has completed.
func (s *Store) Done(ctx context.Context, key string) (bool, error) {
	n, err := s.rdb.Exists(ctx, doneKey(key)).Result()
	return n == 1, err
}
