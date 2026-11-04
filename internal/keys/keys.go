// Package keys builds Redis key names. Every key for a queue carries the hash
// tag {queue}, so all of a queue's keys hash to one Redis Cluster slot and a
// single Lua script may touch any of them.
package keys

import (
	"fmt"
	"regexp"
)

// Group is the consumer group name used on every stream.
const Group = "workers"

// Priority levels. Each has its own stream.
const (
	High    = "high"
	Default = "default"
	Low     = "low"
)

// DefaultPriority is used when a job does not specify one.
const DefaultPriority = Default

// Priorities lists the levels from highest to lowest.
var Priorities = []string{High, Default, Low}

// ValidPriority reports whether p is a known priority level.
func ValidPriority(p string) bool { return p == High || p == Default || p == Low }

var queueRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// ValidateQueue rejects names that could break key structure ('{', '}', ':')
// or are unreasonably long.
func ValidateQueue(q string) error {
	if !queueRe.MatchString(q) {
		return fmt.Errorf("invalid queue name %q (want [A-Za-z0-9_.-]{1,64})", q)
	}
	return nil
}

// Prefix is the common prefix of all of a queue's keys, including the
// trailing colon. Scripts that operate on many jobs (promote, purge) derive
// job and stream keys from it; because of the hash tag those keys are in
// the same slot as the declared keys.
func Prefix(q string) string { return "taskq:{" + q + "}:" }

// Job is the hash holding a job's record and state.
func Job(q, id string) string { return Prefix(q) + "job:" + id }

// Stream is the delivery stream for one priority level of a queue.
func Stream(q, priority string) string { return Prefix(q) + "s:" + priority }

// Streams returns the streams of all priority levels, highest first.
func Streams(q string) []string {
	s := make([]string, len(Priorities))
	for i, p := range Priorities {
		s[i] = Stream(q, p)
	}
	return s
}

// Sched is the ZSET of scheduled and retry-waiting job IDs, scored by
// run-at time in ms (Redis clock).
func Sched(q string) string { return Prefix(q) + "sched" }

// Dead is the ZSET of dead job IDs, scored by time of death in ms.
func Dead(q string) string { return Prefix(q) + "dead" }

// Idem maps an enqueue idempotency key to the job ID it created.
func Idem(q, key string) string { return Prefix(q) + "idem:" + key }

// Config is the hash of runtime queue settings (rate, burst, max depth).
func Config(q string) string { return Prefix(q) + "cfg" }

// RateLimit holds the GCRA theoretical arrival time.
func RateLimit(q string) string { return Prefix(q) + "rl" }

// Queues is the set of queue names that have ever been enqueued to.
const Queues = "taskq:queues"
