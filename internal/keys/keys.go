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

// DefaultPriority is the only priority level until milestone 5.
const DefaultPriority = "default"

var queueRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// ValidateQueue rejects names that could break key structure ('{', '}', ':')
// or are unreasonably long.
func ValidateQueue(q string) error {
	if !queueRe.MatchString(q) {
		return fmt.Errorf("taskq: invalid queue name %q (want [A-Za-z0-9_.-]{1,64})", q)
	}
	return nil
}

func base(q string) string { return "taskq:{" + q + "}" }

// Job is the hash holding a job's record and state.
func Job(q, id string) string { return base(q) + ":job:" + id }

// Stream is the delivery stream for one priority level of a queue.
func Stream(q, priority string) string { return base(q) + ":s:" + priority }
