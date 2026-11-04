// Package job holds the Job record type and its encoding to and from the
// Redis job hash. It is re-exported as taskq.Job.
package job

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/yousufmo27/taskq/internal/state"
)

// Job is a snapshot of a job record.
type Job struct {
	ID          string
	Queue       string
	Type        string
	Payload     []byte
	Priority    string
	State       state.State
	Attempts    int
	MaxAttempts int
	CreatedAt   time.Time     // Redis clock
	RunAt       time.Time     // Redis clock; when the job became (or becomes) runnable
	StartedAt   time.Time     // Redis clock; zero until first start
	FinishedAt  time.Time     // Redis clock; zero until terminal
	Timeout     time.Duration // per-attempt handler timeout; 0 = worker default
	LastError   string
	LeaseOwner  string

	// IdemKey is the enqueue idempotency key, if the producer supplied one.
	IdemKey string

	// TraceParent and TraceState carry the producer's W3C trace context.
	TraceParent, TraceState string

	// StreamEntry is the ID of the job's live stream entry, if any.
	StreamEntry string
	// LeaseToken fences state changes; only the worker holding the current
	// token may complete, fail or release the job.
	LeaseToken string
}

// IdempotencyKey is the key handlers should pass to downstream systems to
// make side effects idempotent. It is identical on every delivery of the
// job: the producer's idempotency key if one was given, else the job ID.
func (j *Job) IdempotencyKey() string {
	if j.IdemKey != "" {
		return j.IdemKey
	}
	return j.ID
}

// NewID returns a random 128-bit hex identifier.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return hex.EncodeToString(b[:])
}

// FromFlat decodes the flat [field, value, ...] reply of HGETALL as returned
// from a Lua script.
func FromFlat(flat []any) (*Job, error) {
	if len(flat)%2 != 0 {
		return nil, fmt.Errorf("job: odd field count %d", len(flat))
	}
	m := make(map[string]string, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		k, ok1 := flat[i].(string)
		v, ok2 := flat[i+1].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("job: non-string field at %d", i)
		}
		m[k] = v
	}
	return FromMap(m)
}

// FromMap decodes a job hash.
func FromMap(m map[string]string) (*Job, error) {
	if m["id"] == "" {
		return nil, fmt.Errorf("job: hash has no id")
	}
	j := &Job{
		ID:          m["id"],
		Queue:       m["queue"],
		Type:        m["type"],
		Payload:     []byte(m["payload"]),
		Priority:    m["priority"],
		State:       state.State(m["state"]),
		LastError:   m["last_error"],
		LeaseOwner:  m["lease_owner"],
		LeaseToken:  m["lease_token"],
		StreamEntry: m["stream_entry"],
		IdemKey:     m["idem_key"],
		TraceParent: m["traceparent"],
		TraceState:  m["tracestate"],
	}
	var err error
	if j.RunAt, err = ms(m, "run_at_ms"); err != nil {
		return nil, err
	}
	var timeout int
	if timeout, err = atoi(m, "timeout_ms"); err != nil {
		return nil, err
	}
	j.Timeout = time.Duration(timeout) * time.Millisecond
	if j.Attempts, err = atoi(m, "attempts"); err != nil {
		return nil, err
	}
	if j.MaxAttempts, err = atoi(m, "max_attempts"); err != nil {
		return nil, err
	}
	if j.CreatedAt, err = ms(m, "created_ms"); err != nil {
		return nil, err
	}
	if j.StartedAt, err = ms(m, "started_ms"); err != nil {
		return nil, err
	}
	if j.FinishedAt, err = ms(m, "finished_ms"); err != nil {
		return nil, err
	}
	return j, nil
}

func atoi(m map[string]string, k string) (int, error) {
	v, ok := m[k]
	if !ok || v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("job: field %s=%q: %w", k, v, err)
	}
	return n, nil
}

func ms(m map[string]string, k string) (time.Time, error) {
	v, ok := m[k]
	if !ok || v == "" {
		return time.Time{}, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("job: field %s=%q: %w", k, v, err)
	}
	return time.UnixMilli(n), nil
}
