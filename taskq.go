// Package taskq is the client library: it enqueues and inspects jobs and
// manages the dead-letter set. Workers live in package worker.
//
// Delivery is at-least-once. See docs/DESIGN.md for where duplicates occur.
package taskq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/state"
)

// Job is a snapshot of a job record.
type Job = job.Job

// State is a job's lifecycle state.
type State = state.State

// Job states.
const (
	StateScheduled = state.Scheduled
	StatePending   = state.Pending
	StateActive    = state.Active
	StateRetry     = state.Retry
	StateSucceeded = state.Succeeded
	StateDead      = state.Dead
)

// DefaultMaxAttempts bounds how many times a job may be started.
const DefaultMaxAttempts = 5

var (
	// ErrNotFound is returned when a job hash does not exist (never
	// created, or expired after its retention period).
	ErrNotFound = errors.New("taskq: job not found")
	// ErrInvalidState is returned when an operation does not apply to the
	// job's current state (e.g. replaying a job that is not dead).
	ErrInvalidState = errors.New("taskq: invalid state for operation")
)

// Client enqueues and inspects jobs. It is safe for concurrent use.
type Client struct {
	rdb    redis.UniversalClient
	queues sync.Map // queue name -> struct{}: already registered in keys.Queues
}

// NewClient wraps an existing go-redis client. The caller owns rdb.
func NewClient(rdb redis.UniversalClient) *Client { return &Client{rdb: rdb} }

// EnqueueOptions describes a job to enqueue.
type EnqueueOptions struct {
	Type        string
	Payload     []byte
	MaxAttempts int           // default DefaultMaxAttempts
	Timeout     time.Duration // per-attempt handler timeout; 0 = worker default

	// Delay makes the job runnable this long after Redis accepts it,
	// measured on the Redis clock. RunAt is an absolute alternative; it is
	// converted to a delay using the producer's clock, so clock skew between
	// producer and Redis shifts it. At most one may be set. A job runs at
	// its run time plus up to one promotion interval plus queue wait.
	Delay time.Duration
	RunAt time.Time
}

// Enqueue creates a job on queue and returns its ID. A nil error means Redis
// applied the write on the primary; see docs/DESIGN.md §8 for what that does
// and does not guarantee across a crash or failover.
func (c *Client) Enqueue(ctx context.Context, queue string, o EnqueueOptions) (string, error) {
	if err := keys.ValidateQueue(queue); err != nil {
		return "", err
	}
	if o.Type == "" {
		return "", errors.New("taskq: job type is required")
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	if o.MaxAttempts < 1 {
		return "", fmt.Errorf("taskq: MaxAttempts must be >= 1, got %d", o.MaxAttempts)
	}
	if o.Timeout < 0 {
		return "", fmt.Errorf("taskq: Timeout must be >= 0, got %v", o.Timeout)
	}
	if o.Delay < 0 {
		return "", fmt.Errorf("taskq: Delay must be >= 0, got %v", o.Delay)
	}
	delay := o.Delay
	if !o.RunAt.IsZero() {
		if o.Delay != 0 {
			return "", errors.New("taskq: set Delay or RunAt, not both")
		}
		delay = max(time.Until(o.RunAt), 0)
	}
	c.register(ctx, queue)
	id := job.NewID()
	prio := keys.DefaultPriority
	res, err := scripts.Enqueue.Run(ctx, c.rdb,
		[]string{keys.Job(queue, id), keys.Stream(queue, prio), keys.Sched(queue)},
		id, queue, o.Type, o.Payload, o.MaxAttempts, prio, o.Timeout.Milliseconds(),
		delay.Milliseconds(),
	).Result()
	if err != nil {
		return "", fmt.Errorf("taskq: enqueue: %w", err)
	}
	status, _, err := scripts.Status(res)
	if err != nil {
		return "", err
	}
	if status != "OK" {
		// A 128-bit random ID collided; treat as a bug rather than retrying.
		return "", fmt.Errorf("taskq: enqueue: unexpected status %s", status)
	}
	return id, nil
}

// register records the queue name for discovery (API, metrics). It is
// best-effort and deliberately outside the enqueue script: the registry key
// is global, so putting it in the script would make the script cross-slot.
func (c *Client) register(ctx context.Context, queue string) {
	if _, ok := c.queues.Load(queue); ok {
		return
	}
	if c.rdb.SAdd(ctx, keys.Queues, queue).Err() == nil {
		c.queues.Store(queue, struct{}{})
	}
}

// GetJob returns the current record of a job.
func (c *Client) GetJob(ctx context.Context, queue, id string) (*Job, error) {
	if err := keys.ValidateQueue(queue); err != nil {
		return nil, err
	}
	m, err := c.rdb.HGetAll(ctx, keys.Job(queue, id)).Result()
	if err != nil {
		return nil, fmt.Errorf("taskq: get job: %w", err)
	}
	if len(m) == 0 {
		return nil, ErrNotFound
	}
	return job.FromMap(m)
}

// ListDead returns up to limit dead jobs ordered by time of death, oldest
// first, starting at offset. Offset paging is fine for an operator tool; a
// DLQ that grows under the reader may shift pages.
func (c *Client) ListDead(ctx context.Context, queue string, offset, limit int64) ([]*Job, error) {
	if err := keys.ValidateQueue(queue); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	ids, err := c.rdb.ZRange(ctx, keys.Dead(queue), offset, offset+limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("taskq: list dead: %w", err)
	}
	return c.getMany(ctx, queue, ids)
}

func (c *Client) getMany(ctx context.Context, queue string, ids []string) ([]*Job, error) {
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, keys.Job(queue, id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("taskq: get jobs: %w", err)
	}
	out := make([]*Job, 0, len(ids))
	for _, cmd := range cmds {
		m := cmd.Val()
		if len(m) == 0 {
			continue // purged between the index read and the hash read
		}
		j, err := job.FromMap(m)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

// ReplayDead moves one dead job back to its stream with its attempt count
// reset. It returns ErrInvalidState if the job is not dead (e.g. a concurrent
// replay already took it) and ErrNotFound if it no longer exists.
func (c *Client) ReplayDead(ctx context.Context, queue, id string) error {
	if err := keys.ValidateQueue(queue); err != nil {
		return err
	}
	res, err := scripts.Replay.Run(ctx, c.rdb,
		[]string{keys.Job(queue, id), keys.Dead(queue)},
		keys.Prefix(queue), state.SourcesCSV(state.Replay), id).Result()
	if err != nil {
		return fmt.Errorf("taskq: replay: %w", err)
	}
	status, rest, err := scripts.Status(res)
	if err != nil {
		return err
	}
	switch status {
	case "OK":
		return nil
	case "MISSING":
		return ErrNotFound
	default:
		return fmt.Errorf("%w: job is %v", ErrInvalidState, rest)
	}
}

// ReplayDeadBatch replays up to limit of the oldest dead jobs and returns how
// many were replayed. Each replay is individually atomic.
func (c *Client) ReplayDeadBatch(ctx context.Context, queue string, limit int64) (int, error) {
	if err := keys.ValidateQueue(queue); err != nil {
		return 0, err
	}
	ids, err := c.rdb.ZRange(ctx, keys.Dead(queue), 0, limit-1).Result()
	if err != nil {
		return 0, fmt.Errorf("taskq: replay batch: %w", err)
	}
	n := 0
	for _, id := range ids {
		switch err := c.ReplayDead(ctx, queue, id); {
		case err == nil:
			n++
		case errors.Is(err, ErrInvalidState), errors.Is(err, ErrNotFound):
			// Raced with another operator; not an error for a batch.
		default:
			return n, err
		}
	}
	return n, nil
}

// PurgeDead deletes dead jobs that died at or before cutoff and returns how
// many were deleted.
func (c *Client) PurgeDead(ctx context.Context, queue string, cutoff time.Time) (int, error) {
	if err := keys.ValidateQueue(queue); err != nil {
		return 0, err
	}
	const batch = 100
	total := 0
	for {
		n, err := scripts.Purge.Run(ctx, c.rdb, []string{keys.Dead(queue)},
			keys.Prefix(queue), cutoff.UnixMilli(), batch).Int()
		if err != nil {
			return total, fmt.Errorf("taskq: purge: %w", err)
		}
		total += n
		if n < batch {
			return total, nil
		}
	}
}
