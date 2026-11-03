// Package taskq is the client library: it enqueues and inspects jobs.
// Workers live in package worker.
//
// Delivery is at-least-once. See docs/DESIGN.md for where duplicates occur.
package taskq

import (
	"context"
	"errors"
	"fmt"

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

// ErrNotFound is returned when a job hash does not exist (never created, or
// expired after its retention period).
var ErrNotFound = errors.New("taskq: job not found")

// Client enqueues and inspects jobs. It is safe for concurrent use.
type Client struct {
	rdb redis.UniversalClient
}

// NewClient wraps an existing go-redis client. The caller owns rdb.
func NewClient(rdb redis.UniversalClient) *Client { return &Client{rdb: rdb} }

// EnqueueOptions describes a job to enqueue.
type EnqueueOptions struct {
	Type        string
	Payload     []byte
	MaxAttempts int // default DefaultMaxAttempts
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
	id := job.NewID()
	prio := keys.DefaultPriority
	res, err := scripts.Enqueue.Run(ctx, c.rdb,
		[]string{keys.Job(queue, id), keys.Stream(queue, prio)},
		id, queue, o.Type, o.Payload, o.MaxAttempts, prio,
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
