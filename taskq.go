// Package taskq is the client library: it enqueues and inspects jobs and
// manages the dead-letter set. Workers live in package worker.
//
// Delivery is at-least-once. See docs/DESIGN.md for where duplicates occur.
package taskq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/yousufmohi/tasq/internal/job"
	"github.com/yousufmohi/tasq/internal/keys"
	"github.com/yousufmohi/tasq/internal/obs"
	"github.com/yousufmohi/tasq/internal/scripts"
	"github.com/yousufmohi/tasq/internal/state"
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
	// ErrQueueFull is returned by Enqueue when the queue's MaxDepth is
	// reached. Producers should back off and retry.
	ErrQueueFull = errors.New("taskq: queue full")
)

// Priority selects which stream a job is delivered from.
type Priority string

const (
	PriorityHigh    Priority = keys.High
	PriorityDefault Priority = keys.Default
	PriorityLow     Priority = keys.Low
)

// Client enqueues and inspects jobs. It is safe for concurrent use.
type Client struct {
	rdb    redis.UniversalClient
	queues sync.Map // queue name -> struct{}: already registered in keys.Queues
	tracer trace.Tracer
	prop   propagation.TextMapPropagator
}

// Option configures a Client.
type Option func(*clientOptions)

type clientOptions struct {
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
}

// WithTracerProvider sets the OpenTelemetry tracer provider (default: the
// global one).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *clientOptions) { o.tp = tp }
}

// WithPropagator sets how trace context is written onto jobs (default: the
// global propagator; binaries should install propagation.TraceContext).
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *clientOptions) { o.prop = p }
}

// NewClient wraps an existing go-redis client. The caller owns rdb.
func NewClient(rdb redis.UniversalClient, opts ...Option) *Client {
	o := clientOptions{tp: otel.GetTracerProvider(), prop: otel.GetTextMapPropagator()}
	for _, f := range opts {
		f(&o)
	}
	return &Client{rdb: rdb, tracer: o.tp.Tracer(obs.TracerName), prop: o.prop}
}

// EnqueueOptions describes a job to enqueue.
type EnqueueOptions struct {
	Type        string
	Payload     []byte
	Priority    Priority      // default PriorityDefault
	MaxAttempts int           // default DefaultMaxAttempts
	Timeout     time.Duration // per-attempt handler timeout; 0 = worker default

	// Delay makes the job runnable this long after Redis accepts it,
	// measured on the Redis clock. RunAt is an absolute alternative; it is
	// converted to a delay using the producer's clock, so clock skew between
	// producer and Redis shifts it. At most one may be set. A job runs at
	// its run time plus up to one promotion interval plus queue wait.
	Delay time.Duration
	RunAt time.Time

	// IdempotencyKey deduplicates enqueues: while the key is remembered
	// (IdempotencyTTL, default 24h), enqueueing again with the same key on
	// the same queue creates nothing and returns the original job's ID with
	// ErrDuplicate. Use it to make producer retries safe after a timeout.
	// The key is also what Job.IdempotencyKey() returns to handlers.
	IdempotencyKey string
	IdempotencyTTL time.Duration

	// Durability, if set, makes Enqueue wait until the write is also on
	// disk and/or on replicas before returning. See Durability.
	Durability Durability
}

// Durability asks Redis to confirm a write beyond the primary's memory
// before Enqueue returns, at the cost of one extra wait per enqueue.
//
// It narrows, but does not close, the windows in docs/DESIGN.md §8:
// WAIT/WAITAOF confirm that copies exist, yet Redis replication is still
// asynchronous and a failover may promote a replica that did not receive
// the write. When the wait times out, Enqueue returns the job ID together
// with ErrDurabilityUncertain: the job exists on the primary but its
// durability is unconfirmed. Retrying with the same IdempotencyKey is safe.
type Durability struct {
	// LocalFsync waits until the primary has fsynced the write to its AOF
	// (WAITAOF 1 ...), so a crash of the primary's host cannot lose it.
	// Requires appendonly yes.
	LocalFsync bool
	// Replicas waits until this many replicas acknowledge the write
	// (WAIT n), or have fsynced it when LocalFsync is also set (WAITAOF).
	Replicas int
	// Timeout bounds the wait. Default 1s.
	Timeout time.Duration
}

// ErrDurabilityUncertain: the job was enqueued on the primary, but the
// requested Durability was not confirmed within its timeout.
var ErrDurabilityUncertain = errors.New("taskq: durability not confirmed")

// enqueueDurable runs the enqueue script and the wait in one pipeline: WAIT
// and WAITAOF only cover writes made earlier on the *same connection*, and
// a pipeline is guaranteed to use one connection. EVAL (not EVALSHA) is used
// so that a flushed script cache cannot fail the pipeline.
func (c *Client) enqueueDurable(ctx context.Context, d Durability, keyz []string, args []any) (res any, waitErr, err error) {
	if d.Replicas < 0 || d.Timeout < 0 {
		return nil, nil, invalidf("Durability values must be >= 0")
	}
	timeout := d.Timeout
	if timeout == 0 {
		timeout = time.Second
	}
	pipe := c.rdb.Pipeline()
	evalCmd := scripts.Enqueue.Eval(ctx, pipe, keyz, args...)
	var waitCmd *redis.Cmd
	if d.LocalFsync {
		waitCmd = pipe.Do(ctx, "WAITAOF", 1, d.Replicas, timeout.Milliseconds())
	} else {
		waitCmd = pipe.Do(ctx, "WAIT", d.Replicas, timeout.Milliseconds())
	}
	_, _ = pipe.Exec(ctx) // errors are read per command below
	if res, err = evalCmd.Result(); err != nil {
		return nil, nil, err
	}
	w, err := waitCmd.Result()
	if err != nil {
		return res, fmt.Errorf("%w: %v", ErrDurabilityUncertain, err), nil
	}
	if d.LocalFsync {
		// WAITAOF replies [local fsynced, replicas fsynced].
		arr, _ := w.([]any)
		if len(arr) != 2 {
			return res, fmt.Errorf("%w: unexpected WAITAOF reply %v", ErrDurabilityUncertain, w), nil
		}
		local, _ := arr[0].(int64)
		reps, _ := arr[1].(int64)
		if local < 1 || reps < int64(d.Replicas) {
			return res, fmt.Errorf("%w: fsynced locally=%d replicas=%d", ErrDurabilityUncertain, local, reps), nil
		}
		return res, nil, nil
	}
	n, _ := w.(int64)
	if n < int64(d.Replicas) {
		return res, fmt.Errorf("%w: %d of %d replicas acknowledged", ErrDurabilityUncertain, n, d.Replicas), nil
	}
	return res, nil, nil
}

// MaxIdempotencyKeyLen bounds IdempotencyKey.
const MaxIdempotencyKeyLen = 256

// ErrDuplicate is returned by Enqueue, together with the existing job's ID,
// when the IdempotencyKey was already used. It signals success-by-dedup,
// not a failure.
var ErrDuplicate = errors.New("taskq: duplicate idempotency key")

// Enqueue creates a job on queue and returns its ID. A nil error means Redis
// applied the write on the primary; see docs/DESIGN.md §8 for what that does
// and does not guarantee across a crash or failover.
//
// Enqueue records a producer span and stores its trace context on the job, so
// the worker's span can link back to it.
func (c *Client) Enqueue(ctx context.Context, queue string, o EnqueueOptions) (id string, err error) {
	ctx, span := c.tracer.Start(ctx, "send "+queue,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(obs.System, obs.Destination.String(queue),
			obs.Operation.String("send"), obs.JobType.String(o.Type)))
	defer func() {
		if id != "" {
			span.SetAttributes(obs.MessageID.String(id))
		}
		if err != nil && !errors.Is(err, ErrDuplicate) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "enqueue failed")
		}
		span.End()
	}()
	return c.enqueue(ctx, queue, o)
}

func (c *Client) enqueue(ctx context.Context, queue string, o EnqueueOptions) (string, error) {
	if err := validQueue(queue); err != nil {
		return "", err
	}
	if o.Type == "" {
		return "", invalidf("job type is required")
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	if o.MaxAttempts < 1 {
		return "", invalidf("MaxAttempts must be >= 1, got %d", o.MaxAttempts)
	}
	if o.Timeout < 0 {
		return "", invalidf("Timeout must be >= 0, got %v", o.Timeout)
	}
	if o.Delay < 0 {
		return "", invalidf("Delay must be >= 0, got %v", o.Delay)
	}
	delay := o.Delay
	if !o.RunAt.IsZero() {
		if o.Delay != 0 {
			return "", invalidf("set Delay or RunAt, not both")
		}
		delay = max(time.Until(o.RunAt), 0)
	}
	prio := string(o.Priority)
	if prio == "" {
		prio = keys.DefaultPriority
	}
	if !keys.ValidPriority(prio) {
		return "", invalidf("unknown priority %q", prio)
	}
	if len(o.IdempotencyKey) > MaxIdempotencyKeyLen {
		return "", invalidf("IdempotencyKey longer than %d bytes", MaxIdempotencyKeyLen)
	}
	if o.IdempotencyTTL < 0 {
		return "", invalidf("IdempotencyTTL must be >= 0, got %v", o.IdempotencyTTL)
	}
	idemTTL := o.IdempotencyTTL
	if idemTTL == 0 {
		idemTTL = 24 * time.Hour
	}
	c.register(ctx, queue)
	id := job.NewID()
	streams := keys.Streams(queue)
	traceparent, tracestate := obs.Inject(ctx, c.prop)
	keyz := []string{keys.Job(queue, id), keys.Sched(queue), keys.Config(queue),
		streams[0], streams[1], streams[2], keys.Idem(queue, o.IdempotencyKey)}
	args := []any{id, queue, o.Type, o.Payload, o.MaxAttempts, prio, o.Timeout.Milliseconds(),
		delay.Milliseconds(), o.IdempotencyKey, idemTTL.Milliseconds(), traceparent, tracestate}

	var res any
	var err error
	var waitErr error
	if o.Durability == (Durability{}) {
		res, err = scripts.Enqueue.Run(ctx, c.rdb, keyz, args...).Result()
	} else {
		res, waitErr, err = c.enqueueDurable(ctx, o.Durability, keyz, args)
	}
	if err != nil {
		return "", fmt.Errorf("taskq: enqueue: %w", err)
	}
	status, rest, err := scripts.Status(res)
	if err != nil {
		return "", err
	}
	switch status {
	case "OK":
		if waitErr != nil {
			return id, waitErr
		}
		return id, nil
	case "DUP":
		existing, _ := rest[0].(string)
		return existing, ErrDuplicate
	case "FULL":
		return "", fmt.Errorf("%w (depth %v)", ErrQueueFull, rest[0])
	default:
		// A 128-bit random ID collided; treat as a bug rather than retrying.
		return "", fmt.Errorf("taskq: enqueue: unexpected status %s", status)
	}
}

// QueueConfig holds runtime settings shared by all producers and workers of
// a queue. Changes take effect immediately.
type QueueConfig struct {
	// MaxDepth rejects enqueues with ErrQueueFull once this many jobs are
	// unfinished (in streams, in flight, or scheduled). 0 = unlimited.
	MaxDepth int64
	// Rate limits job starts across all workers, in jobs per second.
	// 0 = unlimited.
	Rate float64
	// Burst is how many starts may happen back-to-back after an idle
	// period. Default 1 when Rate is set.
	Burst int64
}

// SetQueueConfig stores cfg for queue.
func (c *Client) SetQueueConfig(ctx context.Context, queue string, cfg QueueConfig) error {
	if err := validQueue(queue); err != nil {
		return err
	}
	if cfg.MaxDepth < 0 || cfg.Rate < 0 || cfg.Burst < 0 {
		return invalidf("queue config values must be >= 0")
	}
	if cfg.Rate > 0 && cfg.Burst == 0 {
		cfg.Burst = 1
	}
	c.register(ctx, queue)
	return c.rdb.HSet(ctx, keys.Config(queue),
		"max_depth", cfg.MaxDepth, "rate", cfg.Rate, "burst", cfg.Burst).Err()
}

// GetQueueConfig returns the queue's settings (zero values if never set).
func (c *Client) GetQueueConfig(ctx context.Context, queue string) (QueueConfig, error) {
	if err := validQueue(queue); err != nil {
		return QueueConfig{}, err
	}
	m, err := c.rdb.HGetAll(ctx, keys.Config(queue)).Result()
	if err != nil {
		return QueueConfig{}, err
	}
	var cfg QueueConfig
	cfg.MaxDepth, _ = strconv.ParseInt(m["max_depth"], 10, 64)
	cfg.Rate, _ = strconv.ParseFloat(m["rate"], 64)
	cfg.Burst, _ = strconv.ParseInt(m["burst"], 10, 64)
	return cfg, nil
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
	if err := validQueue(queue); err != nil {
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
	if err := validQueue(queue); err != nil {
		return nil, err
	}
	if offset < 0 || limit <= 0 {
		// Redis would read a negative index from the end of the set.
		return nil, invalidf("offset must be >= 0 and limit > 0")
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
	if err := validQueue(queue); err != nil {
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
	if err := validQueue(queue); err != nil {
		return 0, err
	}
	if limit <= 0 {
		// ZRANGE 0 -1 would replay the entire dead-letter set.
		return 0, invalidf("limit must be > 0")
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
	if err := validQueue(queue); err != nil {
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

// PriorityStats describes one priority stream.
type PriorityStats struct {
	// Waiting entries have not been delivered to any worker yet.
	Waiting int64 `json:"waiting"`
	// InFlight entries were delivered and not yet acknowledged (the PEL).
	InFlight int64 `json:"in_flight"`
}

// Stats is a point-in-time view of a queue. Fields are read in one
// pipeline but not atomically, so they may be mutually inconsistent by a
// few jobs under load.
type Stats struct {
	Queue      string                   `json:"queue"`
	Priorities map[string]PriorityStats `json:"priorities"`
	Scheduled  int64                    `json:"scheduled"` // delayed + retry-waiting
	Dead       int64                    `json:"dead"`
}

// Stats returns queue depth by priority, in-flight, scheduled and dead
// counts.
func (c *Client) Stats(ctx context.Context, queue string) (Stats, error) {
	if err := validQueue(queue); err != nil {
		return Stats{}, err
	}
	pipe := c.rdb.Pipeline()
	streams := keys.Streams(queue)
	lens := make([]*redis.IntCmd, len(streams))
	pels := make([]*redis.XPendingCmd, len(streams))
	for i, s := range streams {
		lens[i] = pipe.XLen(ctx, s)
		pels[i] = pipe.XPending(ctx, s, keys.Group)
	}
	sched := pipe.ZCard(ctx, keys.Sched(queue))
	dead := pipe.ZCard(ctx, keys.Dead(queue))
	_, _ = pipe.Exec(ctx) // per-command errors are inspected below

	st := Stats{Queue: queue, Priorities: map[string]PriorityStats{}}
	for i, p := range keys.Priorities {
		n, err := lens[i].Result()
		if err != nil {
			return Stats{}, fmt.Errorf("taskq: stats: %w", err)
		}
		var inflight int64
		if pr, err := pels[i].Result(); err == nil {
			inflight = pr.Count
		} else if !isNoGroup(err) {
			return Stats{}, fmt.Errorf("taskq: stats: %w", err)
		}
		st.Priorities[p] = PriorityStats{Waiting: n - inflight, InFlight: inflight}
	}
	var err error
	if st.Scheduled, err = sched.Result(); err != nil {
		return Stats{}, fmt.Errorf("taskq: stats: %w", err)
	}
	if st.Dead, err = dead.Result(); err != nil {
		return Stats{}, fmt.Errorf("taskq: stats: %w", err)
	}
	return st, nil
}

// isNoGroup reports a missing stream or consumer group: no worker has run on
// that queue yet, which for stats simply means nothing is in flight.
func isNoGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOGROUP")
}

// ListQueues returns every queue that has been enqueued to or configured.
func (c *Client) ListQueues(ctx context.Context) ([]string, error) {
	qs, err := c.rdb.SMembers(ctx, keys.Queues).Result()
	if err != nil {
		return nil, fmt.Errorf("taskq: list queues: %w", err)
	}
	sort.Strings(qs)
	return qs, nil
}

// ErrInvalidArgument wraps every input-validation error returned by the
// client, so callers (such as the HTTP API) can map them to "bad request".
var ErrInvalidArgument = errors.New("taskq: invalid argument")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidArgument}, args...)...)
}

func validQueue(q string) error {
	if err := keys.ValidateQueue(q); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return nil
}
