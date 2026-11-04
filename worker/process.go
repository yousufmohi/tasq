package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/backoff"
	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/obs"
	"github.com/yousufmo27/taskq/internal/scripts"
	"github.com/yousufmo27/taskq/internal/state"
)

// maxErrorLen bounds the error message stored on the job hash.
const maxErrorLen = 4096

// delivery is a stream entry this worker has read and is about to start.
type delivery struct {
	stream    string
	msg       redis.XMessage
	reclaimed bool
}

// process starts, runs and settles one delivery. ctx is used for Redis calls
// and is never cancelled by shutdown, so a job can always be completed,
// failed or released. handlerBase is the parent of the handler's context;
// shutdown cancels it once the drain timeout expires.
func (w *Worker) process(ctx, handlerBase context.Context, d delivery) {
	id, _ := d.msg.Values["id"].(string)
	q := w.opts.Queue
	log := w.log.With("job_id", id, "entry", d.msg.ID)
	if id == "" {
		// Not written by taskq. Leave it in the PEL for inspection rather
		// than silently deleting data we don't understand.
		log.Error("stream entry without job id")
		return
	}
	jkey := keys.Job(q, id)
	token := job.NewID()

	done := w.metrics.scriptTimer("start")
	res, err := scripts.Start.Run(ctx, w.rdb, []string{jkey, d.stream, keys.Dead(q)},
		keys.Group, d.msg.ID, w.opts.Consumer, token, state.SourcesCSV(state.Start)).Result()
	done()
	if err != nil {
		log.Error("start failed", "err", err)
		return
	}
	status, rest, err := scripts.Status(res)
	if err != nil {
		log.Error("start reply", "err", err)
		return
	}
	switch status {
	case "OK":
	case "DEAD":
		w.metrics.outcome(q, OutcomeExhausted, 0)
		log.Error("job dead-lettered: attempts exhausted without a reported result")
		return
	default:
		w.metrics.outcome(q, OutcomeSkipped, 0)
		log.Warn("delivery skipped", "status", status, "reason", rest)
		return
	}
	flat, _ := rest[0].([]any)
	j, err := job.FromFlat(flat)
	if err != nil {
		log.Error("decode job", "err", err)
		return
	}
	w.metrics.jobStarted(q, j.Priority, j.StartedAt.Sub(j.RunAt), d.reclaimed)
	defer w.metrics.jobDone(q)

	// A new root span linked to the producer's span (see package obs).
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithAttributes(obs.System, obs.Destination.String(q), obs.MessageID.String(j.ID),
			obs.Operation.String("process"), obs.JobType.String(j.Type), obs.Attempt.Int(j.Attempts)),
	}
	if sc := obs.Extract(w.opts.Propagator, j.TraceParent, j.TraceState); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	jctx, cancel := context.WithCancelCause(handlerBase)
	defer cancel(nil)
	jctx, span := w.tracer.Start(jctx, "process "+q, opts...)
	defer span.End()

	log = log.With("type", j.Type, "attempt", j.Attempts, "reclaimed", d.reclaimed)
	if sc := span.SpanContext(); sc.IsValid() {
		log = log.With("trace_id", sc.TraceID().String())
	}

	w.leases.add(&inflight{
		jkey: jkey, stream: d.stream, entry: d.msg.ID, token: token,
		cancel: cancel, lastRenewed: time.Now(),
	})
	t0 := time.Now()
	herr := w.call(jctx, j)
	took := time.Since(t0)
	w.leases.remove(d.msg.ID)

	if herr == nil {
		w.complete(ctx, log, d, j, jkey, token, took)
		return
	}
	span.RecordError(herr)
	span.SetStatus(codes.Error, "handler failed")

	cause := context.Cause(jctx)
	if errors.Is(cause, errShutdown) {
		// We interrupted it; the job did nothing wrong. Hand it back now
		// rather than burning an attempt and a backoff delay.
		if w.release(ctx, jkey, d.stream, d.msg.ID, token) {
			w.metrics.outcome(q, OutcomeReleased, took)
		}
		return
	}
	if errors.Is(cause, errLeaseLost) || errors.Is(cause, errLeaseExpired) {
		// The job may belong to another worker now. Reporting a failure
		// would be fenced anyway (LEASE_LOST) or, if our lease turns out to
		// be intact, would burn a retry for a failure we caused ourselves.
		// Leaving the entry in the PEL lets reclaim decide.
		w.metrics.outcome(q, OutcomeLeaseLost, took)
		log.Warn("handler stopped after lease loss; leaving job for reclaim", "cause", cause, "err", herr)
		return
	}
	w.fail(ctx, log, d, j, jkey, token, herr, took)
}

// call runs the handler with the job's timeout, converting a panic into an
// error so one bad job cannot take down the worker.
func (w *Worker) call(ctx context.Context, j *taskq.Job) (err error) {
	timeout := j.Timeout
	if timeout <= 0 {
		timeout = w.opts.JobTimeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	// A handler that ignores its deadline and returns nil late has still
	// done its work, so nil is passed through and the job completes; the
	// timeout only bounds how long we wait for a cooperative handler.
	return w.h(ctx, j)
}

func (w *Worker) complete(ctx context.Context, log *slog.Logger, d delivery, j *taskq.Job, jkey, token string, took time.Duration) {
	q := w.opts.Queue
	retention := w.opts.Retention.Milliseconds()
	if w.opts.Retention < 0 {
		retention = 0
	}
	done := w.metrics.scriptTimer("complete")
	res, err := scripts.Complete.Run(ctx, w.rdb, []string{jkey, d.stream},
		keys.Group, d.msg.ID, token, retention, state.SourcesCSV(state.Complete)).Result()
	done()
	if err != nil {
		// The job is still in the PEL and will be re-delivered: a duplicate,
		// not a loss.
		log.Error("complete failed", "err", err)
		return
	}
	status, rest, _ := scripts.Status(res)
	if status != "OK" {
		w.metrics.outcome(q, OutcomeLeaseLost, took)
		log.Warn("complete rejected", "status", status, "detail", rest)
		return
	}
	w.metrics.outcome(q, OutcomeSucceeded, took)
	if finished, err := scripts.Int(rest[0]); err == nil {
		w.metrics.endToEnd(q, time.UnixMilli(finished).Sub(j.CreatedAt))
	}
	log.Debug("job succeeded", "duration", took)
}

func (w *Worker) fail(ctx context.Context, log *slog.Logger, d delivery, j *taskq.Job, jkey, token string, herr error, took time.Duration) {
	msg := herr.Error()
	if len(msg) > maxErrorLen {
		msg = msg[:maxErrorLen]
	}
	delay := backoff.Full(j.Attempts, w.opts.RetryBase, w.opts.RetryMax, nil)
	permanent := "0"
	if taskq.IsPermanent(herr) {
		permanent = "1"
	}
	q := w.opts.Queue
	done := w.metrics.scriptTimer("fail")
	res, err := scripts.Fail.Run(ctx, w.rdb,
		[]string{jkey, d.stream, keys.Sched(q), keys.Dead(q)},
		keys.Group, d.msg.ID, token, msg, delay.Milliseconds(), permanent,
		state.SourcesCSV(state.Fail)).Result()
	done()
	if err != nil {
		// Still in the PEL: it will be reclaimed and re-run.
		log.Error("fail failed", "err", err, "handler_err", herr)
		return
	}
	status, rest, _ := scripts.Status(res)
	switch status {
	case "RETRY":
		w.metrics.outcome(q, OutcomeRetried, took)
		log.Warn("job failed, retry scheduled", "err", herr, "delay", delay.Round(time.Millisecond))
	case "DEAD":
		w.metrics.outcome(q, OutcomeDead, took)
		log.Error("job dead-lettered", "err", herr, "permanent", permanent == "1")
	default:
		w.metrics.outcome(q, OutcomeLeaseLost, took)
		log.Warn("fail rejected", "status", status, "detail", rest, "handler_err", herr)
	}
}
