package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/backoff"
	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
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

func (w *Worker) process(ctx context.Context, d delivery) {
	id, _ := d.msg.Values["id"].(string)
	log := w.log.With("job_id", id, "entry", d.msg.ID)
	if id == "" {
		// Not written by taskq. Leave it in the PEL for inspection rather
		// than silently deleting data we don't understand.
		log.Error("stream entry without job id")
		return
	}
	q := w.opts.Queue
	jkey := keys.Job(q, id)
	token := job.NewID()

	res, err := scripts.Start.Run(ctx, w.rdb, []string{jkey, d.stream, keys.Dead(q)},
		keys.Group, d.msg.ID, w.opts.Consumer, token, state.SourcesCSV(state.Start)).Result()
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
		log.Error("job dead-lettered: attempts exhausted without a reported result")
		return
	default:
		log.Warn("delivery skipped", "status", status, "reason", rest)
		return
	}
	flat, _ := rest[0].([]any)
	j, err := job.FromFlat(flat)
	if err != nil {
		log.Error("decode job", "err", err)
		return
	}
	log = log.With("type", j.Type, "attempt", j.Attempts, "reclaimed", d.reclaimed)

	jctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	w.leases.add(&inflight{
		jkey: jkey, stream: d.stream, entry: d.msg.ID, token: token,
		cancel: cancel, lastRenewed: time.Now(),
	})
	herr := w.call(jctx, j)
	w.leases.remove(d.msg.ID)

	if herr == nil {
		w.complete(ctx, log, d, jkey, token)
		return
	}
	if cause := context.Cause(jctx); errors.Is(cause, errLeaseLost) || errors.Is(cause, errLeaseExpired) {
		// The job may belong to another worker now. Reporting a failure
		// would be fenced anyway (LEASE_LOST) or, if our lease turns out to
		// be intact, would burn a retry for a failure we caused ourselves.
		// Leaving the entry in the PEL lets reclaim decide.
		log.Warn("handler stopped after lease loss; leaving job for reclaim", "cause", cause, "err", herr)
		return
	}
	w.fail(ctx, log, d, j, jkey, token, herr)
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

func (w *Worker) complete(ctx context.Context, log *slog.Logger, d delivery, jkey, token string) {
	retention := w.opts.Retention.Milliseconds()
	if w.opts.Retention < 0 {
		retention = 0
	}
	res, err := scripts.Complete.Run(ctx, w.rdb, []string{jkey, d.stream},
		keys.Group, d.msg.ID, token, retention, state.SourcesCSV(state.Complete)).Result()
	if err != nil {
		// The job is still in the PEL and will be re-delivered: a duplicate,
		// not a loss.
		log.Error("complete failed", "err", err)
		return
	}
	if status, rest, _ := scripts.Status(res); status != "OK" {
		log.Warn("complete rejected", "status", status, "detail", rest)
		return
	}
	log.Debug("job succeeded")
}

func (w *Worker) fail(ctx context.Context, log *slog.Logger, d delivery, j *taskq.Job, jkey, token string, herr error) {
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
	res, err := scripts.Fail.Run(ctx, w.rdb,
		[]string{jkey, d.stream, keys.Sched(q), keys.Dead(q)},
		keys.Group, d.msg.ID, token, msg, delay.Milliseconds(), permanent,
		state.SourcesCSV(state.Fail)).Result()
	if err != nil {
		// Still in the PEL: it will be reclaimed and re-run.
		log.Error("fail failed", "err", err, "handler_err", herr)
		return
	}
	status, rest, _ := scripts.Status(res)
	switch status {
	case "RETRY":
		log.Warn("job failed, retry scheduled", "err", herr, "delay", delay.Round(time.Millisecond))
	case "DEAD":
		log.Error("job dead-lettered", "err", herr, "permanent", permanent == "1")
	default:
		log.Warn("fail rejected", "status", status, "detail", rest, "handler_err", herr)
	}
}
