//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/api"
	"github.com/yousufmohi/tasq/internal/testutil"
	"github.com/yousufmohi/tasq/worker"
)

// metricValue returns the value of the series name{labels} (counter, gauge
// or histogram sample count), or -1 if absent.
func metricValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue series
				}
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return -1
}

func TestWorkerMetrics(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	reg := prometheus.NewRegistry()
	o := worker.Options{Queue: q, Concurrency: 2, Metrics: worker.NewMetrics(reg)}
	runWorker(t, newWorkerOpts(t, rdb, o, func(_ context.Context, j *taskq.Job) error {
		if j.Type == "bad" {
			return taskq.Permanent(errors.New("no"))
		}
		return nil
	}))
	var ids []string
	for range 3 {
		id, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "good", Priority: taskq.PriorityHigh})
		ids = append(ids, id)
	}
	bad, _ := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "bad"})
	for _, id := range ids {
		waitState(t, c, q, id, taskq.StateSucceeded)
	}
	waitState(t, c, q, bad, taskq.StateDead)

	checks := []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"taskq_jobs_started_total", map[string]string{"queue": q, "priority": "high"}, 3},
		{"taskq_jobs_started_total", map[string]string{"queue": q, "priority": "default"}, 1},
		{"taskq_jobs_finished_total", map[string]string{"queue": q, "outcome": "succeeded"}, 3},
		{"taskq_jobs_finished_total", map[string]string{"queue": q, "outcome": "dead"}, 1},
		{"taskq_job_duration_seconds", map[string]string{"queue": q, "outcome": "succeeded"}, 3},
		{"taskq_job_end_to_end_seconds", map[string]string{"queue": q}, 3},
		{"taskq_queue_wait_seconds", map[string]string{"queue": q, "priority": "high"}, 3},
		{"taskq_jobs_in_flight", map[string]string{"queue": q}, 0},
		{"taskq_redis_script_duration_seconds", map[string]string{"script": "complete"}, 3},
	}
	testutil.WaitFor(t, 5*time.Second, "metrics settle", func() bool {
		return metricValue(t, reg, "taskq_jobs_finished_total", map[string]string{"queue": q, "outcome": "dead"}) == 1
	})
	for _, c := range checks {
		if got := metricValue(t, reg, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
}

// The producer span's context travels on the job; the consumer span is a
// new root that links to it, and spans the handler creates are children of
// the consumer span.
func TestTracePropagation(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })
	prop := propagation.TraceContext{}

	c := taskq.NewClient(rdb, taskq.WithTracerProvider(tp), taskq.WithPropagator(prop))
	handlerSpan := make(chan trace.SpanContext, 1)
	o := worker.Options{Queue: q, TracerProvider: tp, Propagator: prop}
	runWorker(t, newWorkerOpts(t, rdb, o, func(ctx context.Context, _ *taskq.Job) error {
		_, s := tp.Tracer("handler").Start(ctx, "charge card")
		handlerSpan <- s.SpanContext()
		s.End()
		return nil
	}))

	id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, c, q, id, taskq.StateSucceeded)
	child := <-handlerSpan

	var producer, consumer sdktrace.ReadOnlySpan
	testutil.WaitFor(t, 5*time.Second, "spans ended", func() bool {
		for _, s := range sr.Ended() {
			switch s.Name() {
			case "send " + q:
				producer = s
			case "process " + q:
				consumer = s
			}
		}
		return producer != nil && consumer != nil
	})
	if producer.SpanKind() != trace.SpanKindProducer || consumer.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("kinds: %v %v", producer.SpanKind(), consumer.SpanKind())
	}
	if consumer.Parent().IsValid() {
		t.Fatal("consumer span should be a new root, not a child of the producer")
	}
	links := consumer.Links()
	if len(links) != 1 || links[0].SpanContext.SpanID() != producer.SpanContext().SpanID() ||
		links[0].SpanContext.TraceID() != producer.SpanContext().TraceID() {
		t.Fatalf("consumer links %+v do not point at producer %v", links, producer.SpanContext())
	}
	if child.TraceID() != consumer.SpanContext().TraceID() {
		t.Fatal("handler span is not in the consumer's trace")
	}
	found := false
	for _, a := range consumer.Attributes() {
		if string(a.Key) == "messaging.message.id" && a.Value.AsString() == id {
			found = true
		}
	}
	if !found {
		t.Fatal("consumer span lacks messaging.message.id")
	}
}

func TestAPIMetricsAndQueueGauges(t *testing.T) {
	t.Parallel()
	q := testutil.Queue(t)
	reg := prometheus.NewRegistry()
	a := newAPIWith(t, api.Options{Registry: reg})

	a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{"type": "x", "priority": "low"})
	a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{"type": "x", "delay_ms": 60000})
	a.do("POST", "/v1/queues/bad:q/jobs", map[string]any{"type": "x"})

	if v := metricValue(t, reg, "taskq_api_requests_total",
		map[string]string{"route": "POST /v1/queues/{queue}/jobs", "code": "202"}); v < 2 {
		t.Errorf("requests_total{202} = %v, want >= 2", v)
	}
	if v := metricValue(t, reg, "taskq_enqueued_total", map[string]string{"queue": "_invalid", "result": "rejected"}); v != 1 {
		t.Errorf("invalid queue label: %v", v)
	}
	if v := metricValue(t, reg, "taskq_queue_waiting", map[string]string{"queue": q, "priority": "low"}); v != 1 {
		t.Errorf("queue_waiting{low} = %v, want 1", v)
	}
	if v := metricValue(t, reg, "taskq_queue_scheduled", map[string]string{"queue": q}); v != 1 {
		t.Errorf("queue_scheduled = %v, want 1", v)
	}
	if v := metricValue(t, reg, "taskq_queue_stats_up", nil); v != 1 {
		t.Errorf("stats_up = %v", v)
	}
}
