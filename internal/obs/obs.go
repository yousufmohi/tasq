// Package obs holds tracing helpers shared by the client and the worker.
//
// Trace context crosses the queue in the job hash (fields traceparent and
// tracestate, W3C format). The worker's span is a new root *linked* to the
// producer's span rather than its child, following the OpenTelemetry
// messaging conventions: a job may run long after it was enqueued, and more
// than once, so parent/child timing would be misleading.
package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TracerName identifies taskq's instrumentation scope.
const TracerName = "github.com/yousufmohi/tasq"

// Attribute keys (OpenTelemetry messaging semantic conventions).
var (
	System      = attribute.String("messaging.system", "taskq")
	Destination = attribute.Key("messaging.destination.name")
	MessageID   = attribute.Key("messaging.message.id")
	Operation   = attribute.Key("messaging.operation.type")
	Attempt     = attribute.Key("taskq.attempt")
	JobType     = attribute.Key("taskq.job.type")
)

// Inject serialises ctx's span context.
func Inject(ctx context.Context, p propagation.TextMapPropagator) (traceparent, tracestate string) {
	c := propagation.MapCarrier{}
	p.Inject(ctx, c)
	return c.Get("traceparent"), c.Get("tracestate")
}

// Extract returns the span context stored on a job (invalid if none).
func Extract(p propagation.TextMapPropagator, traceparent, tracestate string) trace.SpanContext {
	if traceparent == "" {
		return trace.SpanContext{}
	}
	c := propagation.MapCarrier{"traceparent": traceparent}
	if tracestate != "" {
		c["tracestate"] = tracestate
	}
	return trace.SpanContextFromContext(p.Extract(context.Background(), c))
}
