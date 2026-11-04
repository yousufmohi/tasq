package worker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the worker's Prometheus instruments. A nil *Metrics is a
// valid no-op, so instrumentation never needs nil checks at call sites.
//
// Label cardinality is bounded: queue, priority (3 values), outcome
// (fixed set), script (fixed set). Job type and job ID are deliberately not
// labels.
type Metrics struct {
	started   *prometheus.CounterVec
	reclaimed *prometheus.CounterVec
	outcomes  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	wait      *prometheus.HistogramVec
	e2e       *prometheus.HistogramVec
	leaseLost *prometheus.CounterVec
	rateWaits *prometheus.CounterVec
	inFlight  *prometheus.GaugeVec
	script    *prometheus.HistogramVec
}

// Outcomes recorded in taskq_jobs_finished_total.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeRetried   = "retried"
	OutcomeDead      = "dead"
	OutcomeExhausted = "exhausted" // dead at start: crashed/lost every attempt
	OutcomeReleased  = "released"  // handed back on shutdown
	OutcomeLeaseLost = "lease_lost"
	OutcomeSkipped   = "skipped" // stale or duplicate delivery suppressed by start.lua
)

// NewMetrics creates and registers the worker metrics on reg. Workers of
// several queues may share one Metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	// 1ms .. ~131s
	buckets := prometheus.ExponentialBuckets(0.001, 2, 18)
	m := &Metrics{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_jobs_started_total", Help: "Job starts (attempts), including reclaimed deliveries.",
		}, []string{"queue", "priority"}),
		reclaimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_jobs_reclaimed_total", Help: "Deliveries reclaimed with XAUTOCLAIM after their lease expired.",
		}, []string{"queue"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_jobs_finished_total", Help: "Delivery outcomes.",
		}, []string{"queue", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskq_job_duration_seconds", Help: "Handler run time per attempt.", Buckets: buckets,
		}, []string{"queue", "outcome"}),
		wait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskq_queue_wait_seconds", Help: "Time from runnable to started (Redis clock).", Buckets: buckets,
		}, []string{"queue", "priority"}),
		e2e: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskq_job_end_to_end_seconds", Help: "Time from enqueue to success (Redis clock).", Buckets: buckets,
		}, []string{"queue"}),
		leaseLost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_lease_lost_total", Help: "Handlers cancelled because their lease was lost or could not be renewed.",
		}, []string{"queue", "reason"}),
		rateWaits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_rate_limited_total", Help: "Times a worker waited for the queue rate limiter.",
		}, []string{"queue"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskq_jobs_in_flight", Help: "Handlers currently running in this process.",
		}, []string{"queue"}),
		script: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskq_redis_script_duration_seconds", Help: "Round-trip latency of taskq Lua scripts.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms .. ~3.3s
		}, []string{"script"}),
	}
	reg.MustRegister(m.started, m.reclaimed, m.outcomes, m.duration, m.wait, m.e2e,
		m.leaseLost, m.rateWaits, m.inFlight, m.script)
	return m
}

func (m *Metrics) jobStarted(q, prio string, wait time.Duration, reclaimed bool) {
	if m == nil {
		return
	}
	m.started.WithLabelValues(q, prio).Inc()
	m.wait.WithLabelValues(q, prio).Observe(max(wait, 0).Seconds())
	if reclaimed {
		m.reclaimed.WithLabelValues(q).Inc()
	}
	m.inFlight.WithLabelValues(q).Inc()
}

func (m *Metrics) jobDone(q string) {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(q).Dec()
}

func (m *Metrics) outcome(q, outcome string, handlerTime time.Duration) {
	if m == nil {
		return
	}
	m.outcomes.WithLabelValues(q, outcome).Inc()
	if handlerTime > 0 {
		m.duration.WithLabelValues(q, outcome).Observe(handlerTime.Seconds())
	}
}

func (m *Metrics) endToEnd(q string, d time.Duration) {
	if m == nil {
		return
	}
	m.e2e.WithLabelValues(q).Observe(max(d, 0).Seconds())
}

func (m *Metrics) lostLease(q, reason string) {
	if m == nil {
		return
	}
	m.leaseLost.WithLabelValues(q, reason).Inc()
}

func (m *Metrics) rateWait(q string) {
	if m == nil {
		return
	}
	m.rateWaits.WithLabelValues(q).Inc()
}

// scriptTimer returns a func that records the elapsed time for script.
func (m *Metrics) scriptTimer(script string) func() {
	if m == nil {
		return func() {}
	}
	t0 := time.Now()
	return func() { m.script.WithLabelValues(script).Observe(time.Since(t0).Seconds()) }
}
