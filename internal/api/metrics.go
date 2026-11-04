package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/keys"
)

type metrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	enqueued *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_api_requests_total", Help: "HTTP requests by route pattern and status code.",
		}, []string{"route", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskq_api_request_duration_seconds", Help: "HTTP request latency by route pattern.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms .. ~4s
		}, []string{"route"}),
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_enqueued_total", Help: "Enqueue results through the API.",
		}, []string{"queue", "result"}), // created | duplicate | full | rejected | error
	}
	reg.MustRegister(m.requests, m.latency, m.enqueued)
	return m
}

func (m *metrics) enqueue(queue, result string) {
	if m != nil {
		// The queue comes from the URL; never let an invalid name become a
		// label value.
		if keys.ValidateQueue(queue) != nil {
			queue = "_invalid"
		}
		m.enqueued.WithLabelValues(queue, result).Inc()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

// instrument records request count and latency labelled by the matched
// route pattern (never the raw path, which would explode cardinality).
func (m *metrics) instrument(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern // set by ServeMux on match
		if route == "" {
			route = "unmatched"
		}
		m.requests.WithLabelValues(route, strconv.Itoa(rec.code)).Inc()
		m.latency.WithLabelValues(route).Observe(time.Since(t0).Seconds())
	})
}

// queueCollector reports queue depth gauges at scrape time from Redis, so
// they are correct even with zero workers running and are emitted once
// (by the API) rather than once per worker.
type queueCollector struct {
	c   *taskq.Client
	log *slog.Logger

	waiting, inFlight, scheduled, dead, up *prometheus.Desc
}

func newQueueCollector(c *taskq.Client, log *slog.Logger) *queueCollector {
	return &queueCollector{
		c: c, log: log,
		waiting:   prometheus.NewDesc("taskq_queue_waiting", "Jobs waiting for delivery.", []string{"queue", "priority"}, nil),
		inFlight:  prometheus.NewDesc("taskq_queue_in_flight", "Delivered, unacknowledged jobs (PEL).", []string{"queue", "priority"}, nil),
		scheduled: prometheus.NewDesc("taskq_queue_scheduled", "Delayed and retry-waiting jobs.", []string{"queue"}, nil),
		dead:      prometheus.NewDesc("taskq_queue_dead", "Dead-lettered jobs.", []string{"queue"}, nil),
		up:        prometheus.NewDesc("taskq_queue_stats_up", "1 if queue stats were read from Redis on this scrape.", nil, nil),
	}
}

func (q *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{q.waiting, q.inFlight, q.scheduled, q.dead, q.up} {
		ch <- d
	}
}

func (q *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	up := 1.0
	defer func() { ch <- prometheus.MustNewConstMetric(q.up, prometheus.GaugeValue, up) }()
	names, err := q.c.ListQueues(ctx)
	if err != nil {
		q.log.Warn("metrics: list queues", "err", err)
		up = 0
		return
	}
	for _, name := range names {
		st, err := q.c.Stats(ctx, name)
		if err != nil {
			q.log.Warn("metrics: queue stats", "queue", name, "err", err)
			up = 0
			continue
		}
		for _, p := range keys.Priorities {
			ps := st.Priorities[p]
			ch <- prometheus.MustNewConstMetric(q.waiting, prometheus.GaugeValue, float64(ps.Waiting), name, p)
			ch <- prometheus.MustNewConstMetric(q.inFlight, prometheus.GaugeValue, float64(ps.InFlight), name, p)
		}
		ch <- prometheus.MustNewConstMetric(q.scheduled, prometheus.GaugeValue, float64(st.Scheduled), name)
		ch <- prometheus.MustNewConstMetric(q.dead, prometheus.GaugeValue, float64(st.Dead), name)
	}
}
