// Package api is taskq's HTTP API: enqueue, inspect, DLQ operations and
// queue configuration. It is a thin JSON layer over the taskq client.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/yousufmohi/tasq"
)

// MaxBodyBytes bounds request bodies (and so job payloads via the API).
const MaxBodyBytes = 1 << 20

// Options configures the API.
type Options struct {
	// Token, if set, is required as "Authorization: Bearer <token>" on
	// every /v1 route. Health and metrics routes are unauthenticated.
	Token  string
	Logger *slog.Logger
	// Registry, if set, receives the API and queue-depth metrics and is
	// served at /metrics. Register worker or runtime collectors on it too.
	Registry *prometheus.Registry
}

// Server holds the API's dependencies.
type Server struct {
	c    *taskq.Client
	rdb  redis.UniversalClient
	opts Options
	log  *slog.Logger
	m    *metrics
}

// New returns the API server.
func New(rdb redis.UniversalClient, opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s := &Server{c: taskq.NewClient(rdb), rdb: rdb, opts: opts, log: opts.Logger}
	if opts.Registry != nil {
		s.m = newMetrics(opts.Registry)
		opts.Registry.MustRegister(newQueueCollector(s.c, s.log))
	}
	return s
}

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
	v1 := http.NewServeMux()
	v1.HandleFunc("GET /v1/queues", s.listQueues)
	v1.HandleFunc("POST /v1/queues/{queue}/jobs", s.enqueue)
	v1.HandleFunc("GET /v1/queues/{queue}/jobs/{id}", s.getJob)
	v1.HandleFunc("GET /v1/queues/{queue}/stats", s.stats)
	v1.HandleFunc("GET /v1/queues/{queue}/config", s.getConfig)
	v1.HandleFunc("PUT /v1/queues/{queue}/config", s.putConfig)
	v1.HandleFunc("GET /v1/queues/{queue}/dead", s.listDead)
	v1.HandleFunc("POST /v1/queues/{queue}/dead/replay", s.replayBatch)
	v1.HandleFunc("POST /v1/queues/{queue}/dead/{id}/replay", s.replayOne)
	v1.HandleFunc("DELETE /v1/queues/{queue}/dead", s.purgeDead)

	mux := http.NewServeMux()
	mux.Handle("/v1/", s.auth(v1))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.ready)
	if s.opts.Registry != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.opts.Registry, promhttp.HandlerOpts{}))
	}
	return s.m.instrument(s.accessLog(mux))
}

func (s *Server) auth(next http.Handler) http.Handler {
	if s.opts.Token == "" {
		return next
	}
	want := []byte("Bearer " + s.opts.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			writeErr(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "redis: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// EnqueueRequest is the body of POST /v1/queues/{queue}/jobs.
type EnqueueRequest struct {
	Type             string          `json:"type"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	Priority         string          `json:"priority,omitempty"`
	MaxAttempts      int             `json:"max_attempts,omitempty"`
	TimeoutMS        int64           `json:"timeout_ms,omitempty"`
	DelayMS          int64           `json:"delay_ms,omitempty"`
	RunAt            *time.Time      `json:"run_at,omitempty"`
	IdempotencyKey   string          `json:"idempotency_key,omitempty"`
	IdempotencyTTLMS int64           `json:"idempotency_ttl_ms,omitempty"`
}

// EnqueueResponse is returned with 202 (created) or 200 (duplicate).
type EnqueueResponse struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if !decode(w, r, &req) {
		return
	}
	o := taskq.EnqueueOptions{
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       taskq.Priority(req.Priority),
		MaxAttempts:    req.MaxAttempts,
		Timeout:        time.Duration(req.TimeoutMS) * time.Millisecond,
		Delay:          time.Duration(req.DelayMS) * time.Millisecond,
		IdempotencyKey: req.IdempotencyKey,
		IdempotencyTTL: time.Duration(req.IdempotencyTTLMS) * time.Millisecond,
	}
	if req.RunAt != nil {
		o.RunAt = *req.RunAt
	}
	id, err := s.c.Enqueue(r.Context(), r.PathValue("queue"), o)
	switch {
	case err == nil:
		s.m.enqueue(r.PathValue("queue"), "created")
		writeJSON(w, http.StatusAccepted, EnqueueResponse{ID: id})
	case errors.Is(err, taskq.ErrDuplicate):
		s.m.enqueue(r.PathValue("queue"), "duplicate")
		writeJSON(w, http.StatusOK, EnqueueResponse{ID: id, Duplicate: true})
	case errors.Is(err, taskq.ErrQueueFull):
		s.m.enqueue(r.PathValue("queue"), "full")
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusTooManyRequests, err.Error())
	default:
		if errors.Is(err, taskq.ErrInvalidArgument) {
			s.m.enqueue(r.PathValue("queue"), "rejected")
		} else {
			s.m.enqueue(r.PathValue("queue"), "error")
		}
		s.clientErr(w, err)
	}
}

// JobView is the API representation of a job.
type JobView struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Priority       string          `json:"priority"`
	State          string          `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	CreatedAt      *time.Time      `json:"created_at,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	// PayloadBytes is set instead of Payload when the payload is not JSON
	// (e.g. enqueued from Go with binary data); it is base64 in JSON.
	PayloadBytes []byte `json:"payload_bytes,omitempty"`
}

func view(j *taskq.Job) JobView {
	v := JobView{
		ID: j.ID, Queue: j.Queue, Type: j.Type, Priority: j.Priority,
		State: string(j.State), Attempts: j.Attempts, MaxAttempts: j.MaxAttempts,
		CreatedAt: tp(j.CreatedAt), RunAt: tp(j.RunAt), StartedAt: tp(j.StartedAt),
		FinishedAt: tp(j.FinishedAt), LastError: j.LastError,
		IdempotencyKey: j.IdempotencyKey(),
	}
	if len(j.Payload) > 0 {
		if json.Valid(j.Payload) {
			v.Payload = j.Payload
		} else {
			v.PayloadBytes = j.Payload
		}
	}
	return v
}

func tp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.c.GetJob(r.Context(), r.PathValue("queue"), r.PathValue("id"))
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view(j))
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	qs, err := s.c.ListQueues(r.Context())
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"queues": qs})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.c.Stats(r.Context(), r.PathValue("queue"))
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ConfigBody is the body of GET/PUT /v1/queues/{queue}/config.
type ConfigBody struct {
	MaxDepth int64   `json:"max_depth"`
	Rate     float64 `json:"rate"`
	Burst    int64   `json:"burst"`
}

func (s *Server) getConfig(w http.ResponseWriter, r *http.Request) {
	cfg, err := s.c.GetQueueConfig(r.Context(), r.PathValue("queue"))
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ConfigBody(cfg))
}

func (s *Server) putConfig(w http.ResponseWriter, r *http.Request) {
	var b ConfigBody
	if !decode(w, r, &b) {
		return
	}
	q := r.PathValue("queue")
	if err := s.c.SetQueueConfig(r.Context(), q, taskq.QueueConfig(b)); err != nil {
		s.clientErr(w, err)
		return
	}
	cfg, err := s.c.GetQueueConfig(r.Context(), q)
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ConfigBody(cfg))
}

func (s *Server) listDead(w http.ResponseWriter, r *http.Request) {
	offset, err1 := intParam(r, "offset", 0, 0, 1<<40)
	limit, err2 := intParam(r, "limit", 50, 1, 500)
	if err := errors.Join(err1, err2); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	jobs, err := s.c.ListDead(r.Context(), r.PathValue("queue"), offset, limit)
	if err != nil {
		s.clientErr(w, err)
		return
	}
	out := make([]JobView, len(jobs))
	for i, j := range jobs {
		out[i] = view(j)
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out, "offset": offset, "limit": limit})
}

func (s *Server) replayOne(w http.ResponseWriter, r *http.Request) {
	if err := s.c.ReplayDead(r.Context(), r.PathValue("queue"), r.PathValue("id")); err != nil {
		s.clientErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) replayBatch(w http.ResponseWriter, r *http.Request) {
	limit, err := intParam(r, "limit", 100, 1, 10000)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := s.c.ReplayDeadBatch(r.Context(), r.PathValue("queue"), limit)
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func (s *Server) purgeDead(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("older_than")
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		writeErr(w, http.StatusBadRequest, "older_than must be a non-negative duration such as 24h")
		return
	}
	n, err := s.c.PurgeDead(r.Context(), r.PathValue("queue"), time.Now().Add(-d))
	if err != nil {
		s.clientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"purged": n})
}

// clientErr maps client errors to status codes. Anything unexpected is a
// 500 and is logged; its detail is not sent to the caller.
func (s *Server) clientErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, taskq.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, taskq.ErrInvalidState):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, taskq.ErrInvalidArgument):
		writeErr(w, http.StatusBadRequest, err.Error())
	default:
		s.log.Error("api: internal error", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

func intParam(r *http.Request, name string, def, lo, hi int64) (int64, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s must be an integer in [%d, %d]", name, lo, hi)
	}
	return n, nil
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB")
		} else {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return false
	}
	if dec.More() {
		writeErr(w, http.StatusBadRequest, "invalid JSON: trailing data after the object")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// accessLog writes one structured line per request (method, route pattern,
// status, duration). Probe and scrape endpoints are skipped as noise.
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/readyz", "/metrics":
			next.ServeHTTP(w, r)
			return
		}
		t0 := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http request", "method", r.Method, "route", r.Pattern, "path", r.URL.Path,
			"status", rec.code, "duration_ms", float64(time.Since(t0).Microseconds())/1000)
	})
}
