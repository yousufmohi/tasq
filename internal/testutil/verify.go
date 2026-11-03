package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq/internal/demo"
	"github.com/yousufmo27/taskq/internal/job"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/state"
)

// Report is the outcome of a failure test. Every failure test ends by
// building one, asserting Lost == 0 (unless the scenario documents
// otherwise), and publishing it with Publish.
type Report struct {
	Test string `json:"test"`
	// Acked is the number of jobs whose Enqueue returned success.
	Acked     int `json:"acked"`
	Succeeded int `json:"succeeded"`
	Dead      int `json:"dead"`
	// Lost: acked jobs that are missing or not in a terminal state.
	Lost int `json:"lost"`
	// Executions counts handler invocations; DuplicateExecutions is
	// Executions minus the number of distinct jobs that executed.
	Executions          int `json:"executions"`
	DuplicateExecutions int `json:"duplicate_executions"`
	// Effects counts completed naive (non-idempotent) side effects.
	Effects          int `json:"effects"`
	DuplicateEffects int `json:"duplicate_effects"`
	// Applied counts distinct idempotency keys an idempotent downstream
	// accepted; it can never exceed the number of distinct jobs.
	Applied int `json:"applied"`
	// Reclaimed counts jobs that needed more than one start.
	Reclaimed int `json:"reclaimed"`
	// SucceededWithoutExecution must be 0; anything else is a bug in the
	// test or the queue.
	SucceededWithoutExecution int `json:"succeeded_without_execution"`

	Extra map[string]any `json:"extra,omitempty"`

	LostIDs []string `json:"-"`
}

// Verify computes a Report for the acked job IDs of queue q.
func Verify(ctx context.Context, rdb redis.UniversalClient, q string, acked []string) (Report, error) {
	r := Report{Acked: len(acked)}
	pipe := rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(acked))
	for i, id := range acked {
		cmds[i] = pipe.HGetAll(ctx, keys.Job(q, id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return r, err
	}
	exec, err := rdb.HGetAll(ctx, demo.ExecKey(q)).Result()
	if err != nil {
		return r, err
	}
	effect, err := rdb.HGetAll(ctx, demo.EffectKey(q)).Result()
	if err != nil {
		return r, err
	}
	applied, err := rdb.SCard(ctx, demo.AppliedKey(q)).Result()
	if err != nil {
		return r, err
	}
	r.Applied = int(applied)

	for i, id := range acked {
		m := cmds[i].Val()
		if len(m) == 0 {
			r.Lost++
			r.LostIDs = append(r.LostIDs, id)
			continue
		}
		j, err := job.FromMap(m)
		if err != nil {
			return r, err
		}
		switch j.State {
		case state.Succeeded:
			r.Succeeded++
			if atoi(exec[id]) == 0 {
				r.SucceededWithoutExecution++
			}
		case state.Dead:
			r.Dead++
		default:
			r.Lost++
			r.LostIDs = append(r.LostIDs, id)
		}
		if j.Attempts > 1 {
			r.Reclaimed++
		}
	}
	for _, v := range exec {
		r.Executions += atoi(v)
	}
	r.DuplicateExecutions = r.Executions - len(exec)
	for _, v := range effect {
		r.Effects += atoi(v)
	}
	r.DuplicateEffects = r.Effects - len(effect)
	return r, nil
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// Publish logs the report and, if TASKQ_RESULTS_DIR is set, writes it as
// <dir>/<test>.json.
func Publish(t testing.TB, r Report) {
	t.Helper()
	if r.Test == "" {
		r.Test = t.Name()
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	t.Logf("failure report:\n%s", b)
	dir := os.Getenv("TASKQ_RESULTS_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Errorf("results dir: %v", err)
		return
	}
	type out struct {
		Report
		RecordedAt string `json:"recorded_at"`
	}
	b, _ = json.MarshalIndent(out{r, time.Now().UTC().Format(time.RFC3339)}, "", "  ")
	name := filepath.Join(dir, sanitize(r.Test)+".json")
	if err := os.WriteFile(name, append(b, '\n'), 0o644); err != nil {
		t.Errorf("write report: %v", err)
	}
}

func sanitize(s string) string { return unsafe.ReplaceAllString(s, "_") }

// WaitTerminal waits until every acked job is succeeded or dead.
func WaitTerminal(ctx context.Context, rdb redis.UniversalClient, q string, acked []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	remaining := append([]string(nil), acked...)
	for {
		pipe := rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(remaining))
		for i, id := range remaining {
			cmds[i] = pipe.HGet(ctx, keys.Job(q, id), "state")
		}
		_, _ = pipe.Exec(ctx)
		next := remaining[:0]
		for i, id := range remaining {
			s := cmds[i].Val()
			if s != string(state.Succeeded) && s != string(state.Dead) {
				next = append(next, id)
			}
		}
		remaining = next
		if len(remaining) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d jobs not terminal after %v (e.g. %s)", len(remaining), timeout, remaining[0])
		}
		time.Sleep(100 * time.Millisecond)
	}
}
