//go:build failure

package failure

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/demo"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/testutil"
)

// TestSIGKILLWorkersMidJob runs 4 worker processes, SIGKILLs one at random
// every 700ms for 10 rounds while 400 jobs (50-250ms each) are processed,
// and restarts each killed process. One extra job is a poison pill that
// exits its worker on every attempt.
//
// Asserts: no acked job lost; every succeeded job executed at least once;
// the poison pill is dead after exactly MaxAttempts starts; queue drained.
// Reports duplicate executions and duplicate naive side effects.
func TestSIGKILLWorkersMidJob(t *testing.T) {
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)
	ctx := context.Background()

	const jobs = 400
	var acked []string
	for range jobs {
		p := demo.Payload{SleepMS: 50 + rand.IntN(200)}
		id, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{Type: "demo", Payload: p.Encode()})
		if err != nil {
			t.Fatal(err)
		}
		acked = append(acked, id)
	}
	poison, err := c.Enqueue(ctx, q, taskq.EnqueueOptions{
		Type: "demo", Payload: demo.Payload{Crash: true}.Encode(), MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	acked = append(acked, poison)

	addr := testutil.SharedAddr(t)
	f := newFleet(t, 4,
		"-redis", addr, "-record-redis", addr, "-queue", q,
		"-concurrency", "8", "-visibility", "1500ms",
		"-retry-base", "10ms", "-retry-max", "100ms", "-log-level", "warn")

	time.Sleep(300 * time.Millisecond) // let work start before the first kill
	for range 10 {
		f.killRandom()
		time.Sleep(700 * time.Millisecond)
	}

	if err := testutil.WaitTerminal(ctx, rdb, q, acked, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	f.stop()

	r, err := testutil.Verify(ctx, rdb, q, acked)
	if err != nil {
		t.Fatal(err)
	}
	r.Extra = map[string]any{
		"worker_processes": 4, "concurrency": 8, "visibility_timeout": "1500ms",
		"sigkills": f.kills, "processes_started": f.started,
	}
	testutil.Publish(t, r)

	if r.Lost != 0 {
		t.Fatalf("LOST %d acknowledged jobs: %v", r.Lost, r.LostIDs)
	}
	if r.SucceededWithoutExecution != 0 {
		t.Fatalf("%d jobs succeeded without executing", r.SucceededWithoutExecution)
	}
	if r.Succeeded != jobs || r.Dead != 1 {
		t.Fatalf("succeeded=%d dead=%d, want %d/1", r.Succeeded, r.Dead, jobs)
	}
	pj, _ := c.GetJob(ctx, q, poison)
	if pj.State != taskq.StateDead || pj.Attempts != 3 {
		t.Fatalf("poison pill: %+v", pj)
	}
	if r.Reclaimed == 0 || f.kills < 10 {
		t.Fatalf("test did not exercise recovery: reclaimed=%d kills=%d", r.Reclaimed, f.kills)
	}
	if r.Applied != jobs {
		t.Fatalf("idempotent downstream applied %d, want %d", r.Applied, jobs)
	}
	for _, s := range keys.Streams(q) {
		if n := rdb.XLen(ctx, s).Val(); n != 0 {
			t.Fatalf("%s still has %d entries", s, n)
		}
	}
}
