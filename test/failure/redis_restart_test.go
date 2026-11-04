//go:build failure

package failure

import (
	"context"
	"testing"
	"time"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/demo"
	"github.com/yousufmohi/tasq/internal/testutil"
	"github.com/yousufmohi/tasq/internal/verify"
)

type restartCase struct {
	name string
	args []string // redis-server persistence settings
	// disrupt takes Redis down and brings it back.
	disrupt func(context.Context, *testutil.Redis) error
	// lazy runs Redis on a LazyFS mount (test/failure/lazyfs), which hostCrash
	// needs to drop unsynced writes.
	lazy bool
	// expect: "none" asserts zero loss; "some" asserts loss > 0 (the
	// positive control proving the harness can detect loss); "report"
	// only records the measurement.
	expect string
	why    string
}

func killRestart(ctx context.Context, r *testutil.Redis) error {
	if err := r.Kill(ctx); err != nil {
		return err
	}
	return r.Start(ctx)
}

// hostCrash simulates a power loss: Redis is SIGKILLed and every write
// that was never fsynced is dropped before it restarts.
func hostCrash(ctx context.Context, r *testutil.Redis) error { return r.HostCrash(ctx) }

func gracefulRestart(ctx context.Context, r *testutil.Redis) error {
	if err := r.Stop(ctx); err != nil {
		return err
	}
	return r.Start(ctx)
}

// TestRedisRestart restarts the queue's Redis under continuous load (8
// producer goroutines, 2 worker processes) for each persistence setting.
// Workers and producers ride through the outage; afterwards every acked
// enqueue is checked against the queue Redis and an independent recorder.
func TestRedisRestart(t *testing.T) {
	cases := []restartCase{
		{
			name: "graceful_aof_everysec", args: []string{"--appendfsync", "everysec"},
			disrupt: gracefulRestart, expect: "none",
			why: "SIGTERM makes Redis fsync the AOF before exiting",
		},
		{
			name: "sigkill_aof_always", args: []string{"--appendfsync", "always"},
			disrupt: killRestart, expect: "none",
			why: "every write is fsynced before Redis replies",
		},
		{
			name: "sigkill_aof_everysec", args: []string{"--appendfsync", "everysec"},
			disrupt: killRestart, expect: "report",
			why: "a process kill keeps the kernel page cache, so written-but-unfsynced AOF " +
				"data survives; the everysec loss window applies to host/kernel crashes (hostcrash_aof_everysec)",
		},
		{
			name: "hostcrash_aof_always", args: []string{"--appendfsync", "always"}, lazy: true,
			disrupt: hostCrash, expect: "none",
			why: "every write is fsynced before Redis replies, so dropping unsynced data loses nothing acked",
		},
		{
			name: "hostcrash_aof_everysec", args: []string{"--appendfsync", "everysec"}, lazy: true,
			disrupt: hostCrash, expect: "report",
			why: "Redis fsyncs the AOF about once per second; writes acked since the last fsync are lost",
		},
		{
			name: "hostcrash_aof_no_control", args: []string{"--appendfsync", "no"}, lazy: true,
			disrupt: hostCrash, expect: "some",
			why: "positive control: Redis never fsyncs, so the simulated crash must lose acked jobs; " +
				"this proves the crash simulation really drops unsynced data",
		},
		{
			name: "sigkill_no_persistence_control", args: []string{"--appendonly", "no"},
			disrupt: killRestart, expect: "some",
			why: "positive control: with no persistence, everything not yet executed is lost",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runRestart(t, tc) })
	}
}

func runRestart(t *testing.T, tc restartCase) {
	ctx := context.Background()
	o := testutil.RedisOptions{Args: tc.args, FixedPort: true}
	if tc.lazy {
		// The container is never stopped (crash.sh restarts redis-server
		// inside it), so its ephemeral port stays valid.
		o = testutil.RedisOptions{Args: tc.args, LazyFSDir: "lazyfs"}
	}
	r, err := testutil.StartRedisWith(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Terminate() })
	qrdb := r.Client(t)
	rec := recorder(t)
	q := testutil.Queue(t)
	c := taskq.NewClient(qrdb)

	f := newFleet(t, 2,
		"-redis", r.Addr, "-record-redis", testutil.SharedAddr(t), "-queue", q,
		"-concurrency", "8", "-visibility", "2s",
		"-retry-base", "50ms", "-retry-max", "500ms", "-log-level", "error")
	p := startProducer(t, c, q, 8, 2*time.Millisecond,
		taskq.EnqueueOptions{Type: "demo", Payload: demo.Payload{SleepMS: 20}.Encode(), MaxAttempts: 25})

	time.Sleep(1500 * time.Millisecond)
	ackedBefore := len(p.Acked())
	t0 := time.Now()
	if err := tc.disrupt(ctx, r); err != nil {
		t.Fatal(err)
	}
	outage := time.Since(t0)
	time.Sleep(1500 * time.Millisecond)
	p.Stop()
	acked := p.Acked()

	if err := verify.WaitSettled(ctx, qrdb, q, acked, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	f.stop()

	rep, err := verify.VerifyWith(ctx, qrdb, rec, q, acked)
	if err != nil {
		t.Fatal(err)
	}
	rep.Test = "TestRedisRestart/" + tc.name
	rep.ZeroLossAsserted = tc.expect == "none"
	rep.Extra = map[string]any{
		"redis_args": tc.args, "expectation": tc.expect, "why": tc.why,
		"acked_before_disruption": ackedBefore, "enqueue_errors": p.failed,
		"outage_ms": outage.Milliseconds(),
	}
	if tc.lazy {
		rep.Extra["simulation"] = "LazyFS clear-cache: unsynced writes dropped (test/failure/lazyfs)"
	}
	if len(rep.LostIDs) > 0 {
		// How far back from the disruption the loss reaches: the oldest
		// lost job's ack time, relative to the start of the disruption.
		oldest := t0
		for _, id := range rep.LostIDs {
			if at, ok := p.AckedAt(id); ok && at.Before(oldest) {
				oldest = at
			}
		}
		rep.Extra["lost_window_ms"] = t0.Sub(oldest).Milliseconds()
	}
	verify.Publish(t, rep)

	if rep.SucceededWithoutExecution != 0 {
		t.Fatalf("%d jobs succeeded without executing", rep.SucceededWithoutExecution)
	}
	switch tc.expect {
	case "none":
		if rep.Lost != 0 || rep.ForgottenAfterSuccess != 0 {
			t.Fatalf("LOST %d acked jobs (%d forgotten after success): %v",
				rep.Lost, rep.ForgottenAfterSuccess, rep.LostIDs[:min(5, len(rep.LostIDs))])
		}
	case "some":
		if rep.Lost == 0 {
			t.Fatal("positive control lost nothing: the harness cannot detect loss")
		}
	}
}
