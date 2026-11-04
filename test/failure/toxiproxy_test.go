//go:build failure

package failure

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/demo"
	"github.com/yousufmohi/tasq/internal/testutil"
	"github.com/yousufmohi/tasq/internal/verify"
	"github.com/yousufmohi/tasq/worker"
)

const toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"

// runToxiproxy starts a Toxiproxy container. Docker Desktop occasionally
// never brings up the host port-forward for a new container (the fault
// testutil.StartRedisWith also works around): the container runs normally
// but its API is unreachable from the host. In 30 repetitions of
// TestFailoverDemo this happened once, and the subtest failed after the
// module's default 60 s readiness wait. So the wait is 15 s and an
// unreachable container is replaced, up to three attempts.
func runToxiproxy(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*toxiproxy.Container, error) {
	t.Helper()
	opts = append(opts, testcontainers.WithWaitStrategy(
		wait.ForHTTP("/version").WithPort(toxiproxy.ControlPort).WithStartupTimeout(15*time.Second)))
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		var tp *toxiproxy.Container
		if tp, err = toxiproxy.Run(ctx, toxiproxyImage, opts...); err == nil {
			return tp, nil
		}
		t.Logf("toxiproxy attempt %d not reachable from the host, replacing it: %v", attempt, err)
		if tp != nil {
			_ = testcontainers.TerminateContainer(tp)
		}
	}
	return nil, err
}

// netEnv is a queue Redis reachable directly (Addr) and through a Toxiproxy
// proxy (ProxyAddr) whose faults are controlled via Proxy.
type netEnv struct {
	Redis     *testutil.Redis
	ProxyAddr string
	Proxy     *toxiclient.Proxy
}

func newNetEnv(t *testing.T) *netEnv {
	t.Helper()
	ctx := context.Background()
	nw, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	r, err := testutil.StartRedisWith(ctx, testutil.RedisOptions{Network: nw, Aliases: []string{"redis"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Terminate() })
	tp, err := runToxiproxy(t, ctx,
		tcnetwork.WithNetwork([]string{"toxiproxy"}, nw),
		toxiproxy.WithProxy("redis", "redis:6379"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(tp) })
	host, port, err := tp.ProxiedEndpoint(8666)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := tp.URI(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := toxiclient.NewClient(uri).Proxy("redis")
	if err != nil {
		t.Fatal(err)
	}
	return &netEnv{Redis: r, ProxyAddr: net.JoinHostPort(host, port), Proxy: proxy}
}

func enqueueN(t *testing.T, c *taskq.Client, q string, n int, p demo.Payload) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for range n {
		id, err := c.Enqueue(context.Background(), q, taskq.EnqueueOptions{Type: "demo", Payload: p.Encode(), MaxAttempts: 20})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// finish waits for every acked job to finish, verifies, and asserts no loss.
// The caller publishes the report.
func finish(t *testing.T, env *netEnv, q string, acked []string, extra map[string]any) verify.Report {
	t.Helper()
	ctx := context.Background()
	qrdb := env.Redis.Client(t)
	if err := verify.WaitTerminal(ctx, qrdb, q, acked, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	rep, err := verify.VerifyWith(ctx, qrdb, recorder(t), q, acked)
	if err != nil {
		t.Fatal(err)
	}
	rep.Extra = extra
	rep.ZeroLossAsserted = true
	return rep
}

// publishAndCheck records rep, then fails the test if any acked job was
// lost or did not succeed (publishing first keeps the evidence of a failure).
func publishAndCheck(t *testing.T, rep verify.Report, acked []string) {
	t.Helper()
	verify.Publish(t, rep)
	if rep.Lost != 0 || rep.SucceededWithoutExecution != 0 {
		t.Fatalf("lost=%d succeeded_without_execution=%d", rep.Lost, rep.SucceededWithoutExecution)
	}
	if rep.Succeeded != len(acked) {
		t.Fatalf("succeeded %d of %d", rep.Succeeded, len(acked))
	}
}

// TestToxiproxyLatency: every worker reaches Redis through 50ms ± 10ms of
// added latency in each direction. Throughput drops; correctness must not.
func TestToxiproxyLatency(t *testing.T) {
	env := newNetEnv(t)
	q := testutil.Queue(t)
	for _, dir := range []string{"upstream", "downstream"} {
		if _, err := env.Proxy.AddToxic("lat_"+dir, "latency", dir, 1, toxiclient.Attributes{"latency": 50, "jitter": 10}); err != nil {
			t.Fatal(err)
		}
	}
	acked := enqueueN(t, taskq.NewClient(env.Redis.Client(t)), q, 300, demo.Payload{SleepMS: 10})
	t0 := time.Now()
	rec := recorder(t)
	for range 2 {
		inprocWorker(t, env.ProxyAddr, rec, q, worker.Options{VisibilityTimeout: 5 * time.Second})
	}
	rep := finish(t, env, q, acked, nil)
	took := time.Since(t0)
	rep.Extra = map[string]any{"latency_ms_each_way": "50±10", "workers": 2, "concurrency": 8,
		"seconds_to_drain_300_jobs": took.Seconds()}
	publishAndCheck(t, rep, acked)
}

// TestToxiproxyPartitionOneWorker: worker A reaches Redis through the
// proxy, worker B directly. While A is mid-job, A's link goes silent (the
// timeout toxic stops all data without closing connections) for 4s, longer
// than the 1s visibility timeout. B must reclaim A's jobs; A must stop its
// handlers (self-fencing). Duplicates are expected and reported.
func TestToxiproxyPartitionOneWorker(t *testing.T) {
	env := newNetEnv(t)
	q := testutil.Queue(t)
	const vt = time.Second
	acked := enqueueN(t, taskq.NewClient(env.Redis.Client(t)), q, 60, demo.Payload{SleepMS: 400})
	rec := recorder(t)

	a, _ := inprocWorker(t, env.ProxyAddr, rec, q, worker.Options{VisibilityTimeout: vt, Concurrency: 8})
	testutil.WaitFor(t, 10*time.Second, "worker A busy", func() bool { return a.InFlight() >= 4 })
	inFlightAtCut := a.InFlight()

	if _, err := env.Proxy.AddToxic("partition", "timeout", "downstream", 1, toxiclient.Attributes{"timeout": 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Proxy.AddToxic("partition_up", "timeout", "upstream", 1, toxiclient.Attributes{"timeout": 0}); err != nil {
		t.Fatal(err)
	}
	cut := time.Now()
	inprocWorker(t, env.Redis.Addr, rec, q, worker.Options{VisibilityTimeout: vt, Concurrency: 8})
	time.Sleep(4 * time.Second)
	_ = env.Proxy.RemoveToxic("partition")
	_ = env.Proxy.RemoveToxic("partition_up")

	rep := finish(t, env, q, acked, nil)
	rep.Extra = map[string]any{
		"visibility_timeout": vt.String(), "partition": "4s silent (timeout toxic) on worker A only",
		"a_in_flight_at_cut": inFlightAtCut, "seconds_from_cut_to_all_done": time.Since(cut).Seconds(),
	}
	publishAndCheck(t, rep, acked)
	if rep.Reclaimed == 0 {
		t.Fatal("no job was reclaimed: the partition did not exercise recovery")
	}
	if rep.Applied != len(acked) {
		t.Fatalf("idempotent downstream applied %d of %d", rep.Applied, len(acked))
	}
}

// TestToxiproxyConnectionResets: the proxy is disabled and re-enabled every
// 300ms for 3s, resetting every worker connection mid-command.
func TestToxiproxyConnectionResets(t *testing.T) {
	env := newNetEnv(t)
	q := testutil.Queue(t)
	acked := enqueueN(t, taskq.NewClient(env.Redis.Client(t)), q, 400, demo.Payload{SleepMS: 20})
	rec := recorder(t)
	for range 2 {
		inprocWorker(t, env.ProxyAddr, rec, q, worker.Options{VisibilityTimeout: 2 * time.Second})
	}
	flaps := 0
	for range 10 {
		time.Sleep(150 * time.Millisecond)
		if err := env.Proxy.Disable(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(150 * time.Millisecond)
		if err := env.Proxy.Enable(); err != nil {
			t.Fatal(err)
		}
		flaps++
	}
	rep := finish(t, env, q, acked, map[string]any{"flaps": fmt.Sprintf("%d x (150ms down, 150ms up)", flaps)})
	publishAndCheck(t, rep, acked)
}
