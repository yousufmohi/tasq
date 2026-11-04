// Package testutil starts real Redis containers for integration tests.
// It is imported only from files built with the "integration" tag.
package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yousufmohi/tasq/internal/job"
)

// RedisImage is pinned so results are reproducible.
const RedisImage = "redis:7.4.12-alpine"

// Redis is a running Redis container.
type Redis struct {
	Addr string
	c    *testcontainers.DockerContainer
}

// RedisOptions customises StartRedisWith.
type RedisOptions struct {
	// Args are appended to the redis-server command line and override the
	// defaults (AOF everysec, noeviction, no RDB snapshots).
	Args []string
	// FixedPort binds a fixed host port so the address survives container
	// restarts (Docker otherwise reassigns ephemeral ports on restart).
	FixedPort bool
	// Network and Aliases attach the container to a user-defined network
	// under these DNS names (for Toxiproxy or replication).
	Network *testcontainers.DockerNetwork
	Aliases []string
}

// StartRedis starts a container. extraArgs are appended to the
// redis-server command line and override the defaults.
func StartRedis(ctx context.Context, extraArgs ...string) (*Redis, error) {
	return StartRedisWith(ctx, RedisOptions{Args: extraArgs})
}

// StartRedisWith starts a container with options.
//
// Docker Desktop occasionally never brings up the host port-forward for a
// freshly published port when many containers are created in quick
// succession (seen as a connection refused for the container's whole life
// while other containers work). That is an environment fault, not a Redis
// one, so a container unreachable from the host within 10s is replaced, up
// to three attempts.
func StartRedisWith(ctx context.Context, o RedisOptions) (*Redis, error) {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		var r *Redis
		if r, err = startRedisOnce(ctx, o); err == nil {
			return r, nil
		}
	}
	return nil, err
}

func startRedisOnce(ctx context.Context, o RedisOptions) (*Redis, error) {
	cmd := append([]string{
		"redis-server",
		"--appendonly", "yes",
		"--appendfsync", "everysec",
		"--maxmemory-policy", "noeviction",
		"--save", "",
	}, o.Args...)
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithCmd(cmd...),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second)),
	}
	if o.FixedPort {
		port, err := freePort()
		if err != nil {
			return nil, err
		}
		opts = append(opts, testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("6379/tcp"): {{HostPort: strconv.Itoa(port)}},
			}
		}))
	}
	if o.Network != nil {
		opts = append(opts, tcnetwork.WithNetwork(o.Aliases, o.Network))
	}
	c, err := testcontainers.Run(ctx, RedisImage, opts...)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	addr, err := c.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		return nil, err
	}
	// The wait strategy only proves Redis is ready *inside* the container.
	// With Docker Desktop the host port-forward can come up a moment later,
	// so the first connection was occasionally refused (3 of 10 runs).
	// Wait until Redis answers from the host.
	if err := waitPing(ctx, addr); err != nil {
		_ = testcontainers.TerminateContainer(c)
		return nil, err
	}
	return &Redis{Addr: addr, c: c}, nil
}

// waitPing waits until Redis at addr answers PING from this host (it also
// rides through LOADING while an AOF is replayed after a restart).
func waitPing(ctx context.Context, addr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1,
		DialTimeout: time.Second, ReadTimeout: time.Second})
	defer rdb.Close()
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if err = rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("redis at %s not reachable from the host: %w", addr, err)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Kill sends SIGKILL to redis-server (no shutdown save, no AOF flush beyond
// what the kernel already has) and leaves the container stopped.
func (r *Redis) Kill(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "docker", "kill", "--signal", "KILL", r.c.GetContainerID()).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker kill: %v: %s", err, out)
	}
	// Wait until Docker reports it stopped.
	for range 100 {
		st, err := r.c.State(ctx)
		if err == nil && !st.Running {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("container still running after SIGKILL")
}

// Stop shuts redis-server down gracefully (SIGTERM: Redis flushes and
// fsyncs the AOF before exiting).
func (r *Redis) Stop(ctx context.Context) error {
	timeout := 10 * time.Second
	return r.c.Stop(ctx, &timeout)
}

// Start starts a stopped container (same filesystem, same fixed port) and
// waits until Redis answers PING with its data loaded.
func (r *Redis) Start(ctx context.Context) error {
	if err := r.c.Start(ctx); err != nil {
		return err
	}
	return waitPing(ctx, r.Addr)
}

// Exec runs a command inside the container.
func (r *Redis) Exec(ctx context.Context, cmd ...string) error {
	code, _, err := r.c.Exec(ctx, cmd)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("%v exited %d", cmd, code)
	}
	return nil
}

// Terminate stops and removes the container.
func (r *Redis) Terminate() error { return testcontainers.TerminateContainer(r.c) }

// Client returns a client for this container, closed at test cleanup.
func (r *Redis) Client(t testing.TB) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: r.Addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

var (
	once     sync.Once
	shared   *Redis
	startErr error
)

// Shared returns a client for a Redis container shared by the whole test
// binary. Tests isolate themselves with Queue names rather than flushing, so
// they may run in parallel. Call Main from TestMain to remove the container.
func Shared(t testing.TB) *redis.Client {
	t.Helper()
	once.Do(func() { shared, startErr = StartRedis(context.Background()) })
	if startErr != nil {
		t.Fatalf("integration tests need Docker: %v", startErr)
	}
	return shared.Client(t)
}

// SharedAddr returns the host:port of the shared container, starting it if
// needed (for handing to worker subprocesses).
func SharedAddr(t testing.TB) string {
	t.Helper()
	Shared(t)
	return shared.Addr
}

// Main runs the tests and then terminates the shared container.
func Main(m *testing.M) {
	code := m.Run()
	if shared != nil {
		if err := shared.Terminate(); err != nil {
			fmt.Fprintln(os.Stderr, "terminate redis:", err)
		}
	}
	os.Exit(code)
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// Queue returns a queue name unique to this test.
func Queue(t testing.TB) string {
	name := unsafe.ReplaceAllString(t.Name(), "_")
	if len(name) > 40 {
		name = name[:40]
	}
	return name + "-" + job.NewID()[:8]
}

// WaitFor polls cond until it returns true or timeout elapses.
func WaitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
