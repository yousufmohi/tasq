// Package testutil starts real Redis containers for integration tests.
// It is imported only from files built with the "integration" tag.
package testutil

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yousufmo27/taskq/internal/job"
)

// RedisImage is pinned so results are reproducible.
const RedisImage = "redis:7.4.12-alpine"

// Redis is a running Redis container.
type Redis struct {
	Addr string
	c    *testcontainers.DockerContainer
}

// StartRedis starts a container. extraArgs are appended to the
// redis-server command line and override the defaults.
func StartRedis(ctx context.Context, extraArgs ...string) (*Redis, error) {
	cmd := append([]string{
		"redis-server",
		"--appendonly", "yes",
		"--appendfsync", "everysec",
		"--maxmemory-policy", "noeviction",
		"--save", "",
	}, extraArgs...)
	c, err := testcontainers.Run(ctx, RedisImage,
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithCmd(cmd...),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	addr, err := c.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		return nil, err
	}
	return &Redis{Addr: addr, c: c}, nil
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
