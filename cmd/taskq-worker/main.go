// Command taskq-worker runs the demo handler against one queue. It is the
// process the failure tests SIGKILL and the benchmarks scale out.
//
// Every flag can also be set by an environment variable (shown in -help).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmo27/taskq/internal/demo"
	"github.com/yousufmo27/taskq/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "taskq-worker:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		redisAddr   = flag.String("redis", env("REDIS_ADDR", "localhost:6379"), "Redis address (REDIS_ADDR)")
		recordAddr  = flag.String("record-redis", env("RECORD_REDIS_ADDR", ""), "if set, record exec/effect counters to this Redis (RECORD_REDIS_ADDR)")
		queue       = flag.String("queue", env("QUEUE", "default"), "queue name (QUEUE)")
		concurrency = flag.Int("concurrency", envInt("CONCURRENCY", 8), "max jobs in flight (CONCURRENCY)")
		visibility  = flag.Duration("visibility", envDur("VISIBILITY_TIMEOUT", 30*time.Second), "visibility timeout (VISIBILITY_TIMEOUT)")
		retryBase   = flag.Duration("retry-base", envDur("RETRY_BASE", time.Second), "retry backoff base (RETRY_BASE)")
		retryMax    = flag.Duration("retry-max", envDur("RETRY_MAX", 10*time.Minute), "retry backoff cap (RETRY_MAX)")
		logLevel    = flag.String("log-level", env("LOG_LEVEL", "info"), "debug|info|warn|error (LOG_LEVEL)")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	var rec *demo.Recorder
	if *recordAddr != "" {
		rr := redis.NewClient(&redis.Options{Addr: *recordAddr})
		defer rr.Close()
		rec = demo.NewRecorder(rr, *queue)
	}

	w, err := worker.New(rdb, worker.Options{
		Queue:             *queue,
		Concurrency:       *concurrency,
		VisibilityTimeout: *visibility,
		RetryBase:         *retryBase,
		RetryMax:          *retryMax,
		Logger:            log,
	}, demo.Handler(rec))
	if err != nil {
		return err
	}
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
