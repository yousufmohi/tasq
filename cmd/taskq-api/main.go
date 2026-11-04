// Command taskq-api serves the taskq HTTP API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/yousufmohi/tasq/internal/api"
	"github.com/yousufmohi/tasq/internal/otelsetup"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "taskq-api:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr      = flag.String("addr", env("ADDR", ":8080"), "listen address (ADDR)")
		redisAddr = flag.String("redis", env("REDIS_ADDR", "localhost:6379"), "Redis address (REDIS_ADDR)")
		logLevel  = flag.String("log-level", env("LOG_LEVEL", "info"), "debug|info|warn|error (LOG_LEVEL)")
	)
	flag.Parse()
	token := os.Getenv("TASKQ_API_TOKEN") // env only: keep secrets out of argv

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := otelsetup.Setup(ctx, "taskq-api")
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(rdb, api.Options{Token: token, Logger: log, Registry: reg}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("taskq-api listening", "addr", *addr, "auth", token != "")
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
