#!/usr/bin/env bash
# Reproducible benchmark matrix. Writes bench/results/<UTC timestamp>/:
#   env.json        hardware, versions, git SHA, Redis RTT
#   <run>.json      one summary per run (config + results)
#   raw/<run>.csv.gz  per-job Redis timestamps for every run
#   summary.md      tables generated from the JSON files
#
# Usage: bench/run.sh [reps]        (default 5 repetitions per config)
# Needs Docker and Go. Uses a dedicated Redis container on port 6390 and
# FLUSHALLs it between runs; it never touches any other Redis.
set -euo pipefail
cd "$(dirname "$0")/.."

REPS=${1:-5}
PORT=6390
ADDR=localhost:$PORT
CONTAINER=taskq-bench-redis
IMAGE=redis:7.4.12-alpine
OUT=bench/results/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"

echo ">> building (no -race: the benchmark measures the real binaries)"
go build -trimpath -o bench/bin/ ./cmd/taskq-worker ./cmd/taskq-bench

echo ">> starting dedicated Redis ($IMAGE, repo redis.conf: AOF everysec, noeviction)"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" -p "$PORT:6379" \
  -v "$PWD/deploy/redis/redis.conf:/usr/local/etc/redis/redis.conf:ro" \
  "$IMAGE" redis-server /usr/local/etc/redis/redis.conf >/dev/null
trap 'docker rm -f "$CONTAINER" >/dev/null 2>&1 || true' EXIT
until docker exec "$CONTAINER" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done

bench/hardware.sh "$ADDR" "$CONTAINER" > "$OUT/env.json"
echo ">> environment:"; cat "$OUT/env.json"

run() {
  echo ">> $*"
  bench/bin/taskq-bench -redis "$ADDR" -worker-bin bench/bin/taskq-worker -out "$OUT" "$@" >/dev/null
  docker exec "$CONTAINER" redis-cli FLUSHALL >/dev/null
}

for rep in $(seq 1 "$REPS"); do
  # 1. Consume throughput as worker processes scale (CPU-bound: no-op
  #    handler; I/O-bound: 5ms handler).
  for w in 1 2 4 8; do
    run -mode drain -workers "$w" -concurrency 16 -sleep-ms 0 -jobs 20000 -rep "$rep"
    run -mode drain -workers "$w" -concurrency 16 -sleep-ms 5 -jobs 20000 -rep "$rep"
  done
  # 2. End-to-end latency under open-loop load.
  for rate in 500 2000 5000; do
    run -mode latency -workers 4 -concurrency 16 -sleep-ms 0 -rate "$rate" -duration 20s -rep "$rep"
  done
  # 3. Recovery after SIGKILL of one worker: vs visibility timeout, and as
  #    the number of workers scales.
  for vt in 2s 5s 10s; do
    run -mode recovery -workers 4 -concurrency 32 -sleep-ms 200 -rate 100 -duration 25s \
        -warmup 8s -kill 1 -visibility "$vt" -rep "$rep"
  done
  for w in 2 8; do
    run -mode recovery -workers "$w" -concurrency 32 -sleep-ms 200 -rate 100 -duration 25s \
        -warmup 8s -kill 1 -visibility 5s -rep "$rep"
  done
done

bench/bin/taskq-bench -mode summarize -out "$OUT" > "$OUT/summary.md"
echo ">> done: $OUT"
cat "$OUT/summary.md"
