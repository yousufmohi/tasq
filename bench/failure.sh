#!/usr/bin/env bash
# Run the failure-injection suite N times and keep every artefact:
#   results/failure/<UTC timestamp>/
#     env.json        hardware, versions, git SHA (bench/hardware.sh)
#     runs.tsv        run, go test exit code, seconds, failed tests
#     run<N>/*.json   one report per scenario (internal/verify.Report)
#     run<N>.log.gz   the full `go test -v` output of that run
#     summary.md      every scenario across runs (taskq-bench -mode summarize-failure)
#
# Usage: bench/failure.sh [reps]     (default 5)
# A failing run is recorded, not retried: the loop always continues.
set -uo pipefail
cd "$(dirname "$0")/.."

REPS=${1:-5}
OUT=results/failure/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"

go build -trimpath -o bench/bin/ ./cmd/taskq-bench ./cmd/taskq-worker || exit 1

# hardware.sh measures the round trip to a Redis; use a throwaway one.
CONTAINER=taskq-failure-env-redis
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" -p 6391:6379 redis:7.4.12-alpine >/dev/null || exit 1
until docker exec "$CONTAINER" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done
bench/hardware.sh localhost:6391 "$CONTAINER" > "$OUT/env.json"
docker rm -f "$CONTAINER" >/dev/null

printf 'run\texit\tseconds\tfailed\n' > "$OUT/runs.tsv"
for i in $(seq 1 "$REPS"); do
  echo ">> failure suite run $i/$REPS"
  t0=$(date +%s)
  TASKQ_RESULTS_DIR="$OUT/run$i" go test -race -count=1 -tags failure -v -timeout 45m \
    ./test/failure/ > "$OUT/run$i.log" 2>&1
  code=$?
  failed=$(grep -E '^\s*--- FAIL' "$OUT/run$i.log" | awk '{print $3}' | paste -sd, -)
  printf '%d\t%d\t%d\t%s\n' "$i" "$code" "$(( $(date +%s) - t0 ))" "${failed:--}" >> "$OUT/runs.tsv"
  gzip -9 "$OUT/run$i.log"
  echo "   exit $code ${failed:+(failed: $failed)}"
done

bench/bin/taskq-bench -mode summarize-failure -out "$OUT" > "$OUT/summary.md"
echo ">> done: $OUT"
