#!/usr/bin/env bash
# Run the unit + integration suite N times (race detector on) and keep the
# results, so every "proven by <test>" in the docs points at a recorded pass:
#   results/tests/<UTC timestamp>/
#     env.json         hardware, versions, git SHA (bench/hardware.sh)
#     runs.tsv         run, go test exit code, seconds, failed tests
#     run<N>.json.gz   `go test -json` output
#     summary.md       pass/fail/skip per test (taskq-bench -mode summarize-tests)
#
# Usage: bench/integration.sh [reps]     (default 3)
set -uo pipefail
cd "$(dirname "$0")/.."

REPS=${1:-3}
OUT=results/tests/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"

go build -trimpath -o bench/bin/ ./cmd/taskq-bench || exit 1

CONTAINER=taskq-tests-env-redis
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" -p 6391:6379 redis:7.4.12-alpine >/dev/null || exit 1
until docker exec "$CONTAINER" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done
bench/hardware.sh localhost:6391 "$CONTAINER" > "$OUT/env.json"
docker rm -f "$CONTAINER" >/dev/null

printf 'run\texit\tseconds\tfailed\n' > "$OUT/runs.tsv"
for i in $(seq 1 "$REPS"); do
  echo ">> test suite run $i/$REPS"
  t0=$(date +%s)
  go test -race -count=1 -tags integration -json ./... > "$OUT/run$i.json" 2>&1
  code=$?
  failed=$(grep -o '"Action":"fail","Package":"[^"]*","Test":"[^"]*"' "$OUT/run$i.json" \
    | sed 's/.*"Test":"\([^"]*\)"/\1/' | paste -sd, -)
  printf '%d\t%d\t%d\t%s\n' "$i" "$code" "$(( $(date +%s) - t0 ))" "${failed:--}" >> "$OUT/runs.tsv"
  gzip -9 "$OUT/run$i.json"
  echo "   exit $code ${failed:+(failed: $failed)}"
done

bench/bin/taskq-bench -mode summarize-tests -out "$OUT" > "$OUT/summary.md"
echo ">> done: $OUT"
