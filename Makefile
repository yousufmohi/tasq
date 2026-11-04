GO ?= go
RESULTS ?= $(CURDIR)/results

STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@v0.8.2
GOVULNCHECK ?= golang.org/x/vuln/cmd/govulncheck@v1.9.0

.PHONY: test unit failure vet lint build bench up down

# Unit + integration tests with the race detector. Needs Docker.
test: vet
	$(GO) test -race -count=1 -tags integration ./...

# Unit tests only (no Docker).
unit:
	$(GO) test -race -count=1 ./...

# Failure-injection suite (SIGKILL, Redis restarts, Toxiproxy). Slow.
# Writes one JSON report per test to $(RESULTS)/failure.
failure:
	TASKQ_RESULTS_DIR=$(RESULTS)/failure $(GO) test -race -count=1 -tags failure -v -timeout 30m ./test/failure/

vet:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	$(GO) vet -tags integration ./...
	$(GO) vet -tags failure ./...

lint: vet
	$(GO) run $(STATICCHECK) -tags integration,failure ./...
	$(GO) run $(GOVULNCHECK) ./...

# Benchmark matrix (3 repetitions). Writes bench/results/<timestamp>/.
bench:
	bench/run.sh 3

build:
	$(GO) build -o bin/ ./cmd/...

up:
	docker compose -f deploy/docker-compose.yml up -d --build --wait

down:
	docker compose -f deploy/docker-compose.yml --profile tracing down -v
