GO ?= go

.PHONY: test unit vet fmt up down

# Everything, with the race detector. Integration tests need Docker.
test: vet
	$(GO) test -race -count=1 -tags integration ./...

# Unit tests only (no Docker).
unit:
	$(GO) test -race -count=1 ./...

vet:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	$(GO) vet -tags integration ./...

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down
