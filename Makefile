# STREAMX — developer Makefile
#
# Local dev (no Docker):  make dev        (postgres via tools/pg OR sqlite, 3 brokers, api, web)
# Docker:                 make docker-up / docker-down
#
# All Go targets require a Go toolchain (1.27+; go.mod requires 1.27.1 and the
# GOTOOLCHAIN mechanism fetches it if needed).

SHELL       := /bin/bash
BIN         := bin
GO          ?= go
NPM         ?= npm
WEB         := apps/web

BROKER_IDS  := broker-1 broker-2 broker-3
BROKER_PORTS := 9092 9093 9094
BROKER_METRICS := 9102 9103 9104

# Default cluster wiring (override with STREAMX_CLUSTER=... make dev)
CLUSTER ?= broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094

# DB: postgres via the embedded tools/pg server when present, else sqlite.
PG_SERVER := tools/pg/pg-server.mjs
DB_URL    ?=

.PHONY: all
all: build

# ---------------------------------------------------------------------------
# setup — install Go deps + web deps
# ---------------------------------------------------------------------------
.PHONY: setup
setup:
	$(GO) mod download
	cd $(WEB) && $(NPM) install

# ---------------------------------------------------------------------------
# dev — start the full local stack in the foreground (Ctrl-C stops all)
# ---------------------------------------------------------------------------
.PHONY: dev
dev:
	@mkdir -p $(BIN) data
	@echo "==> STREAMX dev cluster"
	@echo "    cluster: $(CLUSTER)"
	@bash -c '\
		BROKER_PORTS=(9092 9093 9094); \
		BROKER_METRICS=(9102 9103 9104); \
		CLUSTER="$(CLUSTER)"; \
		GO="$(GO)"; \
		if [ -f "$(PG_SERVER)" ]; then \
			echo "==> postgres: tools/pg (embedded) on :5432"; \
			DB_URL=$${DB_URL:-postgres://streamx:streamx@127.0.0.1:5432/streamx}; \
			node $(PG_SERVER) --port 5432 --user streamx --password streamx --db streamx --data ./data/pg & PG_PID=$$!; \
		else \
			echo "==> postgres: not found — falling back to sqlite (data/streamx.db)"; \
			DB_URL=$${DB_URL:-sqlite://./data/streamx.db}; \
			PG_PID=""; \
		fi; \
		for i in 0 1 2; do \
			n=$$((i+1)); \
			echo "==> broker-$$n on :$${BROKER_PORTS[$$i]} (metrics :$${BROKER_METRICS[$$i]})"; \
			STREAMX_BROKER_ID=broker-$$n \
			STREAMX_BROKER_ADDR=:$${BROKER_PORTS[$$i]} \
			STREAMX_METRICS_ADDR=:$${BROKER_METRICS[$$i]} \
			STREAMX_CLUSTER="$$CLUSTER" \
			STREAMX_DATA_DIR=./data/broker-$$n \
			$$GO run ./cmd/broker & \
		done; \
		echo "==> api on :8080 (DB: $$DB_URL)"; \
		STREAMX_API_ADDR=:8080 STREAMX_DB_URL="$$DB_URL" \
		STREAMX_ADMIN_USER=$${STREAMX_ADMIN_USER:-admin} \
		STREAMX_ADMIN_PASSWORD=$${STREAMX_ADMIN_PASSWORD:-admin123} \
		STREAMX_OPERATOR_USER=$${STREAMX_OPERATOR_USER:-operator} \
		STREAMX_OPERATOR_PASSWORD=$${STREAMX_OPERATOR_PASSWORD:-operator123} \
		STREAMX_REDIS_URL=$${STREAMX_REDIS_URL:-} \
		STREAMX_BROKER_CLUSTER="$$CLUSTER" \
		$$GO run ./cmd/api & \
		echo "==> web on :3000"; \
		cd $(WEB) && $(NPM) run dev & \
		echo "==> all processes running — Ctrl-C to stop"; \
		wait'

# ---------------------------------------------------------------------------
# build — compile all Go binaries into ./bin
# ---------------------------------------------------------------------------
.PHONY: build
build:
	@mkdir -p $(BIN)
	$(GO) build -o $(BIN)/streamx-broker ./cmd/broker
	$(GO) build -o $(BIN)/streamx-api ./cmd/api
	$(GO) build -o $(BIN)/streamx-worker ./cmd/worker
	$(GO) build -o $(BIN)/streamx-bench ./cmd/bench
	$(GO) build -o $(BIN)/streamx ./cmd/streamx
	@echo "==> built: $(BIN)/streamx-broker streamx-api streamx-worker streamx-bench streamx"

# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------
.PHONY: test
test:
	$(GO) test -race ./...

.PHONY: integration-test
integration-test:
	$(GO) test -race -tags integration ./tests/...

.PHONY: lint
lint:
	$(GO) vet ./...
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed — ran go vet only"

.PHONY: security
security:
	@echo "==> gosec (if installed)"
	@command -v gosec >/dev/null 2>&1 && gosec ./... || echo "gosec not installed — skipping"
	@echo "==> govulncheck (if installed)"
	@command -v govulncheck >/dev/null 2>&1 && govulncheck ./... || echo "govulncheck not installed — skipping"
	@echo "==> npm audit (web)"
	cd $(WEB) && $(NPM) audit --omit=dev || true

# ---------------------------------------------------------------------------
# benchmark — 100k events, 512B payload, 4 producers, 2 consumers
# ---------------------------------------------------------------------------
.PHONY: benchmark
benchmark: build
	./bin/streamx bench --events 100000 --payload 512 --producers 4 --consumers 2

# ---------------------------------------------------------------------------
# docker
# ---------------------------------------------------------------------------
.PHONY: docker-up
docker-up:
	docker compose up -d --build

.PHONY: docker-down
docker-down:
	docker compose down

# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------
.PHONY: clean
clean:
	rm -rf $(BIN) data
	rm -rf $(WEB)/.next $(WEB)/node_modules
	docker compose down -v --remove-orphans 2>/dev/null || true

# ---------------------------------------------------------------------------
# demo — 10-minute engineering demo (needs a running cluster: make dev)
# ---------------------------------------------------------------------------
.PHONY: demo
demo:
	./scripts/demo.sh