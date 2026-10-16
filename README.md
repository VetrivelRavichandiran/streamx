STREAMX
A distributed event streaming platform — Kafka-inspired, built from scratch in Go, with an honest engineering story.

STREAMX is a production-grade (educational) log-based event streaming system. It gives you durable, partitioned, replicated append-only logs; consumer groups with coordinated rebalancing; at-least-once delivery; dead-letter queues; a full REST control plane; a live web dashboard; and Prometheus/Grafana observability — in a codebase small enough to actually read end to end.

Honesty first. This is an educational system, not a drop-in Kafka replacement. Every design decision — including the ones that are simplified compared to Kafka — is documented and labeled IMPLEMENTED, SIMPLIFIED, or FUTURE WORK throughout this repo. Read ARCHITECTURE.md and the per-subsystem docs before assuming parity with any production system.

What it is
Brokers (streamx-broker) — store partitioned, replicated append-only segment logs over a simple TCP JSON wire protocol. Three brokers gossip, elect leaders deterministically, and replicate with a high-watermark visibility model.
API (streamx-api) — REST control plane on :8080: auth (roles), topics, consumer groups, cluster/broker stats, DLQ, benchmarks, live SSE event streams, failure injection (admin), audit log.
Worker (streamx-worker) — a consumer-group client with pluggable handlers (noop/log/fail:p/slow:ms) that exercises retries and the DLQ.
Bench (streamx-bench) — load generator + benchmark runner (throughput, latency percentiles, resource usage).
CLI (streamx) — topic create, produce, consume, bench, failure ….
Web (apps/web) — a Next.js 15 dashboard: live event stream, per-partition visualization, consumer lag, broker health, DLQ management, benchmark comparison, and admin failure injection.
Observability — Prometheus scrapes brokers + API; two provisioned Grafana dashboards.
Architecture
                        ┌────────────────────────────────────────────────────────┐
                        │                        Browsers                         │
                        └───────────────┬───────────────────────────┬────────────┘
                                        │ :3000                     │ :3001
                                 ┌──────▼──────┐             ┌──────▼──────┐
                                 │  Next.js    │             │   Grafana   │
                                 │  dashboard  │             │ (dashboards)│
                                 └──────┬──────┘             └──────▲──────┘
                                        │ /api (rewrite)           │ :9090
                                 ┌──────▼──────┐             ┌──────┴──────┐
                                 │  streamx-api│◄────────────│ Prometheus  │
                                 │  :8080 REST │  scrape     │  :9090      │
                                 │  + SSE      │             └──────▲──────┘
                                 └──┬─────┬────┘                    │ scrape :9102-9104
                    meta store      │     │                          │
              ┌─────────────────────┘     └──────────────┐           │
        ┌─────▼─────┐                            ┌───────▼─────┐     │
        │ Postgres  │  (or SQLite in dev)        │    Redis    │     │
        │  :5432    │  topics/brokers/groups/    │  :6379      │     │
        │           │  offsets/DLQ/bench/users   │ rate limiting│     │
        └───────────┘                            └─────────────┘     │
                                                                     │
   ┌──────────────────────────────  DATA PLANE (TCP :9092-9094)  ─────┴──────┐
   │                                                                          │
   │   ┌──────────────┐   gossip 500ms   ┌──────────────┐   ┌──────────────┐  │
   │   │  broker-1    │◄────────────────►│  broker-2    │◄─►│  broker-3    │  │
   │   │  :9092       │   heartbeats +   │  :9093       │   │  :9094       │  │
   │   │  metrics     │   leader election│  metrics     │   │  metrics     │  │
   │   │  :9102       │   (lowest live   │  :9103       │   │  :9104       │  │
   │   └──────┬───────┘    id)           └──────┬───────┘   └──────┬───────┘  │
   │          │  produce/fetch/group_commit     │                  │          │
   └──────────┼─────────────────────────────────┼──────────────────┼──────────┘
              │                                 │                  │
        ┌─────▼─────────────────────────────────▼──────────────────▼─────┐
        │  SDK clients: streamx CLI · streamx-worker · streamx-bench      │
        └────────────────────────────────────────────────────────────────┘
Quick start
Prerequisites
Go 1.27+ (go.mod requires 1.27.1; the toolchain manager fetches it if needed)
Node 20+ and npm
Docker (optional — for the full containerized stack)
jq (for the demo script)
Option A — local dev (no Docker)
make setup          # go mod download + npm install
make build          # compile all binaries into ./bin
make dev            # postgres (or sqlite) + 3 brokers + api + web
make dev starts everything in the foreground (Ctrl-C stops all). It uses an embedded Postgres from tools/pg if you add one, otherwise falls back to SQLite (data/streamx.db) — so the dev stack works with zero external dependencies out of the box.

Then open:

Dashboard: http://localhost:3000 (sign in admin / admin123)
API health: http://localhost:8080/health
Prometheus: http://localhost:9090 (if running)
Option B — Docker
make docker-up      # builds images + starts the full stack
make docker-down    # stop + remove
This brings up Postgres, Redis, 3 brokers, the API, the web dashboard, Prometheus, and Grafana.

Run the 10-minute demo
With a cluster running (make dev or make docker-up):

make demo           # → scripts/demo.sh
The demo creates a topic, starts failing workers, produces events, shows offsets/lag, SIGKILLs a worker to trigger a rebalance, shows DLQ entries, and runs a 100k-event benchmark — all against the live API. See DEMO.md.

Features & status
Status legend: IMPLEMENTED = works as documented · SIMPLIFIED = intentionally reduced vs. a production system (documented) · FUTURE WORK = not yet built.

Area	Feature	Status
Storage	Append-only segment logs with sparse .idx	IMPLEMENTED
CRC32c per record + crash recovery (truncate corrupt tail)	IMPLEMENTED
Configurable fsync (per-batch / batched / never)	IMPLEMENTED
Segment rolling + time/size retention	IMPLEMENTED
Zero-copy / memory-mapped reads, compaction, tiered storage	FUTURE WORK
Partitioning	Keyed (xxhash64) + round-robin partitioning	IMPLEMENTED
Per-partition leader/follower + high watermark	IMPLEMENTED
Dynamic partition count changes, partition reassignment	FUTURE WORK
Replication	Leader/follower replication, HW-based visibility	IMPLEMENTED
Deterministic lowest-live-id leader election	SIMPLIFIED (not Raft/Paxos)
acks=all waits for all live replicas	IMPLEMENTED
Atomic leader election, split-brain fencing, ISR shrink/grow	SIMPLIFIED / FUTURE WORK
Consumer groups	Range assignment, generation-based rebalancing	IMPLEMENTED
Offsets committed to the meta store (durable)	IMPLEMENTED
At-least-once delivery (duplicates possible on rebalance/crash)	SIMPLIFIED (no exactly-once)
Cooperative incremental rebalance, sticky assignment	FUTURE WORK
Producer	acks=none/leader/all, batching, keyed routing	IMPLEMENTED
Per-producer + per-topic token-bucket rate limits	IMPLEMENTED
Per-partition backpressure budget	IMPLEMENTED
Idempotent producer, transactional producer	FUTURE WORK
DLQ	Retry with exponential backoff → <topic>.dlq + API record	IMPLEMENTED
Replay / drop from the dashboard	IMPLEMENTED (replay)
Control plane	REST API, bearer-token auth, roles (viewer/operator/admin)	IMPLEMENTED
SSE live event + metrics streams	IMPLEMENTED
Audit log, secure headers, CORS, rate limiting	IMPLEMENTED
Observability	Prometheus metrics (brokers + API)	IMPLEMENTED
Two provisioned Grafana dashboards	IMPLEMENTED
Live web dashboard (events, lag, brokers, DLQ, benchmarks)	IMPLEMENTED
Security	bcrypt passwords, 24 h bearer tokens, login rate limit	IMPLEMENTED
mTLS / SASL between brokers and clients	FUTURE WORK (plaintext TCP)
Per-topic ACLs, SCIM, OIDC	FUTURE WORK
Ops	Docker Compose stack, Dockerfiles, CI (vet/fmt/lint/test/docker/web)	IMPLEMENTED
Kubernetes manifests, HA control plane	FUTURE WORK
The two biggest honest caveats:

Replication is a simplified leader/follower model with deterministic lowest-id leader election — not Raft. Split-brain is possible under a network partition (documented in REPLICATION.md).
Delivery is at-least-once. Consumers may see duplicates after a rebalance or crash. There is no exactly-once.
Repository layout
cmd/            broker, api, worker, bench, streamx (CLI) entrypoints
internal/       protocol, logging, config, metrics, auth, security, meta,
                broker/{storage,partition,replication,groups,cluster,server}
sdk/streamx     Go client SDK (producer, consumer groups, retry/DLQ)
apps/web/       Next.js 15 dashboard (App Router, TypeScript, Tailwind 4)
monitoring/     prometheus.yml + Grafana provisioning + dashboards
deployments/    Dockerfiles (one per binary) + healthcheck probe
scripts/        demo.sh (10-minute demo)
tests/          integration / chaos / e2e (build tags)
docs/           this documentation
Documentation index
Doc	What it covers
ARCHITECTURE.md	Process topology, data flow, design decisions, honest limitations
BROKER.md	Broker internals: server, dispatch, limits, lifecycle
STORAGE.md	Segment log format, CRC, recovery, retention
PARTITIONING.md	Keyed/round-robin routing, partition state, offsets
REPLICATION.md	Leader/follower, HW, deterministic election, split-brain
CONSUMERS.md	Groups, range assignment, rebalance, at-least-once, DLQ
PERFORMANCE.md	Tuning knobs, benchmarking, expected characteristics
SECURITY.md	Auth, roles, tokens, rate limits, audit, what's NOT there
API.md	Full REST + SSE endpoint reference
DEVELOPMENT.md	Local dev, testing, build, contributing
DEMO.md	The 10-minute demo script
FAILURE_TESTING.md	Chaos tests: kill broker/consumer, corruption, lag, overload
CONTRACTS.md	Cross-module interface contracts (source of truth)
Status
Educational, production-grade-in-spirit. The storage, replication, and consumer-group code is written to be correct and testable, and every simplification is documented. It is not a production Kafka substitute: no consensus-based leader election, no exactly-once, no mTLS/SASL, no dynamic reconfiguration. Treat it as a system you can read, extend, and break on purpose — that's the point.

License
MIT — see LICENSE.
