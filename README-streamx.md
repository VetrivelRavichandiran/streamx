<div align="center">

# ⚡ STREAMX

### A distributed event streaming platform — Kafka-inspired, built from scratch in Go.

**Durable · Partitioned · Replicated · Observable**

<br>

[![Go](https://img.shields.io/badge/Go-1.27+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Next.js](https://img.shields.io/badge/Next.js-15-000000?logo=next.js&logoColor=white)](https://nextjs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/VetrivelRavichandiran/streamx/actions/workflows/ci.yml/badge.svg)](https://github.com/VetrivelRavichandiran/streamx/actions/workflows/ci.yml)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com)
[![Docs](https://img.shields.io/badge/docs-14%20pages-blue)](docs/)

<br>

**A production-grade (educational) log-based event streaming system** — in a codebase small enough to actually read end to end.

</div>

---

> ### 🤝 Honesty first
> This is an **educational system, not a drop-in Kafka replacement.** Every design decision — including the ones that are *intentionally simplified* — is documented and labeled **IMPLEMENTED**, **SIMPLIFIED**, or **FUTURE WORK** throughout this repo. Read [ARCHITECTURE.md](docs/ARCHITECTURE.md) before assuming parity with any production system.

---

## 📑 Table of Contents

- [Why STREAMX](#why-streamx)
- [What it is](#what-it-is)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Features & status](#features--status)
- [Tech stack](#tech-stack)
- [Repository layout](#repository-layout)
- [Documentation](#documentation)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)
- [License](#license)

---

## 🚀 Why STREAMX

Most event-streaming projects are either **too big to understand** (Kafka is ~1M lines) or **too shallow to learn from** (toy examples that break the moment you add replication). STREAMX sits in the middle: a real, working, multi-broker streaming platform you can **read, extend, and break on purpose** — because that's the point.

| You get | You don't have to |
|---|---|
| ✅ Durable, partitioned, replicated append-only logs | ❌ Wade through a million lines of production code |
| ✅ Consumer groups with coordinated rebalancing | ❌ Guess at undocumented design decisions |
| ✅ At-least-once delivery + dead-letter queues | ❌ Accept "it works, don't ask how" |
| ✅ Full REST control plane + live web dashboard | ❌ Trust a black box |
| ✅ Prometheus + Grafana observability out of the box | ❌ Wire up your own telemetry |
| ✅ Every simplification **labeled and documented** | ❌ Discover the gaps in production |

---

## 🧩 What it is

| Component | Binary / Path | What it does |
|---|---|---|
| 🖥️ **Brokers** | `streamx-broker` | Store partitioned, replicated append-only segment logs over a simple TCP JSON wire protocol. Three brokers gossip, elect leaders deterministically, and replicate with a high-watermark visibility model. |
| 🌐 **API** | `streamx-api` | REST control plane on `:8080` — auth (roles), topics, consumer groups, cluster/broker stats, DLQ, benchmarks, live SSE event streams, failure injection (admin), audit log. |
| ⚙️ **Worker** | `streamx-worker` | A consumer-group client with pluggable handlers (`noop` / `log` / `fail:p` / `slow:ms`) that exercises retries and the DLQ. |
| 📈 **Bench** | `streamx-bench` | Load generator + benchmark runner (throughput, latency percentiles, resource usage). |
| 💻 **CLI** | `streamx` | `topic create`, `produce`, `consume`, `bench`, `failure …` |
| 🎛️ **Web** | `apps/web` | A **Next.js 15** dashboard — live event stream, per-partition visualization, consumer lag, broker health, DLQ management, benchmark comparison, and admin failure injection. |
| 🔭 **Observability** | `monitoring/` | Prometheus scrapes brokers + API; two provisioned Grafana dashboards. |

---

## 🏗️ Architecture

<div align="center">

```
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
```

</div>

<details>
<summary><b>🔎 The data path in one breath</b></summary>

1. A **producer** (CLI / worker / bench) opens a TCP connection to a broker and sends a `produce` frame.
2. The broker routes the event to a **partition** (keyed via `xxhash64`, or round-robin) and appends it to the **leader**'s segment log.
3. With `acks=all`, the leader waits for all **live followers** to replicate the record, then advances the **high watermark (HW)**.
4. **Consumers** join a **group**, get a **range assignment**, and `fetch` from the leader up to the HW — giving **at-least-once** delivery.
5. The **API** tails the same partitions and pushes new events to the dashboard over **SSE**; **Prometheus** scrapes each broker's `/metrics` for Grafana.

</details>

---

## ⚡ Quick start

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| [Go](https://go.dev/dl/) | **1.27+** | `go.mod` requires 1.27.1; the toolchain manager fetches it if needed |
| [Node](https://nodejs.org) | **20+** | and `npm` |
| Docker | optional | for the full containerized stack |
| `jq` | any | for the demo script |

### Option A — local dev (no Docker)

```bash
make setup          # go mod download + npm install
make build          # compile all binaries into ./bin
make dev            # postgres (or sqlite) + 3 brokers + api + web
```

`make dev` starts everything in the foreground (Ctrl-C stops all). It uses an embedded Postgres from `tools/pg` if you add one, otherwise **falls back to SQLite** (`data/streamx.db`) — so the dev stack works with **zero external dependencies** out of the box.

> **🪟 On Windows?** `make` isn't available by default — use the one-click PowerShell script instead:
> ```powershell
> go mod download
> cd apps\web; npm install; cd ..
> powershell -ExecutionPolicy Bypass -File scripts\dev.ps1
> ```

Then open:
- 🎛️ Dashboard: **http://localhost:3000** (sign in `admin` / `admin123`)
- 🩺 API health: **http://localhost:8080/health**
- 📊 Prometheus: **http://localhost:9090** (if running)

### Option B — Docker

```bash
make docker-up      # builds images + starts the full stack
make docker-down    # stop + remove
```

This brings up Postgres, Redis, 3 brokers, the API, the web dashboard, Prometheus, and Grafana.

### 🎬 Run the 10-minute demo

With a cluster running (`make dev` or `make docker-up`):

```bash
make demo           # → scripts/demo.sh
```

The demo creates a topic, starts failing workers, produces events, shows offsets/lag, **SIGKILLs a worker to trigger a rebalance**, shows DLQ entries, and runs a 100k-event benchmark — all against the live API. See [DEMO.md](docs/DEMO.md).

---

## ✨ Features & status

> **Legend:** 🟢 **IMPLEMENTED** = works as documented · 🟡 **SIMPLIFIED** = intentionally reduced vs. a production system (documented) · ⚪ **FUTURE WORK** = not yet built.

| Area | Feature | Status |
|---|---|:---:|
| **Storage** | Append-only segment logs with sparse `.idx` | 🟢 |
| | CRC32c per record + crash recovery (truncate corrupt tail) | 🟢 |
| | Configurable fsync (per-batch / batched / never) | 🟢 |
| | Segment rolling + time/size retention | 🟢 |
| | Zero-copy / memory-mapped reads, compaction, tiered storage | ⚪ |
| **Partitioning** | Keyed (xxhash64) + round-robin partitioning | 🟢 |
| | Per-partition leader/follower + high watermark | 🟢 |
| | Dynamic partition count changes, partition reassignment | ⚪ |
| **Replication** | Leader/follower replication, HW-based visibility | 🟢 |
| | **Deterministic lowest-live-id leader election** | 🟡 (not Raft/Paxos) |
| | `acks=all` waits for all live replicas | 🟢 |
| | Atomic leader election, split-brain fencing, ISR shrink/grow | 🟡 / ⚪ |
| **Consumer groups** | Range assignment, generation-based rebalancing | 🟢 |
| | Offsets committed to the meta store (durable) | 🟢 |
| | **At-least-once delivery** (duplicates possible on rebalance/crash) | 🟡 (no exactly-once) |
| | Cooperative incremental rebalance, sticky assignment | ⚪ |
| **Producer** | `acks=none/leader/all`, batching, keyed routing | 🟢 |
| | Per-producer + per-topic token-bucket rate limits | 🟢 |
| | Per-partition backpressure budget | 🟢 |
| | Idempotent producer, transactional producer | ⚪ |
| **DLQ** | Retry with exponential backoff → `<topic>.dlq` + API record | 🟢 |
| | Replay / drop from the dashboard | 🟢 (replay) |
| **Control plane** | REST API, bearer-token auth, roles (viewer/operator/admin) | 🟢 |
| | SSE live event + metrics streams | 🟢 |
| | Audit log, secure headers, CORS, rate limiting | 🟢 |
| **Observability** | Prometheus metrics (brokers + API) | 🟢 |
| | Two provisioned Grafana dashboards | 🟢 |
| | Live web dashboard (events, lag, brokers, DLQ, benchmarks) | 🟢 |
| **Security** | bcrypt passwords, 24 h bearer tokens, login rate limit | 🟢 |
| | **mTLS / SASL between brokers and clients** | ⚪ (plaintext TCP) |
| | Per-topic ACLs, SCIM, OIDC | ⚪ |
| **Ops** | Docker Compose stack, Dockerfiles, CI (vet/fmt/lint/test/docker/web) | 🟢 |
| | Kubernetes manifests, HA control plane | ⚪ |

### ⚠️ The two biggest honest caveats

1. **Replication is a simplified leader/follower model with deterministic lowest-id leader election — not Raft.** Split-brain is possible under a network partition (documented in [REPLICATION.md](docs/REPLICATION.md)).
2. **Delivery is at-least-once.** Consumers may see duplicates after a rebalance or crash. There is no exactly-once.

---

## 🛠️ Tech stack

| Layer | Technology |
|---|---|
| **Language** | Go 1.27 (no CGO — pure-Go SQLite via `modernc.org/sqlite`) |
| **Wire protocol** | Custom JSON frames over TCP (64 MiB max frame) |
| **Storage** | Append-only segment logs + sparse index, CRC32c checksums |
| **Meta store** | PostgreSQL (prod) or SQLite (dev) |
| **Caching / rate limiting** | Redis |
| **Control plane** | `net/http` REST + Server-Sent Events (SSE) |
| **Auth** | Bearer tokens, bcrypt, role-based access control |
| **Web dashboard** | Next.js 15 (App Router) · TypeScript · Tailwind CSS 4 · Recharts |
| **Observability** | Prometheus · Grafana |
| **Packaging** | Docker (one image per binary) · Docker Compose |
| **CI** | GitHub Actions — vet, gofmt, lint, test, Docker build, web build |

---

## 📁 Repository layout

```
cmd/            broker, api, worker, bench, streamx (CLI) entrypoints
internal/       protocol, logging, config, metrics, auth, security, meta,
                broker/{storage,partition,replication,groups,cluster,server}
sdk/streamx     Go client SDK (producer, consumer groups, retry/DLQ)
apps/web/       Next.js 15 dashboard (App Router, TypeScript, Tailwind 4)
monitoring/     prometheus.yml + Grafana provisioning + dashboards
deployments/    Dockerfiles (one per binary) + healthcheck probe
scripts/        demo.sh (10-minute demo) · dev.ps1 (Windows dev stack)
tests/          integration / chaos / e2e (build tags)
docs/           this documentation
```

---

## 📚 Documentation

| Doc | What it covers |
|---|---|
| [🏛️ ARCHITECTURE.md](docs/ARCHITECTURE.md) | Process topology, data flow, design decisions, honest limitations |
| [🖥️ BROKER.md](docs/BROKER.md) | Broker internals: server, dispatch, limits, lifecycle |
| [💾 STORAGE.md](docs/STORAGE.md) | Segment log format, CRC, recovery, retention |
| [🔀 PARTITIONING.md](docs/PARTITIONING.md) | Keyed/round-robin routing, partition state, offsets |
| [🔄 REPLICATION.md](docs/REPLICATION.md) | Leader/follower, HW, deterministic election, split-brain |
| [👥 CONSUMERS.md](docs/CONSUMERS.md) | Groups, range assignment, rebalance, at-least-once, DLQ |
| [📈 PERFORMANCE.md](docs/PERFORMANCE.md) | Tuning knobs, benchmarking, expected characteristics |
| [🔒 SECURITY.md](docs/SECURITY.md) | Auth, roles, tokens, rate limits, audit, what's NOT there |
| [🌐 API.md](docs/API.md) | Full REST + SSE endpoint reference |
| [🧑‍💻 DEVELOPMENT.md](docs/DEVELOPMENT.md) | Local dev, testing, build, contributing |
| [🎬 DEMO.md](docs/DEMO.md) | The 10-minute demo script |
| [💥 FAILURE_TESTING.md](docs/FAILURE_TESTING.md) | Chaos tests: kill broker/consumer, corruption, lag, overload |
| [📜 CONTRACTS.md](docs/CONTRACTS.md) | Cross-module interface contracts (source of truth) |

---

## 📊 Benchmarks

STREAMX ships its own load generator and benchmark runner. Kick one off against a running cluster:

```bash
make benchmark      # 100k events · 512B payload · 4 producers · 2 consumers
```

Or via the API / dashboard for custom scenarios (throughput, latency percentiles, resource usage). Results are registered and comparable in the **Benchmarks** dashboard page.

> **Honest note:** numbers are environment-dependent. Run the benchmark on *your* hardware for figures that mean anything to you — see [PERFORMANCE.md](docs/PERFORMANCE.md) for tuning knobs and expected characteristics.

---

## 🤝 Contributing

Treat STREAMX as a system you can **read, extend, and break on purpose** — that's the point.

1. Fork the repo and create a feature branch (`git checkout -b feat/my-idea`).
2. Make your change. Add or update tests where the behavior is non-trivial.
3. Keep the docs honest — if you simplify or change a design decision, update the matching doc and the status table above.
4. Run the checks before opening a PR:

```bash
make test             # unit tests (-race)
make integration-test # full data-plane tests (real broker + SQLite)
make lint             # go vet (+ golangci-lint if installed)
```

5. Open a pull request. CI runs vet, gofmt, lint, tests, the Docker build, and the web build.

**Good first places to look:** the `FUTURE WORK` rows in the [features table](#-features--status) — idempotent producer, cooperative rebalancing, tiered storage, mTLS/SASL.

---

## 📄 License

**MIT** — see [LICENSE](LICENSE).

---

<div align="center">

<br>

**Built from scratch in Go. Small enough to read, real enough to break.**

<br>

[⬆ Back to top](#streamx)

</div>