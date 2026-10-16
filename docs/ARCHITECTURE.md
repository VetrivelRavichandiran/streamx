# STREAMX — Architecture

This document describes how STREAMX is put together, the design decisions behind it, and — importantly — where it is deliberately **simplified** relative to a production system like Kafka. Every claim is labeled **IMPLEMENTED**, **SIMPLIFIED**, or **FUTURE WORK**.

The normative interface definitions live in [CONTRACTS.md](CONTRACTS.md); this document explains the *why*.

---

## 1. Design goals

1. **Readable.** A new engineer should be able to trace an event from `produce` to `fetch` to `commit` in an afternoon. No code generation, no obscure frameworks.
2. **Honest.** Simplifications are documented, not hidden. If the replication model is not consensus-based, this document says so and explains the consequences.
3. **Observable.** Every interesting thing (offsets, lag, replication, errors, rate limits, backpressure) is a Prometheus metric and/or a REST endpoint.
4. **Breakable on purpose.** Failure injection is a first-class, admin-gated feature, and the test suite includes chaos scenarios.

Non-goals: Kafka wire compatibility, exactly-once, multi-datacenter, dynamic reconfiguration, consensus-based metadata.

---

## 2. Process topology

| Process | Binary | Ports | Role |
|---|---|---|---|
| broker-1/2/3 | `streamx-broker` | 9092-9094 (data), 9102-9104 (metrics) | Append-only logs, partitions, replication, group coordination |
| api | `streamx-api` | 8080 | REST control plane, SSE, auth, rate limiting, DLQ store, benchmark registry |
| worker | `streamx-worker` | — | Consumer-group client with pluggable handlers |
| bench | `streamx-bench` | — | Load generator + benchmark runner |
| streamx | `streamx` | — | Operator CLI |
| web | Next.js | 3000 | Dashboard |
| postgres | — | 5432 | Control-plane metadata (SQLite in dev) |
| redis | — | 6379 | API rate limiting (optional) |
| prometheus / grafana | — | 9090 / 3001 | Observability |

**IMPLEMENTED.**

### Why a separate API process?

Kafka embeds its admin API in the brokers. STREAMX splits the control plane out because:

- The data plane (TCP JSON) and the control plane (REST/SSE) have different auth, rate-limiting, and scaling needs.
- The API owns state that brokers don't need to: users, tokens, audit, DLQ records, benchmark runs.
- It keeps each binary small and testable in isolation.

The cost is an extra hop for some operations (the API asks brokers for live partition state). That trade is fine at this scale.

---

## 3. Data flow

### 3.1 Produce

```
client (SDK/CLI/bench)
  │  1. route event → partition (xxhash64(key) % N, or round-robin)
  │  2. look up partition leader (metadata cache, refreshed on NotLeader)
  ▼
leader broker
  │  3. validate (size, schema, rate limits, backpressure budget)
  │  4. append to active segment (buffered writer, fsync per policy)
  │  5. acks=none → return; acks=leader → return after append+fsync;
  │     acks=all → wait for all live replicas LEO > offset (5s timeout)
  ▼
followers (fetch loop, every 100 ms from their LEO)
  │  6. append, update LEO, report in heartbeat
  ▼
leader computes HW = min(follower LEOs, own LEO)
```

**IMPLEMENTED** (steps 1-6). The `acks=all` wait and HW computation are the heart of the durability story — see [REPLICATION.md](REPLICATION.md).

### 3.2 Consume

```
consumer (worker)
  │  1. group_join → coordinator assigns partitions (range strategy)
  │  2. fetch assigned partitions from committed offset (or start position)
  │  3. handler(event)  [bounded concurrency, bounded queue]
  │      on error: retry in place (100ms·2^i, cap 5s) up to max_retries
  │      on exhaustion: publish to <topic>.dlq + POST /api/v1/dlq
  │  4. commit offsets every 1s or 100 events (offset = next-to-read)
  │  5. heartbeat every 2s; on generation mismatch → rejoin
  ▼
coordinator broker (lowest-id live broker)
```

**IMPLEMENTED.** Delivery is **at-least-once** — see [CONSUMERS.md](CONSUMERS.md) for exactly where duplicates come from.

### 3.3 Control plane

The API talks to the meta store (Postgres/SQLite) for durable state and to brokers (over the data-plane protocol) for live state (partitions, offsets, group membership). SSE endpoints (`/api/v1/stream/events`, `/api/v1/stream/metrics`) fan out live data to the dashboard.

**IMPLEMENTED.**

---

## 4. Key design decisions

### 4.1 Wire protocol: length-prefixed JSON over TCP

**SIMPLIFIED.** A 4-byte big-endian length + UTF-8 JSON frame is trivially debuggable (you can `nc` a broker and read it) at the cost of throughput vs. a binary protocol like Kafka's. For an educational system, debuggability wins. The max frame is 64 MiB.

### 4.2 Storage: segment logs with sparse indexes

**IMPLEMENTED.** `<data_dir>/<topic>/<partition>/segment-<baseOffset>.log` + `.idx`. Records carry a CRC32c; on recovery, a corrupt/incomplete trailing record is truncated (logged). This is the same fundamental design as Kafka's log, which is well-proven. Details in [STORAGE.md](STORAGE.md).

### 4.3 Leader election: deterministic lowest-live-id

**SIMPLIFIED — the biggest one.** For each partition, the leader is the **lowest broker-id among live replicas**. "Live" = seen a gossip heartbeat within `BROKER_TIMEOUT_MS` (default 1.5 s). There is no consensus protocol:

- **No atomicity.** Two brokers can both believe they are leader if the network partitions (split-brain). The contract documents this and notes a generation-based fencing mitigation.
- **No log matching.** On failover the new leader continues from its own LEO; followers truncate up to the leader's LEO and refetch. With `acks=all` and all replicas alive, acked data is never lost; during a partition, a minority-side leader *can* lose unreplicated tail.
- **Why not Raft?** Implementing Raft correctly is a multi-month project with its own failure modes. The deterministic model is ~100 lines, easy to reason about, and demonstrates the *concepts* (leadership, failover, HW visibility) without pretending to have consensus-grade safety.

If you need real safety, replace `internal/broker/cluster` with a Raft implementation — the rest of the system is insulated behind the `partition`/`replication` interfaces. That is **FUTURE WORK**, and the module boundaries are drawn so it can be done.

### 4.4 Group coordination: single coordinator (lowest-id live broker)

**SIMPLIFIED.** All consumer groups are coordinated by one broker. If it dies, consumers rejoin the new coordinator and the generation resets to 1. Kafka spreads coordinators across brokers; here, simplicity wins, and the failure mode is a brief global rebalance.

### 4.5 Assignment: range strategy

**IMPLEMENTED, SIMPLIFIED vs. Kafka's options.** Members are sorted by join time and partitions split evenly (remainder to earliest joiners). No sticky/cooperative rebalance, so every membership change reassigns *all* partitions — which is why rebalances cause the duplicate-processing window described in [CONSUMERS.md](CONSUMERS.md).

### 4.6 Metadata: Postgres (or SQLite)

**IMPLEMENTED.** Topics, brokers, groups, consumers, offsets, DLQ entries, benchmarks, users, tokens, and audit logs all live in the meta store. Offsets are durable across restarts; in-memory group membership is rebuilt from heartbeats. SQLite is a first-class dev fallback so `make dev` works with zero external services.

### 4.7 DLQ in the SDK, recorded in the API

**IMPLEMENTED.** Retry/backoff/DLQ logic lives in the consumer SDK (where the handler runs). The SDK publishes the failed event to `<topic>.dlq` (auto-created) **and** records it via `POST /api/v1/dlq` so the dashboard can show, replay, and drop entries.

### 4.8 Backpressure: bounded everywhere

**IMPLEMENTED.** Per-partition pending-append budget (64 MiB / 100k records) → status 17; fetch `max_bytes`; API produce via a bounded channel → 429; worker handler queue bounded (10k) → the fetch loop blocks. Nothing in the system is allowed to grow unboundedly.

---

## 5. Consistency & delivery model (honest statement)

| Property | Guarantee | Notes |
|---|---|---|
| Delivery | **at-least-once** | Duplicates possible after rebalance/crash. No exactly-once. |
| Ordering | **within a partition**, single keyed producer, `acks=leader/all` | No cross-partition ordering. |
| Replication | HW-based visibility; deterministic election | **Not** consensus. Split-brain possible under partition. |
| Durability | fsync per append batch (default); `acks=all` waits for replicas | `FSYNC_INTERVAL_MS=-1` trades durability for throughput (benchmark mode). |
| Metadata | Durable in Postgres/SQLite | Offsets survive restarts. |

Full detail in [CONTRACTS.md §14](CONTRACTS.md#14-delivery--consistency-model-honest-statement).

---

## 6. Observability

- **Prometheus** metrics on brokers (`:9102-9104`) and the API (`:8080/metrics`) — see [CONTRACTS.md §11](CONTRACTS.md#11-metrics-prometheus-on-brokers--api) for the full list.
- **Grafana** dashboards: `streamx-overview` (cluster health, rates, latency, resources) and `streamx-topics` (per-partition offsets, lag, replication lag).
- **REST**: `/api/v1/metrics/summary` (windowed rates), `/api/v1/cluster`, `/api/v1/brokers`, per-topic/per-group endpoints.
- **SSE**: live event and metric streams for the dashboard.
- **Audit log**: every admin action, login, topic CRUD, DLQ replay, token mint.

**IMPLEMENTED.**

---

## 7. Security model (summary)

Bearer tokens (32 random bytes, hex) with 24 h expiry and roles (`viewer`/`operator`/`admin`), bcrypt password hashing, login rate limiting (5/min/IP), per-token API rate limiting (500 r/s), secure headers, CORS restricted to the web origin, and a full audit log. The data plane is **plaintext TCP with no auth** — a deliberate simplification for local dev. Details and gaps in [SECURITY.md](SECURITY.md).

---

## 8. Testing strategy

- **Unit** (`go test -race ./...`): storage (append/read/recover/corrupt/roll), partition (offsets, HW), groups (join/expire/rebalance), cluster (election), protocol (framing), auth, rate limiter, schema validator.
- **Integration** (`go test -race -tags integration ./tests/...`): real broker processes on ephemeral ports; produce/consume/ordering/restart/kill.
- **Chaos** (`tests/chaos`): consumer kill → rebalance; broker kill → failover; corrupted segment → recovery; slow consumer → lag; overloaded producer → 429.
- **E2E** (`tests/e2e`): full stack via the API: topics → produce → consume → DLQ → benchmark smoke.

See [FAILURE_TESTING.md](FAILURE_TESTING.md) for the chaos scenarios and what each one proves.

---

## 9. What we'd do next (FUTURE WORK, in rough priority order)

1. **Consensus-based leader election** (Raft or a library) to eliminate split-brain.
2. **Exactly-once** via idempotent producer + transactional offsets.
3. **mTLS/SASL** on the data plane; per-topic ACLs.
4. **Cooperative incremental rebalance** to shrink the duplicate window.
5. **Dynamic partition count** and partition reassignment.
6. **Kubernetes manifests** and a HA control plane.
7. **Log compaction** and tiered storage for cold segments.
8. **Kafka protocol compatibility** (a bridge, not a rewrite).