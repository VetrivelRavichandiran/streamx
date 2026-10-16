# STREAMX — Architecture Contracts (v1)

This document is the single source of truth for cross-module interfaces.
Implementation agents MUST conform to these contracts. If a contract proves
unworkable, change it here first, then in code.

## 1. Process topology

| Process | Binary | Default port | Purpose |
|---|---|---|---|
| broker-1/2/3 | `streamx-broker` | 9092/9093/9094 (data plane TCP) | Append-only log, partitions, replication, group coordination |
| broker metrics | same binary | 9102/9103/9104 | `/metrics` (Prometheus), `/healthz` |
| api | `streamx-api` | 8080 | REST control plane, SSE, auth, rate limiting, DLQ store, benchmark registry |
| worker | `streamx-worker` | — | Consumer-group client with pluggable handlers (noop/log/fail/slow) |
| bench | `streamx-bench` | — | Load generator + benchmark runner |
| streamx | `streamx` (CLI) | — | `streamx topic create`, `streamx produce`, `streamx consume`, `streamx bench`, `streamx failure ...` |
| web | Next.js | 3000 | Dashboard |
| postgres | — | 5432 | Control-plane metadata (Docker) |
| redis | — | 6379 | API rate limiting (optional; in-memory fallback) |
| prometheus/grafana | — | 9090/3001 | Observability (Docker) |

Local dev without Docker: `make dev` starts postgres via `tools/pg/pg-server.mjs`
(embedded PG binaries) OR sqlite fallback (`STREAMX_DB_URL=sqlite://./data/streamx.db`).
SQLite is a first-class dev fallback; PostgreSQL is the production store.

## 2. Wire protocol (broker data plane, TCP)

Frame: `[4-byte big-endian length][UTF-8 JSON payload]`. Max frame 64 MiB.

Request:
```json
{ "id": "uuid", "op": "produce", "correlation": 1, "topic": "orders",
  "partition": 0, "body": { } }
```
Response:
```json
{ "id": "uuid", "correlation": 1, "status": 0, "error": "", "body": { } }
```

### Ops

| op | request body | response body |
|---|---|---|
| `ping` | — | `{"ts": ms}` |
| `metadata` | `{"topic": "orders"}` | `{"topic":"orders","partitions":[{"partition":0,"leader":"broker-1","replicas":["broker-1","broker-2"],"high_watermark":10,"log_end_offset":12,"log_start_offset":0}]}` |
| `produce` | `{"acks":"none\|leader\|all","events":[Event]}` | `{"results":[{"partition":0,"offset":5,"error":null}],"leader":"broker-1"}` |
| `fetch` | `{"max_bytes":1048576,"max_wait_ms":500,"min_bytes":1}` | `{"records":[Event],"high_watermark":n,"log_start_offset":n}` |
| `offsets` | `{"which":"earliest\|latest"}` | `{"offset":n}` |
| `group_join` | `{"group":"g","member_id":"m","topic":"orders"}` | `{"generation":n,"assignment":[0,1],"is_leader":false}` |
| `group_heartbeat` | `{"group":"g","member_id":"m","generation":n}` | `{"ok":true,"generation":n,"rebalance_required":false}` |
| `group_leave` | `{"group":"g","member_id":"m"}` | `{"ok":true}` |
| `group_commit` | `{"group":"g","member_id":"m","offsets":{"0":12,"1":7}}` | `{"ok":true}` |
| `group_describe` | `{"group":"g"}` | `{"generation":n,"members":[{"member_id":"m","assignment":[0]}],"offsets":{"0":12}}` |
| `rebalance` | `{"group":"g"}` | `{"generation":n}` |

### Event

```json
{ "id":"evt-uuid","topic":"orders","partition":0,"offset":42,
  "key":"cust-123","payload":"<base64>","headers":{"type":"order.created"},
  "ts":1719000000000,"producer_id":"p-1","seq":7 }
```
`ts` = broker assign time (ms epoch). `seq` = producer sequence (client-chosen).

### Status codes
0 ok · 1 unknown_op · 2 not_found · 3 not_leader · 4 topic_not_found ·
5 partition_not_found · 6 group_not_found · 7 offset_out_of_range ·
8 validation_failed · 9 rate_limited · 10 quota_exceeded ·
11 leader_not_available · 12 not_enough_replicas · 13 group_fenced ·
14 auth_failed · 15 internal · 16 timeout · 17 backpressure · 18 topic_exists

## 3. REST API (control plane, port 8080)

Auth: `Authorization: Bearer <token>` (tokens minted at login; roles:
`admin`, `operator`, `viewer`). `/health` and `/metrics` are public.
Admin-only routes are under `/api/v1/admin/*`.

| Method & path | Role | Notes |
|---|---|---|
| `POST /api/v1/auth/login` | public | `{username,password}` → `{token,role,user:{id,username,org_id}}` |
| `GET /api/v1/auth/me` | any | |
| `POST /api/v1/topics` | operator+ | `{name,partitions:1-64,replication_factor:1-3,retention:{max_age_ms,max_bytes},schema?:{fields:[{name,type,required}]}}` |
| `GET /api/v1/topics` | viewer+ | list with stats |
| `GET /api/v1/topics/{name}` | viewer+ | |
| `PATCH /api/v1/topics/{name}` | operator+ | retention/schema |
| `DELETE /api/v1/topics/{name}` | operator+ | |
| `GET /api/v1/topics/{name}/partitions` | viewer+ | per-partition: leader, replicas, hw, leo, lso, rate |
| `GET /api/v1/topics/{name}/partitions/{p}/events?offset=&max=&key=` | viewer+ | peek (max ≤ 500) |
| `POST /api/v1/topics/{name}/events` | operator+ | `{key?,payload,headers?,acks?}` → `{offset,partition,ts}` |
| `POST /api/v1/topics/{name}/events/batch` | operator+ | `{events:[...],acks?}` |
| `GET /api/v1/consumer-groups` | viewer+ | |
| `GET /api/v1/consumer-groups/{group}` | viewer+ | members, assignments, generation |
| `GET /api/v1/consumer-groups/{group}/offsets` | viewer+ | per partition: committed, latest, lag |
| `DELETE /api/v1/consumer-groups/{group}` | operator+ | |
| `GET /api/v1/cluster` | viewer+ | brokers, topology, summary |
| `GET /api/v1/brokers` / `GET /api/v1/brokers/{id}` | viewer+ | stats: cpu, mem, disk, conns, throughput |
| `GET /api/v1/metrics/summary` | viewer+ | events/sec, bytes/sec, errors/sec (windowed) |
| `GET /api/v1/dlq?topic=&group=&limit=` | viewer+ | |
| `GET /api/v1/dlq/{id}` | viewer+ | |
| `POST /api/v1/dlq/{id}/replay` | operator+ | republish to original topic |
| `POST /api/v1/benchmarks` | operator+ | `{scenario,events,payload_bytes,producers,consumers,partitions,concurrency}` → `{run_id}` |
| `GET /api/v1/benchmarks` / `GET /api/v1/benchmarks/{run_id}` | viewer+ | status + results (throughput, p50/95/99, errors, cpu/mem/disk) |
| `GET /api/v1/benchmarks/compare?ids=a,b` | viewer+ | |
| `POST /api/v1/admin/failures/broker` | admin | `{broker_id,action:stop\|restart\|kill}` |
| `POST /api/v1/admin/failures/consumer` | admin | `{consumer_id,action:stop\|restart\|slow\|pause}` |
| `POST /api/v1/admin/failures/network` | admin | `{target,delay_ms,duration_ms}` |
| `GET /api/v1/admin/failures` | admin | history |
| `GET /api/v1/admin/users` / `POST /api/v1/admin/users` | admin | |
| `GET /api/v1/admin/audit?limit=` | admin | audit log |
| `GET /api/v1/stream/events?topic=&partition=` | viewer+ | SSE: live events |
| `GET /api/v1/stream/metrics` | viewer+ | SSE: metrics snapshot every 2s |

Error envelope: `{"error":{"code":"validation|auth|not_found|rate_limited|conflict|internal","message":"..."}}`
with proper HTTP status (400/401/403/404/409/429/500).

## 4. Metadata store (internal/meta)

Interface `Store` (all methods take `context.Context` first):
`Init`, `Close`, `CreateTopic`, `GetTopic`, `ListTopics`, `UpdateTopic`,
`DeleteTopic`, `UpsertBroker`, `ListBrokers`, `TouchBroker`, `SetBrokerStatus`,
`UpsertGroup`, `UpsertConsumer`, `SetConsumerAssignment`, `ExpireConsumers`,
`GetGroup`, `ListGroups`, `DeleteGroup`, `CommitOffsets`, `GetOffsets`,
`InsertDLQ`, `ListDLQ`, `GetDLQ`, `SetDLQStatus`, `CreateBenchmark`,
`UpdateBenchmark`, `GetBenchmark`, `ListBenchmarks`, `InsertBenchmarkResult`,
`ListBenchmarkResults`, `CreateUser`, `GetUserByUsername`, `ListUsers`,
`CreateToken`, `ValidateToken`, `RevokeToken`, `InsertAudit`, `ListAudit`.

Implementations: `postgres` (pgx/v5) and `sqlite` (modernc.org/sqlite, DSN
`sqlite://<path>`). DDL in `internal/meta/schema.sql` (Postgres) and
`schema_sqlite.sql`. Entities: organizations, users, tokens, topics,
topic_partitions, brokers, consumer_groups, consumers, offsets, dlq_entries,
benchmarks, benchmark_results, audit_logs.

## 5. Cluster coordination (internal/broker/cluster)

- Brokers gossip over TCP: each broker dials every other broker in
  `STREAMX_CLUSTER`, sends `heartbeat` frames every 500 ms carrying
  `{broker_id, topics, per-partition {leader?, leo, hw}}`.
- A broker is **dead** after 3 missed heartbeats (1.5 s) — configurable.
- **Leader election (deterministic):** for each partition, leader = lowest
  broker-id among live replicas. No consensus; documented as a simplified
  model. On leader change, new leader continues from its LEO; followers
  truncate up to leader LEO and refetch (documented: followers may lose
  unreplicated tail on failover — at-least-once, never silent data loss of
  acked data when acks=all and all replicas are alive).
- **Group coordinator:** the lowest-id live broker coordinates ALL consumer
  groups. Consumers discover it via `metadata`/API. If it dies, consumers
  rejoin the new coordinator (generation resets to 1).
- Follower fetch loop: every 100 ms, fetch from leader starting at own LEO,
  append, update LEO; report LEO in heartbeats; leader computes HW =
  min(follower LEOs, own LEO).

## 6. Storage (internal/broker/storage)

- Layout: `<data_dir>/<topic>/<partition>/segment-<baseOffset20>.log` +
  `.idx` (sparse: every 4096 bytes or 100 records: `[relOffset u32][pos u32]`).
- Record binary format (big-endian):
  `[crc32c u64][total_len u32][key_len u32][key][payload_len u32][payload]
   [hdr_count u16][hdr: klen u32 k vlen u32 v]*[producer_seq u64][pid_len u32 pid]`
  `total_len` covers everything after the length field. CRC32c over the same
  span. Corrupt/incomplete trailing record in the active segment → truncate
  to last valid record at recovery (logged).
- Append: buffered (1 MiB) writer; fsync per batch by default
  (`STREAMX_FSYNC_INTERVAL_MS=0` → fsync every append batch; `>0` → batched;
  `-1` → never, benchmark mode — documented trade-off).
- Segments roll at `STREAMX_SEGMENT_MAX_BYTES` (default 64 MiB).
- Retention: background task every 30 s deletes segments where
  `max_offset_ts < now - max_age` or segment bytes > max_bytes budget;
  never deletes the active segment; updates log_start_offset.
- Storage interface: `Append(rec) (offset, error)`, `ReadAt(offset, maxBytes)
  ([]Record, error)`, `TruncateTo(offset)`, `Len()`, `BaseOffset()`,
  `Close()`.

## 7. Consumer groups (internal/broker/groups)

- In-memory on the coordinator broker, persisted to meta store (consumers +
  offsets tables) so offsets survive restarts.
- Join → assign: **range strategy** — members sorted by join time,
  partitions split evenly (remainder to earliest joiners). Generation bumps
  on every membership change.
- Heartbeat every 2 s (SDK); member expired after 10 s → rebalance.
- Heartbeat response carries `generation`; on mismatch, SDK re-joins and
  re-fetches committed offsets. In-flight duplicates possible → at-least-once.
- Offsets: committed per (group, topic, partition). `START_EARLIEST` →
  log_start_offset; `START_LATEST` → log_end_offset (next offset).

## 8. Producer semantics

- `acks=none`: fire-and-forget, no error after local validation.
- `acks=leader`: ack after leader append (fsync per §6).
- `acks=all`: ack after all live replicas have LEO > offset (timeout 5 s →
  error 12).
- Keyed events: partition = xxhash64(key) % N (stable while N unchanged).
  Unkeyed: round-robin per producer client.
- Batch: up to 16 MiB or 10k events; per-event results.
- Rate limits: token bucket per producer_id (default 50k ev/s) and per topic
  (default 200k ev/s); exceed → status 9 with `retry_after_ms` in error body.
- Backpressure: per-partition pending-append budget 64 MiB / 100k records;
  exceed → status 17.

## 9. Retry / DLQ (consumer side, in SDK)

- Handler error → retry in place with exponential backoff (100 ms base,
  ×2, max 5 s) up to `max_retries` (default 3).
- Exhausted → publish to `<topic>.dlq` (auto-created on demand) with headers
  `dlq_original_topic/partition/offset/key/retry_count/reason/ts`, AND
  `POST /api/v1/dlq` (if API URL configured) storing payload + metadata.
- DLQ entry status: `pending` → `replayed` | `dropped`.

## 10. Backpressure & limits (broker + API)

- Bounded: pending-append budget (§8), fetch `max_bytes`, REST produce via
  API uses a bounded channel (10k) → 429 when full.
- API rate limit: per-token token bucket (default 500 req/s; Redis-backed
  when available, in-memory otherwise).
- Worker saturation: handler concurrency bounded (default 8 per consumer);
  queue between fetch and handler bounded (10k) → consumer slows fetch.

## 11. Metrics (Prometheus, on brokers + API)

`streamx_events_produced_total{topic,partition,broker}`
`streamx_events_consumed_total{topic,partition,group}`
`streamx_event_processing_errors_total{topic,group}`
`streamx_producer_latency_seconds{topic}` (histogram)
`streamx_consumer_latency_seconds{topic,group}` (histogram)
`streamx_consumer_lag{topic,group,partition}` (gauge)
`streamx_partition_offset{topic,partition}` (LEO gauge)
`streamx_partition_high_watermark{topic,partition}` (gauge)
`streamx_partition_log_start_offset{topic,partition}` (gauge)
`streamx_replication_lag{topic,partition,leader,follower}` (gauge)
`streamx_broker_connections{broker}` (gauge)
`streamx_active_consumers{group}` (gauge)
`streamx_bytes_produced_total{topic}` / `streamx_bytes_consumed_total{topic}`
`streamx_rebalances_total{group}`
`streamx_dlq_events_total{topic}`
`streamx_fetch_requests_total{topic,partition}`
`streamx_produce_requests_total{topic,partition,acks}`
`streamx_rate_limited_total{scope}`
`streamx_backpressure_rejects_total{topic,partition}`
`streamx_up{broker}` (1 alive)
`streamx_broker_cpu_seconds_total`, `streamx_broker_memory_bytes`,
`streamx_broker_disk_used_bytes` (runtime metrics)

## 12. Config (env vars, all prefixed STREAMX_)

`BROKER_ID` (broker-1) · `BROKER_ADDR` (:9092) · `METRICS_ADDR` (:9102) ·
`CLUSTER` (broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094) ·
`DATA_DIR` (./data/broker-1) · `FSYNC_INTERVAL_MS` (0) ·
`SEGMENT_MAX_BYTES` (67108864) · `RETENTION_CHECK_INTERVAL_MS` (30000) ·
`HEARTBEAT_INTERVAL_MS` (500) · `BROKER_TIMEOUT_MS` (1500) ·
`PRODUCE_RATE_LIMIT` (50000) · `TOPIC_RATE_LIMIT` (200000) ·
`MAX_BATCH_BYTES` (16777216) · `PENDING_APPEND_BUDGET` (67108864)
API: `API_ADDR` (:8080) · `DB_URL` (postgres://streamx:streamx@localhost:5432/streamx or sqlite://./data/streamx.db) ·
`ADMIN_USER` (admin) · `ADMIN_PASSWORD` (admin123 — dev only) ·
`OPERATOR_USER`/`OPERATOR_PASSWORD` (operator/operator123) ·
`RATE_LIMIT_RPS` (500) · `REDIS_URL` ("" → in-memory) · `BROKER_CLUSTER` (same as broker CLUSTER)
Worker: `WORKER_GROUP` · `WORKER_TOPIC` · `WORKER_HANDLER` (noop|log|fail:0.1|slow:200) ·
`WORKER_MAX_RETRIES` (3) · `WORKER_CONCURRENCY` (8) · `WORKER_MEMBER_ID`
Bench: `BENCH_API` (http://localhost:8080) · `BENCH_TOKEN`
Web: `NEXT_PUBLIC_API_URL` (http://localhost:8080)

## 13. Security

- bcrypt password hashing; bearer tokens (32 random bytes, hex) in `tokens`
  table with expiry (24 h) and role; login rate-limited (5/min/IP).
- Roles: viewer (read), operator (read + write topics/events/benchmarks/dlq
  replay), admin (all + failures/users/audit).
- Secure headers on API responses: `X-Content-Type-Options`, `X-Frame-Options`,
  `Cache-Control: no-store` on API routes, CORS restricted to web origin.
- Audit log: login, topic CRUD, admin actions, DLQ replay, token mint.
- Failure endpoints admin-only; all admin actions audited.

## 14. Delivery & consistency model (HONEST STATEMENT)

- Delivery: **at-least-once** (consumer may process duplicates after
  rebalance/crash). No exactly-once.
- Ordering: **within a partition**, for a single producer with a key,
  acks=leader/all. No cross-partition ordering.
- Replication: simplified leader/follower, deterministic leader election
  (lowest live id), HW-based visibility. **Not** Raft/Paxos; no atomic
  leader election; split-brain possible if network partitions (documented
  limitation — both sides may believe themselves leader; mitigated by
  fencing via generation + documented).
- Durability: fsync per append batch by default; acks=all waits for
  replicas.
- Metadata: Postgres (or SQLite in dev); offsets durable; in-memory group
  membership rebuilt from heartbeats.

## 15. Module boundaries (Go)

```
internal/protocol     wire types + framing (deps: none)
internal/logging      slog setup (deps: none)
internal/config       env parsing (deps: none)
internal/metrics      prometheus registries + shared collectors (deps: protocol)
internal/auth         tokens, roles, middleware (deps: meta)
internal/security     rate limiters, secure headers, audit (deps: meta)
internal/meta         Store interface + postgres + sqlite (deps: none)
internal/broker/storage      segment log (deps: protocol, metrics)
internal/broker/partition    partition = storage + hw + append/fetch (deps: storage, metrics)
internal/broker/replication  leader/follower sync (deps: partition, cluster)
internal/broker/groups       group coordinator (deps: meta, metrics)
internal/broker/cluster      gossip, membership, leader election (deps: protocol)
internal/broker/server       TCP server, dispatch, limits (deps: all broker)
sdk/streamx                Go client SDK (deps: protocol)
cmd/broker cmd/api cmd/worker cmd/bench cmd/streamx
```

## 16. Testing

- Unit: storage (append/read/recover/corrupt/roll), partition (offsets, hw),
  groups (join/expire/rebalance), cluster (election), protocol (framing),
  auth, rate limiter, schema validator.
- Integration (tests/integration, `make integration-test`): real broker
  processes on ephemeral ports; produce/consume/ordering/restart/kill.
- Chaos (tests/chaos): consumer kill → rebalance; broker kill → failover;
  corrupted segment → recovery; slow consumer → lag; overloaded producer →
  429/rate limit.
- E2E (tests/e2e): full stack via API: topics → produce → consume → DLQ →
  benchmark smoke.
- All Go tests pass `-race`.

## 17. Ports plan (local dev, single machine)

9092-9094 brokers · 9102-9104 broker metrics · 8080 api · 3000 web ·
5432 postgres · 6379 redis · 9090 prometheus · 3001 grafana