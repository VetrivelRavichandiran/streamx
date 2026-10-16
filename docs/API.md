# STREAMX — API Reference

Complete reference for the STREAMX control-plane REST API (port **8080**) and SSE streams. This mirrors [CONTRACTS.md §3](CONTRACTS.md#3-rest-api-control-plane-port-8080) with request/response examples.

---

## Conventions

- **Base URL:** `http://localhost:8080`
- **Auth:** `Authorization: Bearer <token>` on all routes except `POST /api/v1/auth/login`, `GET /health`, and `GET /metrics` (public).
- **Roles:** `viewer` (read), `operator` (read + write topics/events/benchmarks/DLQ replay), `admin` (all + failures/users/audit).
- **Content type:** `application/json` (request and response).
- **SSE note:** `EventSource` cannot set headers, so the SSE endpoints also accept the token as a `?token=<token>` query parameter.

### Error envelope

All errors use:

```json
{ "error": { "code": "validation|auth|not_found|rate_limited|conflict|internal", "message": "..." } }
```

with the appropriate HTTP status:

| HTTP | code | Meaning |
|---|---|---|
| 400 | `validation` | Bad request / failed validation |
| 401 | `auth` | Missing/invalid/expired token |
| 403 | `auth` | Insufficient role |
| 404 | `not_found` | Resource does not exist |
| 409 | `conflict` | State conflict (e.g. topic exists) |
| 429 | `rate_limited` | Rate limit exceeded |
| 500 | `internal` | Server error |

### List envelopes

List endpoints return either a bare JSON array or an object with an `items`/`data` array. The dashboard's typed client accepts both. (The contract leaves the envelope open; the Go API picks one — see note at the bottom.)

---

## Public

### `GET /health`
Liveness. `200` when the API is up.

### `GET /metrics`
Prometheus metrics (control-plane side). Public.

---

## Auth

### `POST /api/v1/auth/login` *(public)*
```json
// request
{ "username": "admin", "password": "admin123" }
// 200 response
{ "token": "ab12…", "role": "admin",
  "user": { "id": "u-1", "username": "admin", "org_id": "org-1" } }
```
Rate-limited to 5/min/IP. `401` on bad credentials.

### `GET /api/v1/auth/me` *(any role)*
Returns the authenticated user and role.

---

## Topics

### `POST /api/v1/topics` *(operator+)*
```json
// request
{ "name": "orders", "partitions": 6, "replication_factor": 3,
  "retention": { "max_age_ms": 604800000, "max_bytes": 1073741824 },
  "schema": { "fields": [
    { "name": "order_id", "type": "string", "required": true },
    { "name": "amount", "type": "number", "required": false }
  ] } }
// 201 response
{ "name": "orders", "partitions": 6, "replication_factor": 3, ... }
```
Validation: `partitions` 1-64, `replication_factor` 1-3. `409` if the topic exists.

### `GET /api/v1/topics` *(viewer+)*
List topics with stats (events total, bytes, rates).

### `GET /api/v1/topics/{name}` *(viewer+)*
Single topic.

### `PATCH /api/v1/topics/{name}` *(operator+)*
Update retention and/or schema.
```json
{ "retention": { "max_age_ms": 86400000, "max_bytes": 536870912 } }
```

### `DELETE /api/v1/topics/{name}` *(operator+)*
Delete a topic (and its partitions/segments).

### `GET /api/v1/topics/{name}/partitions` *(viewer+)*
Per-partition: leader, replicas, HW, LEO, LSO, rate.
```json
[ { "partition": 0, "leader": "broker-1",
    "replicas": ["broker-1","broker-2","broker-3"],
    "high_watermark": 10, "log_end_offset": 12, "log_start_offset": 0,
    "rate_per_sec": 123.4, "consumer_lag": 5 } ]
```

### `GET /api/v1/topics/{name}/partitions/{p}/events?offset=&max=&key=` *(viewer+)*
Peek at records in a partition. `max` ≤ 500. `key` filters to a key.
```json
[ { "id": "evt-…", "topic": "orders", "partition": 0, "offset": 42,
    "key": "cust-123", "payload": "<base64>",
    "headers": { "type": "order.created" }, "ts": 1719000000000,
    "producer_id": "p-1", "seq": 7 } ]
```

### `POST /api/v1/topics/{name}/events` *(operator+)*
Produce a single event.
```json
// request
{ "key": "cust-123", "payload": "{\"order\":\"o-1\"}",
  "headers": { "type": "order.created" }, "acks": "all" }
// 200 response
{ "offset": 42, "partition": 0, "ts": 1719000000000 }
```
`acks`: `none` | `leader` | `all`. `429` when the API's bounded produce channel is full.

### `POST /api/v1/topics/{name}/events/batch` *(operator+)*
```json
// request
{ "events": [ { "key": "k1", "payload": "…" }, { "payload": "…" } ],
  "acks": "leader" }
// 200 response
[ { "offset": 42, "partition": 0, "ts": … }, { "offset": 43, "partition": 1, "ts": … } ]
```

---

## Consumer groups

### `GET /api/v1/consumer-groups` *(viewer+)*
List groups (member count, generation, state, lag).

### `GET /api/v1/consumer-groups/{group}` *(viewer+)*
```json
{ "group": "orders-group", "topic": "orders", "generation": 4, "state": "stable",
  "members": [
    { "member_id": "demo-worker-1", "assignment": [0,1,2] },
    { "member_id": "demo-worker-2", "assignment": [3,4,5] }
  ] }
```

### `GET /api/v1/consumer-groups/{group}/offsets` *(viewer+)*
Per-partition committed / latest / lag.
```json
[ { "partition": 0, "committed": 100, "latest": 105, "lag": 5 } ]
```

### `DELETE /api/v1/consumer-groups/{group}` *(operator+)*
Delete a group (and its committed offsets).

---

## Cluster & brokers

### `GET /api/v1/cluster` *(viewer+)*
Brokers, topology, and a summary (brokers up/total, topics, partitions, groups, active consumers, rates).

### `GET /api/v1/brokers` *(viewer+)*
List brokers with stats: cpu, mem, disk, connections, throughput.

### `GET /api/v1/brokers/{id}` *(viewer+)*
Single broker + per-partition leadership + replication status.

### `GET /api/v1/metrics/summary` *(viewer+)*
Windowed cluster rates.
```json
{ "events_per_sec": 1234.5, "bytes_per_sec": 632000,
  "errors_per_sec": 0.0, "window_ms": 2000, "ts": 1719000000000 }
```
The dashboard polls this every 2 s for the Overview live cards + chart.

---

## DLQ

### `GET /api/v1/dlq?topic=&group=&limit=` *(viewer+)*
List dead-letter entries.
```json
[ { "id": "dlq-…", "topic": "orders", "group": "orders-group",
    "partition": 2, "offset": 77, "key": "cust-9",
    "payload": "<base64>", "retry_count": 3, "reason": "handler error",
    "status": "pending", "ts": 1719000000000 } ]
```

### `GET /api/v1/dlq/{id}` *(viewer+)*
Single entry (with payload).

### `POST /api/v1/dlq/{id}/replay` *(operator+)*
Republish the event to its original topic. Status → `replayed`.

> **Note:** the contract defines `replay` but not a `drop` endpoint, even though the DLQ status model is `pending → replayed | dropped`. The dashboard calls `POST /api/v1/dlq/{id}/drop` for the Drop button — **the API implementation should add this endpoint** (see "Notes for the Go team" below).

---

## Benchmarks

### `POST /api/v1/benchmarks` *(operator+)*
```json
// request
{ "scenario": "throughput", "events": 100000, "payload_bytes": 512,
  "producers": 4, "consumers": 2, "partitions": 6, "concurrency": 8 }
// 202 response
{ "run_id": "run-…" }
```

### `GET /api/v1/benchmarks` *(viewer+)*
List runs (status + config + result).

### `GET /api/v1/benchmarks/{run_id}` *(viewer+)*
```json
{ "run_id": "run-…", "status": "completed",
  "config": { "scenario": "throughput", "events": 100000, "payload_bytes": 512,
              "producers": 4, "consumers": 2, "partitions": 6, "concurrency": 8 },
  "result": {
    "throughput_events_per_sec": 42000, "throughput_bytes_per_sec": 21504000,
    "producer_latency_ms": { "p50": 1.2, "p95": 4.8, "p99": 12.1 },
    "consumer_latency_ms": { "p50": 0.8, "p95": 3.1, "p99": 9.4 },
    "error_rate": 0.0, "errors": 0, "produced": 100000, "consumed": 100000,
    "duration_ms": 2380, "cpu_percent": 61.0, "memory_bytes": 184549376,
    "disk_bytes_per_sec": 21504000 } }
```

### `GET /api/v1/benchmarks/compare?ids=a,b` *(viewer+)*
Side-by-side of two runs (for the dashboard compare view).

---

## Admin — failures *(admin only)*

### `POST /api/v1/admin/failures/broker`
```json
{ "broker_id": "broker-2", "action": "stop" }   // stop | restart | kill
```
`stop` = graceful shutdown, `restart` = stop+start, `kill` = SIGKILL (crash path).

### `POST /api/v1/admin/failures/consumer`
```json
{ "consumer_id": "demo-worker-1", "action": "slow" }   // stop | restart | slow | pause
```

### `POST /api/v1/admin/failures/network`
```json
{ "target": "broker-1", "delay_ms": 500, "duration_ms": 10000 }
```

### `GET /api/v1/admin/failures`
Failure history (kind, target, action, detail, status, ts).

All admin actions are **audited**.

---

## Admin — users & audit *(admin only)*

### `GET /api/v1/admin/users`
List users (username, role, id, created).

### `POST /api/v1/admin/users`
```json
{ "username": "analyst", "password": "…", "role": "viewer" }
```

### `GET /api/v1/admin/audit?limit=`
Audit log tail (time, user, action, detail, ip).

---

## SSE streams *(viewer+)*

### `GET /api/v1/stream/events?topic=&partition=`
Server-Sent Events: one `data:` per event (JSON `Event`), in real time as produced. `topic`/`partition` optional filters. The dashboard's **Events (live)** page consumes this.

### `GET /api/v1/stream/metrics`
Server-Sent Events: a metrics snapshot every 2 s (same shape as `/api/v1/metrics/summary`).

**Auth:** pass the token as `?token=<token>` (EventSource can't set headers).

---

## Data-plane wire protocol (broker TCP)

The broker data plane is **not** REST — it's a length-prefixed JSON TCP protocol on `:9092-9094`. Ops: `ping`, `metadata`, `produce`, `fetch`, `offsets`, `group_join`, `group_heartbeat`, `group_leave`, `group_commit`, `group_describe`, `rebalance`. Status codes 0-18. Full spec in [CONTRACTS.md §2](CONTRACTS.md#2-wire-protocol-broker-data-plane-tcp). The SDK and CLI speak this protocol; the REST API is the human/dashboard surface.

---

## Notes for the Go team (contract gaps the dashboard relies on)

1. **DLQ drop endpoint.** The status model implies `dropped`; the dashboard calls `POST /api/v1/dlq/{id}/drop`. Please add it (operator+).
2. **List envelopes.** Pick one (recommend bare arrays) and keep it consistent; the client accepts array or `{items|data: []}`.
3. **SSE token.** Accept `?token=` on `/api/v1/stream/*` since `EventSource` can't send headers.
4. **`/api/v1/metrics/summary`** should include `events_per_sec`, `bytes_per_sec`, `errors_per_sec`, `window_ms`, `ts` (the dashboard charts off these).
5. **`/api/v1/cluster`** should include a `summary` object (brokers_total/up, topics, partitions, consumer_groups, active_consumers) and a `brokers` array with per-broker `status`/`cpu_percent`/`memory_bytes`/`disk_used_bytes`/`connections`/`events_per_sec`/`bytes_per_sec`/`partitions_led`.
6. **Broker detail** (`/api/v1/brokers/{id}`) should include `leadership[]` and `replication[]` arrays for the Brokers page.