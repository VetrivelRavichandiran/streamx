# STREAMX — Broker

The broker is the data-plane process. One binary (`streamx-broker`) runs as `broker-1`, `broker-2`, or `broker-3`, distinguished entirely by environment variables. It owns:

- the **TCP server** (data plane, `:9092-9094`) that speaks the length-prefixed JSON protocol,
- the **metrics/health server** (`:9102-9104`) that serves Prometheus `/metrics` and `/healthz`,
- **partitions** (segment log + high watermark + append/fetch),
- **replication** (leader/follower sync),
- **group coordination** (on the lowest-id live broker),
- **cluster gossip** (membership + leader election).

Module map (from [CONTRACTS.md §15](CONTRACTS.md#15-module-boundaries-go)):

```
internal/broker/server       TCP server, dispatch, limits   (deps: all broker)
internal/broker/partition    partition = storage + hw       (deps: storage, metrics)
internal/broker/replication  leader/follower sync           (deps: partition, cluster)
internal/broker/groups       group coordinator              (deps: meta, metrics)
internal/broker/cluster      gossip, membership, election   (deps: protocol)
internal/broker/storage      segment log                    (deps: protocol, metrics)
```

---

## 1. Lifecycle

```
start
  │  parse config (env)
  │  open meta store (postgres/sqlite) — register broker, load topics
  │  open storage for each partition this broker is a replica of
  │  start metrics server (:9102)
  │  start TCP server (:9092)
  │  start gossip loop (dial peers, heartbeat every 500 ms)
  │  start follower fetch loops (for partitions where I'm a follower)
  │  if I'm the lowest-id live broker: start group coordinator
  ▼
run (dispatch loop)
  ▼
shutdown (graceful: flush, unregister, group_leave)  or  SIGKILL (crash path)
```

**IMPLEMENTED.**

### Configuration (env, all `STREAMX_`-prefixed)

| Var | Default | Meaning |
|---|---|---|
| `BROKER_ID` | `broker-1` | Unique id; used for deterministic election |
| `BROKER_ADDR` | `:9092` | Data-plane listen addr |
| `METRICS_ADDR` | `:9102` | Metrics/health listen addr |
| `CLUSTER` | `broker-1=127.0.0.1:9092,…` | Cluster membership (same on every broker) |
| `DATA_DIR` | `./data/broker-1` | Where segments are written |
| `FSYNC_INTERVAL_MS` | `0` | 0=per-batch, >0=batched, -1=never |
| `SEGMENT_MAX_BYTES` | `67108864` | Segment roll size (64 MiB) |
| `RETENTION_CHECK_INTERVAL_MS` | `30000` | Retention sweep period |
| `HEARTBEAT_INTERVAL_MS` | `500` | Gossip heartbeat period |
| `BROKER_TIMEOUT_MS` | `1500` | Dead after this many ms (3 missed heartbeats) |
| `PRODUCE_RATE_LIMIT` | `50000` | ev/s per producer_id |
| `TOPIC_RATE_LIMIT` | `200000` | ev/s per topic |
| `MAX_BATCH_BYTES` | `16777216` | Max produce batch (16 MiB) |
| `PENDING_APPEND_BUDGET` | `67108864` | Per-partition backpressure budget |

---

## 2. Request dispatch

The TCP server reads frames (`[4-byte BE length][JSON]`), decodes the op, and dispatches:

| op | handler | notes |
|---|---|---|
| `ping` | echo `{"ts":ms}` | liveness |
| `metadata` | return topic partition layout | leader, replicas, HW, LEO, LSO |
| `produce` | route to partition leader, append, ack per `acks` | see [PARTITIONING.md](PARTITIONING.md) |
| `fetch` | read from segment, return records + HW | bounded by `max_bytes` |
| `offsets` | return earliest/latest offset | |
| `group_join` / `group_heartbeat` / `group_leave` / `group_commit` / `group_describe` / `rebalance` | group coordinator | see [CONSUMERS.md](CONSUMERS.md) |

Every response carries the request's `id` and `correlation`, a `status` code (0 = ok, 1-18 = specific errors), and an optional `error` string. Status codes are in [CONTRACTS.md §2](CONTRACTS.md#2-wire-protocol-broker-data-plane-tcp).

**IMPLEMENTED.**

### Limits enforced at the broker

- **Frame size** ≤ 64 MiB.
- **Rate limits** (token bucket per producer_id and per topic) → status 9 with `retry_after_ms`.
- **Backpressure** (per-partition pending-append budget) → status 17.
- **Batch size** ≤ `MAX_BATCH_BYTES` and ≤ 10k events.

**IMPLEMENTED.**

---

## 3. Partition state

Each partition the broker hosts (as leader or follower) holds:

- a **segment log** (see [STORAGE.md](STORAGE.md)),
- **LEO** (log end offset — next offset to assign),
- **HW** (high watermark — visible to consumers),
- **LSO** (log start offset — earliest readable, advanced by retention),
- **leader/follower role** (from cluster election),
- per-partition metrics (offsets, rates, replication lag).

The leader is the only one that accepts `produce`. Followers accept `fetch` (prefer leader, but any live replica works) and run the fetch loop.

**IMPLEMENTED.**

---

## 4. Replication

Followers fetch from the leader every 100 ms starting at their own LEO, append, and report their LEO in the next heartbeat. The leader computes `HW = min(follower LEOs, own LEO)`. Consumers only see records up to HW.

Leader election is **deterministic lowest-live-id** — a deliberate simplification. Full details, including the split-brain limitation, are in [REPLICATION.md](REPLICATION.md).

**IMPLEMENTED (mechanism) / SIMPLIFIED (election).**

---

## 5. Group coordination

The **lowest-id live broker** coordinates all consumer groups. It keeps membership in memory (rebuilt from heartbeats) and persists offsets to the meta store. If the coordinator dies, consumers detect the generation mismatch, rejoin the new coordinator, and the generation resets to 1.

**IMPLEMENTED (SIMPLIFIED: single coordinator, not spread across brokers).**

---

## 6. Failure behavior

| Failure | Detection | Recovery |
|---|---|---|
| Broker process dies | 3 missed heartbeats (1.5 s) | Peers mark it dead; partition leadership moves to next lowest live id; consumers rejoin if it was the coordinator |
| Broker SIGKILLed (no clean shutdown) | same | Storage recovery truncates any corrupt trailing record on next start |
| Network partition | heartbeats stop both ways | **Split-brain possible** — both sides may believe themselves leader. Documented limitation. |
| Disk full | append errors | produce returns status 15 (internal); broker stays up |

**IMPLEMENTED** for the first two; **documented limitation** for partitions. See [FAILURE_TESTING.md](FAILURE_TESTING.md).

---

## 7. Metrics

The broker exposes (on `METRICS_ADDR/metrics`):

- `streamx_up{broker}` — liveness gauge
- `streamx_partition_offset{topic,partition}` (LEO), `streamx_partition_high_watermark`, `streamx_partition_log_start_offset`
- `streamx_events_produced_total{topic,partition,broker}`, `streamx_bytes_produced_total`
- `streamx_events_consumed_total`, `streamx_bytes_consumed_total`
- `streamx_replication_lag{topic,partition,leader,follower}`
- `streamx_consumer_lag{topic,group,partition}`, `streamx_active_consumers{group}`, `streamx_rebalances_total{group}`
- `streamx_produce_requests_total{topic,partition,acks}`, `streamx_fetch_requests_total`
- `streamx_producer_latency_seconds`, `streamx_consumer_latency_seconds` (histograms)
- `streamx_rate_limited_total{scope}`, `streamx_backpressure_rejects_total`
- `streamx_broker_connections`, `streamx_broker_cpu_seconds_total`, `streamx_broker_memory_bytes`, `streamx_broker_disk_used_bytes`

Full list in [CONTRACTS.md §11](CONTRACTS.md#11-metrics-prometheus-on-brokers--api).

**IMPLEMENTED.**