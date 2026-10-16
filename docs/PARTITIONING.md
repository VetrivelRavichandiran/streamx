# STREAMX — Partitioning

A **topic** is split into **partitions** — independent, ordered, replicated append-only logs. Partitioning is how STREAMX scales write throughput (partitions append in parallel) and read parallelism (consumers split partitions across group members).

---

## 1. Topic & partition model

A topic is created with:

- `partitions` (1-64),
- `replication_factor` (1-3) — how many brokers hold a copy of each partition,
- `retention` (`max_age_ms`, `max_bytes`),
- optional `schema` (fields with name/type/required — validated on produce).

Each partition has:

- a **leader** (accepts produces),
- **replicas** (leader + followers),
- **LEO** (log end offset — next offset to assign),
- **HW** (high watermark — highest offset visible to consumers),
- **LSO** (log start offset — earliest readable offset, advanced by retention).

Offsets are **per-partition, monotonically increasing, gap-free** (from LSO to LEO).

**IMPLEMENTED.**

### Creating a topic

```
POST /api/v1/topics
{ "name": "orders", "partitions": 6, "replication_factor": 3,
  "retention": { "max_age_ms": 604800000, "max_bytes": 1073741824 } }
```

The API writes the topic + its partitions to the meta store and assigns replicas to brokers (spread across the cluster). Brokers pick up the new partition on their next metadata sync.

**IMPLEMENTED.**

---

## 2. Partition assignment for events

When a producer sends events, each event is routed to a partition:

| Event has… | Routing |
|---|---|
| a **key** | `partition = xxhash64(key) % N` — **stable** for a given key while N is unchanged |
| **no key** | **round-robin** per producer client |

- **Keyed** events guarantee that all events for the same key land on the same partition — which is what gives you *per-key ordering*.
- **Unkeyed** events are spread for load balancing; no ordering guarantee.

`N` is the partition count. **Changing the partition count invalidates keyed routing** (the same key can map to a different partition) — so partition count is set at creation and changing it is **FUTURE WORK** (would require a rebalance/repartition plan).

**IMPLEMENTED** (routing). **SIMPLIFIED** (no dynamic partition count).

---

## 3. Offsets & visibility

- **LEO** advances as records are appended (on the leader).
- **HW** = `min(follower LEOs, leader LEO)` — the leader only advances HW as fast as the slowest live follower. Consumers read **up to HW**, so they only ever see *committed* (replicated) data.
- **LSO** advances as retention deletes old segments.

The gap between HW and LEO is the "in-flight replication" window — records the leader has but not all followers yet.

**IMPLEMENTED.**

### Offset semantics

- `offsets which=earliest` → LSO
- `offsets which=latest` → LEO (the *next* offset to be assigned)
- A consumer's **committed offset** is the *next offset to read* (i.e. one past the last processed).

**IMPLEMENTED.**

---

## 4. Per-partition state (what the API shows)

`GET /api/v1/topics/{name}/partitions` returns, per partition:

```
{ "partition": 0, "leader": "broker-1",
  "replicas": ["broker-1","broker-2","broker-3"],
  "high_watermark": 10, "log_end_offset": 12, "log_start_offset": 0,
  "rate_per_sec": 123.4, "consumer_lag": 5 }
```

The dashboard renders this as the **partition visualization** (one column per partition: leader/replicas, LSO→LEO bar, HW tick, per-partition consumer lag).

**IMPLEMENTED.**

---

## 5. Peeking events

`GET /api/v1/topics/{name}/partitions/{p}/events?offset=&max=&key=` lets you read records directly from a partition (max 500) without a consumer group — useful for debugging and for the dashboard's event inspector. `key` filters to a specific key (scans the range).

**IMPLEMENTED.**

---

## 6. Ordering guarantees (honest)

- **Within a partition**, for a **single producer** using a key, with `acks=leader` or `acks=all`: ordering is preserved.
- **Across partitions**: no ordering guarantee (that's the point of partitioning).
- **Multiple producers** to the same partition: ordering is by append time, not by any client sequence — `producer_seq` is recorded but not used for ordering.

**IMPLEMENTED.**

---

## 7. What's FUTURE WORK

- **Dynamic partition count** (add/remove partitions with a repartition plan).
- **Partition reassignment** (move replicas between brokers for rebalancing disk).
- **Per-partition rate limits** (currently per-topic and per-producer).
- **Key-scoped compaction** (retain latest value per key).