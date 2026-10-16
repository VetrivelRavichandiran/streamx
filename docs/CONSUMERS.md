# STREAMX — Consumers

Consumer groups let multiple workers share the load of a topic: each partition is assigned to exactly one member, so the group consumes the topic as a whole while individual members consume a subset.

> **Delivery is at-least-once.** Consumers *may* process duplicates after a rebalance or crash. There is no exactly-once. This document explains exactly where duplicates come from and how to make them harmless.

---

## 1. The consumer loop

A worker (`streamx-worker`) runs the SDK consumer loop:

```
join group (group_join)
  │  → coordinator assigns partitions (range strategy), returns generation
  ▼
fetch assigned partitions from committed offset (or start position)
  │  → bounded fetch→handler queue (10k)
  ▼
handler(event)   [bounded concurrency, default 8 per consumer]
  │  on error: retry in place (100 ms · 2^i, cap 5 s) up to max_retries
  │  on exhaustion: publish to <topic>.dlq + POST /api/v1/dlq
  ▼
commit offsets every 1 s or 100 events (offset = next-to-read)
  │
heartbeat every 2 s
  │  on generation mismatch or rebalance_required: stop fetching, rejoin,
  │  re-read committed offsets
  ▼
(clean shutdown: commit + group_leave)
```

**IMPLEMENTED.**

### Start position

- `START_EARLIEST` → begin at **log_start_offset** (consume everything still retained).
- `START_LATEST` → begin at **log_end_offset** (only new events).
- If the group has a **committed offset** for a partition, it always resumes there.

**IMPLEMENTED.**

---

## 2. Group coordination

The **lowest-id live broker** is the group coordinator. It holds:

- **membership** (in memory, rebuilt from heartbeats),
- **generation** (bumped on every membership change),
- **committed offsets** (persisted to the meta store — durable across restarts).

### Heartbeats & expiration

- Consumers heartbeat every **2 s**.
- A member is **expired after 10 s** without a heartbeat → triggers a rebalance.
- The heartbeat response carries the current **generation**. If a consumer's generation doesn't match, it stops fetching, rejoins, and re-reads committed offsets.

**IMPLEMENTED.**

### If the coordinator dies

All consumers detect the generation mismatch (or the coordinator is unreachable), rejoin the **new** coordinator (next lowest-id live broker), and the **generation resets to 1**. This is a brief global rebalance.

**IMPLEMENTED (SIMPLIFIED: single coordinator, not spread across brokers).**

---

## 3. Assignment: range strategy

**SIMPLIFIED vs. Kafka's options.**

Members are **sorted by join time** and partitions are **split evenly**, with the remainder going to the earliest joiners.

Example: 6 partitions, 2 members → member A gets `[0,1,2]`, member B gets `[3,4,5]`. 7 partitions, 3 members → `[0,1]`, `[2,3]`, `[4,5,6]`.

**Consequence:** every membership change (a member joins or leaves) **reassigns all partitions** — this is a *eager* rebalance, not Kafka's *cooperative incremental* rebalance. That's the root cause of the duplicate-processing window below. **FUTURE WORK:** sticky/cooperative assignment to shrink it.

**IMPLEMENTED (range).**

---

## 4. Offsets & the at-least-once window

Offsets are committed **per (group, topic, partition)** as the *next offset to read*. Commit cadence: every **1 s** or every **100 processed events**, whichever first.

### Where duplicates come from

Because the offset is committed *after* the handler runs (and at most every 1 s / 100 events), there's a window of **processed-but-not-yet-committed** events. If any of these happens, those events are re-read on the next fetch:

1. **Rebalance** — a member is reassigned a partition it was already processing. The new owner starts from the last *committed* offset, re-reading the uncommitted tail.
2. **Crash / SIGKILL** — the member dies before its next commit. On rejoin (or when another member takes the partition), the uncommitted tail is re-read.
3. **Slow commit** — under high throughput, up to ~100 events per partition may be in flight between commits.

This is **at-least-once**: every event is delivered ≥1 time; some may be delivered >1 time.

### Making it safe

- **Idempotent handlers.** Design your handler so reprocessing the same event is a no-op (e.g. upsert by event id, dedupe on a processed-set). This is the standard, correct approach for at-least-once.
- **Smaller commit window** (FUTURE WORK: commit per-event, at the cost of throughput).

**IMPLEMENTED (at-least-once). SIMPLIFIED (no exactly-once).**

---

## 5. Retry & DLQ

When the handler returns an error:

1. **Retry in place** with exponential backoff: `100 ms · 2^i`, capped at **5 s**, up to `max_retries` (default **3**).
2. On exhaustion:
   - Publish the event to **`<topic>.dlq`** (auto-created on demand) with headers `dlq_original_topic/partition/offset/key/retry_count/reason/ts`.
   - `POST /api/v1/dlq` to record it in the control plane (if the API URL is configured).
   - Continue with the next event (the group does not stall).

DLQ entries have a status: `pending` → `replayed` | `dropped`. The dashboard lists, inspects, **replays** (republish to the original topic), and drops entries.

**IMPLEMENTED.**

### Handler types (worker)

`STREAMX_WORKER_HANDLER` selects a built-in handler for demos/testing:

| Handler | Behavior |
|---|---|
| `noop` | ignore events (fastest) |
| `log` | log each event |
| `fail:<p>` | fail with probability `p` (e.g. `fail:0.15` → 15% fail) — exercises retry + DLQ |
| `slow:<ms>` | add `ms` of latency per event — builds lag |

**IMPLEMENTED.**

---

## 6. Backpressure

- The **fetch→handler queue is bounded (10k)**. When it's full, the fetch loop **blocks** — the consumer naturally slows its fetch rate instead of buffering unboundedly.
- **Handler concurrency is bounded** (default 8 per consumer).
- This is how a slow handler produces **lag** (visible via `streamx_consumer_lag` and the dashboard) rather than an OOM.

**IMPLEMENTED.**

---

## 7. Observing consumers

- `GET /api/v1/consumer-groups` — list groups (member count, generation, state, total lag).
- `GET /api/v1/consumer-groups/{group}` — members + assignments.
- `GET /api/v1/consumer-groups/{group}/offsets` — per-partition committed / latest / **lag**.
- Metrics: `streamx_consumer_lag{topic,group,partition}`, `streamx_active_consumers{group}`, `streamx_rebalances_total{group}`, `streamx_event_processing_errors_total{topic,group}`.
- Dashboard: **Consumers** page (group list → detail with members, assignments, per-partition lag table, live lag chart).

**IMPLEMENTED.**

---

## 8. What's FUTURE WORK

1. **Exactly-once** — idempotent producer + transactional offset commits.
2. **Cooperative incremental rebalance** — shrink the duplicate window.
3. **Sticky assignment** — minimize partition movement on rebalance.
4. **Per-event commit** option (trade throughput for a smaller at-least-once window).
5. **Consumer lag alerting** wired to a notifier (currently metric + dashboard only).