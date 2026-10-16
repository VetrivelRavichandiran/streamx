# STREAMX — Performance

This document covers the performance characteristics of STREAMX, the knobs that matter, and how to benchmark. It is honest about what the system is optimized for (readability + correctness) and where it will lose to a tuned production system.

---

## 1. Where time goes

| Path | Dominant cost | Notes |
|---|---|---|
| Produce (`acks=leader`) | fsync (default per-batch) | The single biggest lever. See §3. |
| Produce (`acks=all`) | fsync + replication wait (≤5 s) | Bounded by the slowest live follower. |
| Fetch | buffered read + index seek | Sparse index makes this O(1) seek + short scan. |
| Replication | 100 ms follower fetch loop | Lag is usually 0-1 batches. |
| REST produce (via API) | JSON encode/decode + bounded channel | Adds a hop vs. direct TCP. |

**The JSON wire protocol is a deliberate throughput trade** for debuggability (see [ARCHITECTURE.md §4.1](ARCHITECTURE.md#41-wire-protocol-length-prefixed-json-over-tcp)). A binary protocol would raise the ceiling; it is **FUTURE WORK**.

---

## 2. Tuning knobs

| Knob | Default | Effect |
|---|---|---|
| `STREAMX_FSYNC_INTERVAL_MS` | `0` (per-batch) | `0`=durable per batch · `>0`=batched (faster, small durability window) · `-1`=never (fastest, **crash-loses page cache — benchmark mode only**) |
| `STREAMX_SEGMENT_MAX_BYTES` | 64 MiB | Larger = fewer rolls, bigger index; smaller = faster retention deletes |
| `STREAMX_PENDING_APPEND_BUDGET` | 64 MiB / 100k | Backpressure threshold per partition |
| `STREAMX_PRODUCE_RATE_LIMIT` | 50k ev/s | Per-producer token bucket |
| `STREAMX_TOPIC_RATE_LIMIT` | 200k ev/s | Per-topic token bucket |
| `STREAMX_MAX_BATCH_BYTES` | 16 MiB | Max produce batch |
| `WORKER_CONCURRENCY` | 8 | Handler goroutines per consumer |
| `WORKER_MAX_RETRIES` | 3 | In-place retries before DLQ |
| Partition count | — | More partitions = more parallelism (both produce and consume) |

### The fsync decision

- **Default (`0`)**: fsync every append batch. Durable, and the right default.
- **Batched (`>0`)**: amortizes fsync; you trade a small window of in-memory data for throughput.
- **Never (`-1`)**: the benchmark mode. Use it to measure the *pipeline* (routing, replication, consume) without disk as the bottleneck — and **never in production**, because a crash loses everything in the OS page cache.

---

## 3. Benchmarking

The benchmark runner (`streamx-bench`, or `make benchmark`) drives the **live cluster** through the real API + data plane and reports:

- **throughput** (events/s, bytes/s),
- **producer latency** p50 / p95 / p99,
- **consumer latency** p50 / p95 / p99,
- **error rate**,
- **resource usage** (CPU, peak memory, disk write rate).

### Run it

```bash
make benchmark
# = make build && ./bin/streamx bench --events 100000 --payload 512 --producers 4 --consumers 2
```

Or via the API / dashboard (Benchmarks page):

```
POST /api/v1/benchmarks
{ "scenario": "throughput", "events": 100000, "payload_bytes": 512,
  "producers": 4, "consumers": 2, "partitions": 6, "concurrency": 8 }
```

Runs are registered in the control plane; the dashboard shows a run list, a result detail (with charts), and a **side-by-side comparison of two runs**.

**IMPLEMENTED.**

### Interpreting results

- **Throughput** is bounded by the slowest stage. With `acks=all` + 3 replicas, it's bounded by the slowest follower's fsync. With `acks=none`, it's bounded by the leader's append + the JSON encode.
- **p99 producer latency** with `acks=all` will show the tail of the replication wait — expect it to jump if a follower lags.
- **Consumer latency** reflects handler time + queue wait. A `slow:200` handler will show ~200 ms+ and build lag.
- **Disk write rate** ≈ throughput × (payload + record overhead + index).

### What to expect (order of magnitude, single machine)

These are *illustrative*, not guarantees — actual numbers depend on your disk (NVMe vs. HDD dominates), CPU, and `acks`/fsync settings:

- `acks=leader`, fsync per-batch, NVMe: **tens of thousands of ev/s** for small payloads.
- `acks=all`, 3 replicas: somewhat lower (replication wait).
- `acks=none`, fsync `-1`: highest raw throughput (benchmark mode).

**Run `make benchmark` on your hardware for real numbers** — the dashboard and CLI print them. This document deliberately does not fabricate specific figures.

---

## 4. Scaling characteristics

- **Horizontal (partitions):** throughput scales with partition count until a single broker or the disk is the bottleneck. More partitions = more parallel appends and more consumer parallelism.
- **Brokers:** adding brokers adds storage + replication capacity; leadership is spread by the deterministic election.
- **Consumers:** adding group members up to the partition count adds consume parallelism; beyond the partition count, extra members are idle (range assignment).

**SIMPLIFIED:** no automatic partition scaling, no preferred-leader rebalancing — those are **FUTURE WORK**.

---

## 5. Load testing beyond the benchmark

For sustained/load tests, use the `slow` and `fail` handlers to model real consumer behavior, and watch:

- `streamx_consumer_lag` (is the group keeping up?),
- `streamx_replication_lag` (are followers keeping up?),
- `streamx_backpressure_rejects_total` (is a partition hitting its budget?),
- `streamx_rate_limited_total` (is a producer hitting its token bucket?).

The Grafana `streamx-overview` dashboard shows all of these live.