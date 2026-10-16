# STREAMX — Storage

The broker's storage layer is an **append-only segment log** — the same fundamental design as Kafka's log, implemented from scratch in Go. This document describes the on-disk format, the recovery path, and retention.

Module: `internal/broker/storage` (deps: `protocol`, `metrics`).

---

## 1. On-disk layout

```
<data_dir>/
  <topic>/
    <partition>/
      segment-00000000000000000000.log     # active or rolled segment
      segment-00000000000000000000.idx     # sparse index
      segment-0000000000000065536.log      # next segment (base offset 65536)
      segment-0000000000000065536.idx
      ...
```

- **One directory per partition.** The base offset is encoded in the segment filename as a 20-digit zero-padded integer, so segments sort lexicographically.
- **`.log`** holds the records; **`.idx`** is a sparse index into the `.log`.
- Segments roll at `STREAMX_SEGMENT_MAX_BYTES` (default **64 MiB**).

**IMPLEMENTED.**

### Storage interface

```go
Append(rec) (offset, error)   // assign next offset, write record, return offset
ReadAt(offset, maxBytes) ([]Record, error)
TruncateTo(offset)            // drop records >= offset (failover / retention)
Len()                         // number of records (LEO - LSO)
BaseOffset()                  // first offset in this segment
Close()
```

**IMPLEMENTED.**

---

## 2. Record binary format

Every record is self-describing and carries its own checksum. Big-endian throughout:

```
[crc32c u64][total_len u32][key_len u32][key][payload_len u32][payload]
[hdr_count u16][hdr: klen u32 k vlen u32 v]*[producer_seq u64][pid_len u32 pid]
```

- `total_len` covers everything **after** the length field (i.e. from `key_len` to the end).
- `crc32c` is computed over that same span.
- `producer_seq` is the client-chosen sequence number; `pid` is the producer id.
- Headers are an arbitrary set of key/value strings (e.g. `type=order.created`, or DLQ metadata).

**Why CRC32c?** It's hardware-accelerated on modern CPUs and catches both corruption and truncation. On read, a record whose CRC doesn't match is treated as corrupt.

**IMPLEMENTED.**

### The wire Event vs. the stored Record

The REST/SSE `Event` (JSON, with base64 payload) is the *transport* representation. On disk, records are the compact binary form above. The broker converts between them at the protocol boundary. The `ts` field is **broker assign time** (ms epoch), not client time.

---

## 3. Appends

- A **1 MiB buffered writer** backs each active segment.
- **fsync policy** (`STREAMX_FSYNC_INTERVAL_MS`):
  - `0` (default) — fsync **per append batch**. Durable after each batch.
  - `>0` — batch fsyncs every N ms (amortizes fsync cost, small durability window).
  - `-1` — **never fsync** (benchmark mode). Fast, but data in the OS page cache is lost on a crash. Documented trade-off; do not use in production.
- Appends are **synchronous with respect to the ack** for `acks=leader`/`acks=all` (the fsync happens before the ack). For `acks=none` the append is fire-and-forget.

**IMPLEMENTED.**

---

## 4. Sparse index (`.idx`)

The index maps **relative offset → file position** so a `ReadAt(offset)` can seek directly instead of scanning.

- Entry: `[relOffset u32][pos u32]` (8 bytes each).
- **Sparse**: an entry is written every **4096 bytes** of data or every **100 records**, whichever comes first.
- To read offset `O`: binary-search the index for the last entry with `relOffset <= O`, seek to its `pos`, then scan forward to `O`.

The sparsity keeps the index small (a 64 MiB segment has at most ~16k entries = 128 KiB) while keeping reads O(1) seek + short scan.

**IMPLEMENTED.**

---

## 5. Crash recovery

On startup, the broker opens each partition's active segment and **validates the tail**:

1. Read records from the last known-good position.
2. Stop at the first record that is (a) truncated (incomplete length), or (b) fails its CRC32c.
3. **Truncate the segment to the last valid record** and log what was dropped.
4. Re-derive LEO from the surviving records.

This means a crash mid-write can lose the *in-flight* record (and any partial bytes after it), but never corrupts committed data. Because appends are fsync'd per batch (default), the in-flight window is one batch.

**IMPLEMENTED.** This is exercised by the chaos test "corrupted segment → recovery" — see [FAILURE_TESTING.md](FAILURE_TESTING.md).

---

## 6. Retention

A background task runs every `STREAMX_RETENTION_CHECK_INTERVAL_MS` (default **30 s**):

- Delete **rolled** segments where `max_offset_ts < now - max_age` (time-based) **or** where the segment's bytes push the partition over its `max_bytes` budget (size-based).
- **Never delete the active segment.**
- After deletion, advance **log_start_offset** (LSO) to the first surviving offset. Consumers requesting an offset below LSO get `offset_out_of_range` (status 7).

Retention is configured per topic (`retention.max_age_ms`, `retention.max_bytes`) via the API.

**IMPLEMENTED.**

---

## 7. Backpressure

Per partition, the broker tracks a **pending-append budget** (`STREAMX_PENDING_APPEND_BUDGET`, default 64 MiB / 100k records). If unflushed appends exceed the budget, new produces to that partition get **status 17 (backpressure)** rather than being queued unboundedly. This is how a slow disk or a bursty producer is contained without OOM.

**IMPLEMENTED.**

---

## 8. What's NOT here (FUTURE WORK)

- **Zero-copy / `sendfile`** reads — reads go through the buffer.
- **Log compaction** (keyed latest-value retention) — not implemented.
- **Tiered storage** (offload cold segments to object storage) — not implemented.
- **Preallocation / punch-hole** — segments grow by append.
- **Read-ahead / prefetch** — reads are on-demand.

These are performance/feature additions, not correctness gaps. The current design is correct and testable; the additions are optimizations.