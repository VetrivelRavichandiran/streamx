# STREAMX — Failure Testing

STREAMX is designed to be **broken on purpose**. Failure injection is a first-class, admin-gated feature of the API, and the test suite includes chaos scenarios that verify the system recovers correctly. This document lists each scenario, how to trigger it, what should happen, and what it proves.

> **Honest scope:** these tests verify the *supported* failure modes (single-broker crash, consumer crash, corruption, lag, overload). They do **not** verify safety under network partitions — that limitation is documented in [REPLICATION.md](REPLICATION.md) and is a known gap, not a tested guarantee.

---

## 1. How failures are injected

### Via the API (admin)
| Endpoint | Actions |
|---|---|
| `POST /api/v1/admin/failures/broker` | `stop` (graceful) · `restart` (stop+start) · `kill` (SIGKILL) |
| `POST /api/v1/admin/failures/consumer` | `stop` · `restart` · `slow` (add handler latency) · `pause` (stop processing) |
| `POST /api/v1/admin/failures/network` | `{target, delay_ms, duration_ms}` — inject delay |
| `GET /api/v1/admin/failures` | history of injected failures |

The **Failures** page in the dashboard exposes all of these, and the broker/consumer pages update live so you can watch recovery.

### Via the CLI
`streamx failure …` mirrors the admin endpoints.

### In tests
`tests/chaos` drives the same operations programmatically against ephemeral clusters.

---

## 2. Scenarios

### 2.1 Consumer kill → rebalance
- **Inject:** SIGKILL one worker (or `failures/consumer` → `stop`).
- **Expected:**
  - Coordinator expires the member after **10 s** (heartbeat timeout).
  - **Generation bumps**; remaining members are reassigned all partitions (range strategy).
  - A brief **lag bump** as the survivor re-reads the uncommitted tail (at-least-once duplicates).
  - Lag returns to ~0 once caught up.
- **Proves:** group self-healing, generation-based fencing, at-least-once semantics.
- **Status:** IMPLEMENTED (chaos test + demo step 6).

### 2.2 Broker kill → failover
- **Inject:** SIGKILL `broker-1` (a partition leader).
- **Expected:**
  - Peers mark it dead after **3 missed heartbeats (1.5 s)**.
  - Partition **leadership moves to the next lowest live id** (deterministic election).
  - Followers truncate up to the new leader's LEO and refetch.
  - If the killed broker was the **group coordinator**, consumers rejoin the new coordinator (generation resets to 1).
  - `acks=all` produces that were replicated before the crash are **not lost**; unreplicated `acks=leader` tail **may** be lost (documented trade).
- **Proves:** failover, deterministic election, HW visibility, coordinator handoff.
- **Status:** IMPLEMENTED (chaos test). **Limitation:** split-brain under partition is NOT tested-safe — see [REPLICATION.md §2](REPLICATION.md#2-leader-election--deterministic-lowest-live-id).

### 2.3 Broker graceful stop → restart
- **Inject:** `failures/broker` → `stop`, then `restart`.
- **Expected:** clean shutdown (flush + unregister), same failover as above, and on restart the broker re-registers, recovers its segments, and re-joins as a follower.
- **Proves:** clean lifecycle + storage recovery on restart.
- **Status:** IMPLEMENTED.

### 2.4 Corrupted segment → recovery
- **Inject:** flip bytes in a `.log` file (or kill mid-write), restart the broker.
- **Expected:** on startup, the active segment is validated; the corrupt/incomplete trailing record is **truncated to the last valid record** (logged). LEO is re-derived. No committed data is corrupted.
- **Proves:** CRC32c integrity + crash recovery.
- **Status:** IMPLEMENTED (chaos test).

### 2.5 Slow consumer → lag
- **Inject:** `failures/consumer` → `slow` (or a `slow:200` handler) while producing.
- **Expected:** `streamx_consumer_lag` grows; the fetch loop **blocks on the bounded queue** (backpressure) instead of OOMing; lag is visible in the dashboard + Grafana; lag drains when the handler speeds up.
- **Proves:** backpressure, lag observability, no unbounded memory growth.
- **Status:** IMPLEMENTED.

### 2.6 Overloaded producer → rate limit / 429
- **Inject:** produce above `PRODUCE_RATE_LIMIT` (per-producer) or `TOPIC_RATE_LIMIT` (per-topic).
- **Expected:** excess produces get **status 9** (data plane) with `retry_after_ms`, or **HTTP 429** (API, when the bounded produce channel is full). `streamx_rate_limited_total` increments.
- **Proves:** token-bucket rate limiting + bounded API produce path.
- **Status:** IMPLEMENTED.

### 2.7 Backpressure (per-partition budget)
- **Inject:** burst produces faster than the leader can fsync (e.g. slow disk or `acks=all` with a lagging follower).
- **Expected:** once the pending-append budget (64 MiB / 100k records) is exceeded, new produces to that partition get **status 17 (backpressure)**; `streamx_backpressure_rejects_total` increments.
- **Proves:** bounded in-flight appends.
- **Status:** IMPLEMENTED.

### 2.8 Network delay
- **Inject:** `failures/network` → `{target: broker-2, delay_ms: 500, duration_ms: 10000}`.
- **Expected:** replication lag to/from broker-2 grows (`streamx_replication_lag`); HW advances more slowly; if the delay exceeds `BROKER_TIMEOUT_MS`, broker-2 is marked dead and failover occurs (then it rejoins).
- **Proves:** the system degrades observably under latency, and the timeout threshold drives failover.
- **Status:** IMPLEMENTED (injection + observability). **Caveat:** a *partition* (asymmetric cut) is the split-brain case and is a documented limitation, not a tested-safe behavior.

---

## 3. What each scenario asserts (test invariants)

| Invariant | Checked by |
|---|---|
| No committed offset is lost on single-broker crash (with `acks=all`) | 2.2 |
| Leadership always = lowest live id | 2.2, 2.3 |
| Generation bumps exactly on membership change | 2.1 |
| Corrupt tail is truncated, valid prefix intact | 2.4 |
| Lag is bounded and drains; memory bounded | 2.5 |
| Rate limit returns 429/status 9 with `retry_after_ms` | 2.6 |
| Backpressure returns status 17 past budget | 2.7 |
| All admin failure actions are audited | all (audit log) |

---

## 4. Running the chaos suite

```bash
make integration-test     # includes tests/chaos under the integration tag
```

Individual scenarios are Go tests in `tests/chaos` (build tag `integration`), each spinning up an ephemeral cluster on random ports, injecting the failure, and asserting the invariants above. They run with `-race`.

---

## 5. Manual chaos (dashboard)

1. Sign in as **admin**.
2. Open **Failures**.
3. Pick a broker → **Kill**. Watch **Brokers** (status flips to `down` in ~1.5 s) and **Topics** (leadership column changes).
4. Pick a consumer → **Slow**. Watch **Consumers** (lag climbs) and the **Overview** errors/lag.
5. Open **Failures → history** — every action is listed with a timestamp.

This is the fastest way to build intuition: the dashboard and Grafana show the same state the tests assert.

---

## 6. Known gaps (FUTURE WORK)

- **Partition-tolerance safety** — no test can *prove* split-brain safety because the election isn't consensus-based. The gap is documented, not hidden.
- **Multi-broker simultaneous failure** — beyond the replication factor, data is lost (expected); not a "recovery" scenario.
- **Chaos under sustained load** — the current chaos tests run at modest rates; a soak+chaos combination is a good next test.