# STREAMX — Replication

Replication is how STREAMX survives broker failures without losing acked data. This document describes the model precisely — and is equally precise about what it is **not**.

> **Read this first:** STREAMX replication is a **simplified leader/follower** model with **deterministic lowest-live-id leader election**. It is **not** Raft, **not** Paxos, and **not** consensus-based. It is correct for the failure modes it targets (single-broker crash) and has a **documented split-brain limitation** under network partitions.

---

## 1. Model

Each partition has:

- **one leader** — the only replica that accepts `produce`,
- **N-1 followers** (for replication factor N) — they replicate from the leader.

### Follower fetch loop

Every **100 ms**, each follower:

1. Fetches from the leader starting at its own **LEO**.
2. Appends the returned records, advancing its LEO.
3. Reports its LEO in the next gossip heartbeat.

### High watermark (HW)

The leader computes:

```
HW = min(follower LEOs, leader LEO)
```

Consumers only read up to **HW**. This is the key invariant: a consumer never sees a record that hasn't been replicated to all live followers. It's the same visibility mechanism Kafka uses.

**IMPLEMENTED.**

---

## 2. Leader election — deterministic lowest-live-id

**SIMPLIFIED. This is the central honesty point of the system.**

For each partition, the **leader is the lowest broker-id among live replicas.** "Live" means the broker has been seen in gossip within `STREAMX_BROKER_TIMEOUT_MS` (default 1500 ms = 3 missed 500 ms heartbeats).

Example: replicas `[broker-1, broker-2, broker-3]`, all live → **broker-1** leads. broker-1 dies → **broker-2** leads. broker-2 also dies → **broker-3** leads.

### Why this design

- **~100 lines, trivially debuggable.** No state machine to replicate, no log matching, no voting rounds.
- **Deterministic.** Every broker computes the same answer from the same membership view, so there's no election protocol to get wrong in the common case.
- **Fast failover.** The moment a peer is marked dead, leadership is implicit — no election latency.

### What it does NOT give you

| Property | Raft/Kafka | STREAMX |
|---|---|---|
| Atomic leader election | yes (quorum) | **no** |
| Log matching before serving | yes (log matching / truncation to leader) | partial (followers truncate to leader LEO on failover) |
| Split-brain under partition | prevented (quorum) | **possible** |
| Safety under minority partition | yes | **no** |

### The split-brain scenario (documented limitation)

Suppose the network partitions `broker-1` from `{broker-2, broker-3}`:

- `{broker-2, broker-3}` see broker-1 as dead → **broker-2 becomes leader** and accepts produces.
- `broker-1`, isolated, still sees itself as the lowest id → **it also believes it is leader** and accepts produces (if clients can still reach it).
- Both sides now have divergent logs.

**Mitigations (partial):**
- Clients route produces via the API/metadata, which reflects the *API's* view of the cluster — so in practice most clients follow the majority side.
- A **generation** counter fences stale group-coordinator state (consumers rejoin on generation mismatch).
- On re-partition, followers **truncate up to the leader's LEO** and refetch, so the majority side's log wins for the partitions it leads.

**Residual risk:** produces that reached the isolated minority-side leader *and* were acked with `acks=all` on that side can be lost when the partition heals (they were never replicated to the majority). This is the price of not having consensus. It is **documented, accepted, and the reason this system is educational, not a production substitute.**

**FUTURE WORK:** replace `internal/broker/cluster` election with Raft (or a library like etcd) to eliminate this. The module boundaries are drawn so the rest of the system is insulated.

---

## 3. Failover sequence (single-broker crash — the supported case)

```
t=0      broker-1 (leader of p0) SIGKILLed
t≤1.5s   broker-2, broker-3 miss 3 heartbeats → mark broker-1 dead
t≈1.5s   p0 leader = broker-2 (next lowest live id)
         broker-2 continues from its own LEO
t≈1.6s   broker-3 (follower) truncates up to broker-2's LEO, refetches
t≈2s     consumers see a brief HW stall, then resume
         (if broker-1 was the group coordinator, consumers rejoin → generation reset)
```

**Data safety in this case:** with `acks=all` and all replicas alive *before* the crash, every acked offset was replicated to broker-2 and broker-3, so **nothing acked is lost.** With `acks=leader`, records in broker-1's unflushed/unreplicated tail *can* be lost — that's the durability/latency trade the `acks` setting buys.

**IMPLEMENTED.** Exercised by the chaos test "broker kill → failover" — see [FAILURE_TESTING.md](FAILURE_TESTING.md).

---

## 4. `acks` semantics (durability)

| acks | When the producer is acked | Durability |
|---|---|---|
| `none` | immediately after local validation | none — can lose on any crash |
| `leader` | after leader append (+fsync per policy) | survives leader crash only if replicated; else tail loss |
| `all` | after **all live replicas** have LEO > offset (5 s timeout → error 12) | survives single-broker crash (all replicas alive) |

**IMPLEMENTED.**

---

## 5. Replication lag metric

`streamx_replication_lag{topic,partition,leader,follower}` = `leader LEO - follower LEO`. A healthy cluster sits at 0; a slow follower (disk, network) shows a growing gap, and the Grafana `streamx-topics` dashboard alerts visually on it.

**IMPLEMENTED.**

---

## 6. What's FUTURE WORK

1. **Consensus-based election** (Raft) — eliminates split-brain.
2. **ISR (in-sync replica) shrink/grow** — currently "all live replicas" count; a real ISR with a lag threshold would let you serve from the healthy subset.
3. **Unleader / preferred-leader rebalancing** — move leadership to balance load.
4. **Replication factor changes** on existing topics.
5. **Quorum reads** (read from a majority, not just the leader) for post-partition consistency.