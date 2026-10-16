# STREAMX — The 10-Minute Demo

A scripted walkthrough that shows STREAMX doing real work against a live local cluster: topic creation, replication, consumption, **failure + rebalance**, the DLQ, and a benchmark. Every number on screen comes from the real API — nothing is faked.

The script is [`scripts/demo.sh`](../scripts/demo.sh) (`make demo`). This document is the narration + what to point at.

---

## Before you start (2 min)

**Terminal 1 — the cluster:**
```bash
make build
make dev
```
Wait until the API is up: `curl -s localhost:8080/health`.

**Terminal 2 — the dashboard** (for the audience): open **http://localhost:3000**, sign in with `admin` / `admin123`. Leave it on **Overview**.

**Terminal 3 — the demo:**
```bash
make demo
```

> If you're on Docker instead: `make docker-up` in terminal 1, and run the demo with `BIN` pointing at a built CLI (`make build` then `./scripts/demo.sh`). The workers in the demo run as local processes against the cluster.

---

## The script, step by step

### 1. Authenticate
Log in as `admin` via `POST /api/v1/auth/login`, print the role and a token prefix.
*Say:* "Everything from here is a real authenticated API call. Roles matter — failure injection is admin-only."

### 2. Create the topic
`POST /api/v1/topics` → `orders`, **6 partitions, RF 3**, 7-day retention.
*Say:* "Six partitions means six independent replicated logs — parallelism for both produce and consume. RF 3 means every partition lives on all three brokers."
*Point at:* **Topics** in the dashboard — `orders` appears with 6 partitions.

### 3. Start two workers
Two `streamx-worker` processes, group `orders-group`, handler **`fail:0.15`** (15% of events fail on purpose), 3 retries.
*Say:* "Two consumers, one group. The coordinator splits the 6 partitions — three each. The handler fails 15% of events so we get real DLQ traffic in a minute."
*Point at:* **Consumers → orders-group** — two members, generation 1, assignments `[0,1,2]` / `[3,4,5]`.

### 4. Produce 2,000 events
A producer loop (CLI or REST) sends keyed events (`cust-1..20`), so each customer's orders stay on one partition.
*Say:* "Keyed by customer — that's what gives per-customer ordering."
*Point at:* **Events (live)** — the stream fills in real time. **Overview** — events/s card and chart move.

### 5. Inspect state
The script prints, from the API:
- `GET /api/v1/topics/orders/partitions` — leader, replicas, LEO/HW per partition,
- `GET /api/v1/consumer-groups/orders-group` — generation + member assignments,
- `GET /api/v1/consumer-groups/orders-group/offsets` — committed / latest / **lag** per partition.
*Say:* "LEO is where the log ends, HW is what consumers are allowed to see, and lag = latest − committed. Watch lag stay near zero — the group is keeping up."

### 6. Kill a worker (the money shot)
`kill -9` on worker 1. Wait for the 10 s heartbeat timeout.
*Say:* "I just SIGKILLed a consumer. No graceful shutdown. The coordinator expires it after 10 seconds and rebalances."
*Then the script prints the group again:*
- **generation bumped** (1 → 2),
- one member now holds **all 6 partitions**.
*Point at:* **Consumers → orders-group** in the dashboard — members dropped to 1, assignments `[0..5]`, and the **live lag chart** shows the brief bump as the survivor re-reads the uncommitted tail.
*Say (honesty beat):* "Note the duplicates: events the dead worker processed but hadn't committed get re-read. That's **at-least-once** — your handler must be idempotent. There is no exactly-once in this system."

### 7. DLQ
`GET /api/v1/dlq?topic=orders` — entries from the 15% failing handler that exhausted 3 retries, with partition/offset/key/retry-count/reason. The script **replays** the first one (`POST /api/v1/dlq/{id}/replay`).
*Say:* "Failed events don't vanish and don't block the group — they land in the DLQ with full provenance, and an operator can replay or drop them from the dashboard."
*Point at:* **DLQ** page — the table + detail view with the payload.

### 8. Benchmark
`./bin/streamx bench --events 100000 --payload 512 --producers 4 --consumers 2` against the live cluster. The script then fetches the latest registered run from `GET /api/v1/benchmarks/{id}` and prints **real** throughput, p50/p95/p99 producer & consumer latency, and error rate.
*Say:* "Not a synthetic micro-benchmark — this is the full path: API → leader → replicas → HW → consumers. The numbers on screen are what this machine actually did."
*Point at:* **Benchmarks** page — the run, its charts, and (run it twice) the side-by-side compare.

### Cleanup
The script stops the demo workers and producer. The cluster keeps running for free exploration.

---

## Suggested closing (30 s)

- Open **Failures** (admin) and `kill broker-2` live: brokers flip to `down` in ~1.5 s, leadership moves to the next lowest live id, and the dashboard shows it.
- Open **Grafana** (`:3001`) — the same events as Prometheus panels.
- One-liner: "Everything you saw — offsets, lag, rebalance, DLQ, benchmark — is real state from a ~small, readable Go codebase. The parts where it's *simpler* than Kafka (deterministic leader election, at-least-once) are documented in the repo, not hidden."

---

## If something goes wrong

| Symptom | Fix |
|---|---|
| "API did not come up" | `curl localhost:8080/health`; check `make dev` output for DB errors |
| Workers don't appear in the group | Check `/tmp/streamx-demo-worker-*.log`; ensure brokers are up |
| No DLQ entries | The 15% failure rate needs a few hundred events + retries; wait longer or re-run the produce step |
| Benchmark hangs | Check broker health (`/api/v1/brokers`); a dead broker slows `acks=all` |
| Port conflicts | A previous cluster is still running — stop it first |