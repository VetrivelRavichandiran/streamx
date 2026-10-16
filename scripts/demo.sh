#!/usr/bin/env bash
# =============================================================================
# STREAMX — 10-minute engineering demo
#
# Prereq: a running local cluster (in another terminal: `make dev`)
#   - brokers on :9092-9094, api on :8080, web on :3000
#   - `make build` done (binaries in ./bin)
#
# What this script does (all real API calls, no fakes):
#   1. Log in (admin) → token
#   2. Create topic `orders` (6 partitions, RF 3)
#   3. Start 2 workers (group orders-group, handler fail:0.15)
#   4. Produce ~2000 events from a loop
#   5. Show topic/partition stats, group membership, offsets & lag
#   6. SIGKILL one worker → show rebalance (generation bump, new assignment)
#   7. Show DLQ entries produced by the failing handler
#   8. Run a 100k-event benchmark and print real results
#
# Usage: ./scripts/demo.sh   (from the repo root)
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------
API="${API:-http://localhost:8080}"
BIN="${BIN:-./bin}"
TOPIC="orders"
GROUP="orders-group"
PARTITIONS=6
RF=3
EVENTS=2000
BENCH_EVENTS=100000
PAUSE="${PAUSE:-3}"          # seconds to let things settle
WORKER_COUNT=2

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
c_reset=$'\033[0m'; c_dim=$'\033[2m'; c_bold=$'\033[1m'
c_cyan=$'\033[36m'; c_green=$'\033[32m'; c_yellow=$'\033[33m'; c_red=$'\033[31m'

step()  { echo; echo "${c_bold}${c_cyan}━━━ $(printf '%-64s') ${c_reset}"; }
info()  { echo "${c_dim}  $*${c_reset}"; }
ok()    { echo "${c_green}  ✓ $*${c_reset}"; }
warn()  { echo "${c_yellow}  ! $*${c_reset}"; }
die()   { echo "${c_red}  ✗ $*${c_reset}" >&2; exit 1; }

# api <method> <path> [json-body]
api() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
  [ -n "$body" ] && args+=(-d "$body")
  curl "${args[@]}" "${API}${path}"
}

json_get() { # json_get <json> <jq-expr>
  echo "$1" | jq -r "$2" 2>/dev/null || echo "null"
}

wait_for() { # wait_for <desc> <url> [timeout]
  local desc="$1" url="$2" timeout="${3:-30}"
  for _ in $(seq 1 "$timeout"); do
    if curl -fsS "$url" >/dev/null 2>&1; then ok "$desc is up"; return 0; fi
    sleep 1
  done
  die "$desc did not come up within ${timeout}s ($url)"
}

# ---------------------------------------------------------------------------
# 0. preflight
# ---------------------------------------------------------------------------
echo "${c_bold}STREAMX — 10-minute demo${c_reset}"
info "API: $API"

wait_for "API" "$API/health" 30
[ -x "$BIN/streamx" ] || die "$BIN/streamx not found — run \`make build\` first"

# ---------------------------------------------------------------------------
# 1. login
# ---------------------------------------------------------------------------
step "1/8  Authenticate (admin)"
LOGIN=$(curl -sS -X POST -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' "$API/api/v1/auth/login") \
  || die "login failed"
TOKEN=$(json_get "$LOGIN" '.token')
[ "$TOKEN" != "null" ] && [ -n "$TOKEN" ] || die "no token in login response: $LOGIN"
ok "role=$(json_get "$LOGIN" '.role')  token=${TOKEN:0:12}…"

# ---------------------------------------------------------------------------
# 2. create topic
# ---------------------------------------------------------------------------
step "2/8  Create topic \`$TOPIC\` ($PARTITIONS partitions, RF $RF)"
CREATE=$(api POST "/api/v1/topics" \
  "{\"name\":\"$TOPIC\",\"partitions\":$PARTITIONS,\"replication_factor\":$RF,
    \"retention\":{\"max_age_ms\":604800000,\"max_bytes\":1073741824}}")
if echo "$CREATE" | jq -e '.error' >/dev/null 2>&1; then
  code=$(json_get "$CREATE" '.error.code')
  if [ "$code" = "conflict" ]; then
    info "topic already exists — continuing"
  else
    die "create topic failed: $CREATE"
  fi
else
  ok "topic created"
fi
sleep "$PAUSE"

# ---------------------------------------------------------------------------
# 3. start workers (group orders-group, handler fail:0.15)
# ---------------------------------------------------------------------------
step "3/8  Start $WORKER_COUNT workers (group=$GROUP, handler=fail:0.15)"
WORKER_PIDS=()
for i in $(seq 1 "$WORKER_COUNT"); do
  STREAMX_WORKER_GROUP="$GROUP" \
  STREAMX_WORKER_TOPIC="$TOPIC" \
  STREAMX_WORKER_HANDLER="fail:0.15" \
  STREAMX_WORKER_MAX_RETRIES=3 \
  STREAMX_WORKER_MEMBER_ID="demo-worker-$i" \
  "$BIN/streamx-worker" >"/tmp/streamx-demo-worker-$i.log" 2>&1 &
  WORKER_PIDS+=("$!")
  info "worker $i started (pid ${WORKER_PIDS[-1]})"
done
sleep "$((PAUSE + 2))"

# ---------------------------------------------------------------------------
# 4. produce events
# ---------------------------------------------------------------------------
step "4/8  Produce $EVENTS events (producer loop, keyed by customer)"
PRODUCER_PID=""
if [ -x "$BIN/streamx" ]; then
  # Prefer the CLI producer if it supports a count loop; otherwise fall back
  # to REST produce in a bash loop (always available).
  "$BIN/streamx" produce "$TOPIC" --count "$EVENTS" --key-pattern "cust-{n}" \
    --payload '{"order":"demo","amount":42.5}' >"/tmp/streamx-demo-producer.log" 2>&1 &
  PRODUCER_PID=$!
  info "CLI producer started (pid $PRODUCER_PID)"
  # Give it a moment; if it's not a real loop command it will exit fast and we
  # fall through to the REST loop below.
  sleep 2
  if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
    wait "$PRODUCER_PID" 2>/dev/null || true
    PRODUCER_PID=""
  fi
fi
if [ -z "$PRODUCER_PID" ]; then
  info "using REST produce loop (POST /api/v1/topics/$TOPIC/events)"
  (
    n=0
    while [ "$n" -lt "$EVENTS" ]; do
      k=$(( (n % 20) + 1 ))
      curl -sS -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"key\":\"cust-$k\",\"payload\":\"{\\\"order\\\":\\\"demo-$n\\\",\\\"amount\\\":$(( n % 97 ))}\"}" \
        "$API/api/v1/topics/$TOPIC/events" >/dev/null 2>&1 || true
      n=$((n + 1))
    done
  ) &
  PRODUCER_PID=$!
fi
sleep "$((PAUSE + 3))"
info "producer done (or still running in background)"

# ---------------------------------------------------------------------------
# 5. cluster state: partitions, group, offsets/lag
# ---------------------------------------------------------------------------
step "5/8  Inspect cluster state"

echo
info "Topic partitions (GET /api/v1/topics/$TOPIC/partitions):"
api GET "/api/v1/topics/$TOPIC/partitions" | jq -r '
  (if type=="array" then . else (.items // .data // []) end) as $ps
  | $ps[]
  | "  p\(.partition)  leader=\(.leader)  replicas=\(.replicas|join(","))  LEO=\(.log_end_offset)  HW=\(.high_watermark)  start=\(.log_start_offset)"' \
  || warn "could not parse partition list"

echo
info "Consumer group (GET /api/v1/consumer-groups/$GROUP):"
GEN_BEFORE=$(api GET "/api/v1/consumer-groups/$GROUP" | jq -r '.generation // 0')
api GET "/api/v1/consumer-groups/$GROUP" | jq -r '
  "  generation=\(.generation)  members=\(.members|length)",
  (.members[]? | "  member \(.member_id)  assignment=\(.assignment|join(","))")' \
  || warn "could not parse group"

echo
info "Committed offsets & lag (GET /api/v1/consumer-groups/$GROUP/offsets):"
api GET "/api/v1/consumer-groups/$GROUP/offsets" | jq -r '
  (if type=="array" then . else (.items // .data // []) end)[]
  | "  p\(.partition)  committed=\(.committed)  latest=\(.latest)  lag=\(.lag)"' \
  || warn "could not parse offsets"

# ---------------------------------------------------------------------------
# 6. kill one worker (SIGKILL) → rebalance
# ---------------------------------------------------------------------------
step "6/8  SIGKILL worker 1 → watch rebalance"
KILLED_PID="${WORKER_PIDS[0]}"
kill -9 "$KILLED_PID" 2>/dev/null || true
ok "sent SIGKILL to pid $KILLED_PID"
info "waiting $((PAUSE + 3))s for the coordinator to expire the member (10s heartbeat timeout)…"
sleep "$((PAUSE + 3))"

echo
info "Group after kill:"
GEN_AFTER=$(api GET "/api/v1/consumer-groups/$GROUP" | jq -r '.generation // 0')
api GET "/api/v1/consumer-groups/$GROUP" | jq -r '
  "  generation=\(.generation)  members=\(.members|length)",
  (.members[]? | "  member \(.member_id)  assignment=\(.assignment|join(","))")' \
  || warn "could not parse group"
if [ "$GEN_AFTER" != "$GEN_BEFORE" ]; then
  ok "rebalance detected: generation $GEN_BEFORE → $GEN_AFTER"
else
  info "generation unchanged so far ($GEN_AFTER) — rebalance may still be in flight"
fi

echo
info "Lag after rebalance:"
api GET "/api/v1/consumer-groups/$GROUP/offsets" | jq -r '
  (if type=="array" then . else (.items // .data // []) end)[]
  | "  p\(.partition)  committed=\(.committed)  latest=\(.latest)  lag=\(.lag)"' \
  || warn "could not parse offsets"

# ---------------------------------------------------------------------------
# 7. DLQ
# ---------------------------------------------------------------------------
step "7/8  Dead-letter queue (fail:0.15 handler → retries exhausted)"
sleep 2
DLQ=$(api GET "/api/v1/dlq?topic=$TOPIC&limit=10")
DLQ_COUNT=$(echo "$DLQ" | jq -r '(if type=="array" then . else (.items // .data // []) end) | length' 2>/dev/null || echo 0)
if [ "${DLQ_COUNT:-0}" -gt 0 ]; then
  ok "$DLQ_COUNT DLQ entries"
  echo "$DLQ" | jq -r '(if type=="array" then . else (.items // .data // []) end)[]
    | "  \(.id // "?")  p\(.partition)@\(.offset)  key=\(.key // "-")  retries=\(.retry_count // 0)  reason=\(.reason // "-")  status=\(.status)"' \
    | head -10
  FIRST_DLQ=$(echo "$DLQ" | jq -r '(if type=="array" then . else (.items // .data // []) end)[0].id // empty')
  if [ -n "$FIRST_DLQ" ]; then
    echo
    info "Replaying first DLQ entry (POST /api/v1/dlq/$FIRST_DLQ/replay):"
    REPLAY=$(api POST "/api/v1/dlq/$FIRST_DLQ/replay")
    if echo "$REPLAY" | jq -e '.error' >/dev/null 2>&1; then
      warn "replay failed: $(json_get "$REPLAY" '.error.message')"
    else
      ok "replayed"
    fi
  fi
else
  warn "no DLQ entries yet (the 15% failure rate may not have exhausted retries in this window)"
fi

# ---------------------------------------------------------------------------
# 8. benchmark
# ---------------------------------------------------------------------------
step "8/8  Benchmark: $BENCH_EVENTS events, 512B payload, 4 producers, 2 consumers"
info "this runs against the live cluster and takes a bit…"
"$BIN/streamx" bench --events "$BENCH_EVENTS" --payload 512 --producers 4 --consumers 2 \
  || warn "CLI bench failed — check that the cluster is healthy and re-run \`make benchmark\`"

# Also show the latest run from the API registry (if the bench registered it).
sleep 2
LATEST=$(api GET "/api/v1/benchmarks" | jq -r '
  (if type=="array" then . else (.items // .data // []) end) | sort_by(.created_at // 0) | last // empty' 2>/dev/null || echo "")
if [ -n "$LATEST" ] && [ "$LATEST" != "null" ]; then
  RUN_ID=$(echo "$LATEST" | jq -r '.run_id // empty')
  if [ -n "$RUN_ID" ]; then
    echo
    info "Latest registered run ($RUN_ID) from GET /api/v1/benchmarks/$RUN_ID:"
    api GET "/api/v1/benchmarks/$RUN_ID" | jq -r '
      "  status=\(.status)",
      (.result // {} |
       "  throughput=\(.throughput_events_per_sec // 0) ev/s",
       "  producer p50/p95/p99=\(.producer_latency_ms.p50 // 0)/\(.producer_latency_ms.p95 // 0)/\(.producer_latency_ms.p99 // 0) ms",
       "  consumer p50/p95/p99=\(.consumer_latency_ms.p50 // 0)/\(.consumer_latency_ms.p95 // 0)/\(.consumer_latency_ms.p99 // 0) ms",
       "  errors=\(.error_rate // 0)")'
  fi
fi

# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------
step "Demo complete"
info "stopping demo workers…"
for pid in "${WORKER_PIDS[@]}"; do
  kill "$pid" 2>/dev/null || true
done
[ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null || true
sleep 1
ok "done. Dashboard: http://localhost:3000  ·  Grafana: http://localhost:3001"