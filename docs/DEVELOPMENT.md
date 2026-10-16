# STREAMX — Development

How to build, run, test, and extend STREAMX locally.

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Go | 1.25+ | The toolchain manager fetches a newer version if `go.mod` requires one (`GOTOOLCHAIN=auto`) |
| Node | 20+ | For the dashboard |
| npm | 10+ | Package manager (not pnpm) |
| Docker | any recent | Optional — for the containerized stack |
| `jq` | any | For the demo script |
| Postgres | 16 | Optional — `make dev` uses `tools/pg` if present, else SQLite |

---

## 2. Make targets

| Target | What it does |
|---|---|
| `make setup` | `go mod download` + `npm install` (in `apps/web`) |
| `make dev` | Start the full local stack in the foreground: postgres (via `tools/pg/pg-server.mjs` if present, else SQLite), broker-1..3, api, web. Ctrl-C stops all. |
| `make build` | Compile all five Go binaries into `./bin` |
| `make test` | `go test -race ./...` (unit + in-package tests) |
| `make integration-test` | `go test -race -tags integration ./tests/...` (real broker processes) |
| `make lint` | `go vet ./...` + `golangci-lint run ./...` (if installed) |
| `make security` | `gosec` + `govulncheck` (if installed) + `npm audit` |
| `make benchmark` | `make build` + `./bin/streamx bench --events 100000 --payload 512 --producers 4 --consumers 2` |
| `make docker-up` | `docker compose up -d --build` (full stack) |
| `make docker-down` | `docker compose down` |
| `make clean` | Remove `bin/`, `data/`, `apps/web/.next`, `apps/web/node_modules`, and compose volumes |
| `make demo` | Run `scripts/demo.sh` (needs a running cluster) |

---

## 3. Local dev (no Docker)

```bash
make setup
make build
make dev
```

`make dev` does the following:

1. **Database.** If `tools/pg/pg-server.mjs` exists, start the embedded Postgres on `:5432` (user/pass/db `streamx`). Otherwise fall back to SQLite at `data/streamx.db`.
2. **Brokers.** Start `broker-1` (`:9092`/`:9102`), `broker-2` (`:9093`/`:9103`), `broker-3` (`:9094`/`:9104`), each with `STREAMX_CLUSTER` set to all three, data under `data/broker-N`.
3. **API.** Start on `:8080` with `STREAMX_DB_URL` pointing at the chosen database.
4. **Web.** Start the Next.js dev server on `:3000`.

Everything runs in the foreground under one `wait`; **Ctrl-C stops the whole stack.**

### Environment

Copy `.env.example` to `.env` and adjust. All variables are `STREAMX_*`-prefixed (plus `NEXT_PUBLIC_API_URL` for the web). See [.env.example](../.env.example) for the full annotated list.

### Ports

| Port | Service |
|---|---|
| 9092-9094 | brokers (data plane) |
| 9102-9104 | broker metrics/health |
| 8080 | API (REST + SSE) |
| 3000 | web dashboard |
| 5432 | postgres (if used) |
| 6379 | redis (optional) |
| 9090 | prometheus (if running) |
| 3001 | grafana (if running) |

Full port plan in [CONTRACTS.md §17](CONTRACTS.md#17-ports-plan-local-dev-single-machine).

---

## 4. Docker

```bash
make docker-up
```

Builds the broker/api/worker/bench/cli images and the web image, then starts: postgres, redis, 3 brokers, api, web, prometheus, grafana. Volumes persist broker data, postgres data, prometheus TSDB, and grafana state.

```bash
make docker-down        # stop + remove (volumes kept)
make clean              # also removes volumes
```

### Dockerfiles

One per binary, in `deployments/`:

| File | Binary | Healthcheck |
|---|---|---|
| `Dockerfile.broker` | `streamx-broker` | probes `STREAMX_METRICS_ADDR/healthz` |
| `Dockerfile.api` | `streamx-api` | probes `STREAMX_API_ADDR/healthz` |
| `Dockerfile.worker` | `streamx-worker` | none (not an HTTP server) |
| `Dockerfile.bench` | `streamx-bench` | none (one-shot job) |
| `Dockerfile.cli` | `streamx` | none (one-shot tool) |

All are **multi-stage** `golang:1.25` builds producing static binaries on `distroless/static-debian12:nonroot`. The broker and API images embed a tiny Go HTTP probe (`deployments/healthcheck/hc.go`) because distroless has no `curl`/`wget`/shell.

> **Recommendation for the repo:** add a `.dockerignore` (the build context is the repo root). A minimal one should exclude `.git`, `data`, `bin`, `node_modules`, `apps/web/.next`, `docs`, `tests`, `.env*`, etc. This keeps builds fast and avoids leaking local data into images.

---

## 5. Testing

```bash
make test               # unit + in-package, with -race
make integration-test   # real broker processes on ephemeral ports
```

- **Unit** (`go test -race ./...`): storage, partition, groups, cluster, protocol, auth, rate limiter, schema validator.
- **Integration** (`-tags integration`): spins up real broker processes; produce/consume/ordering/restart/kill.
- **Chaos** (`tests/chaos`): see [FAILURE_TESTING.md](FAILURE_TESTING.md).
- **E2E** (`tests/e2e`): full stack via the API.

All Go tests run with `-race`.

### Frontend

```bash
cd apps/web
npm install
npm run dev      # dev server on :3000
npm run build    # production build (must pass with zero errors)
```

The dashboard is a client-side app: it fetches the API directly (with a Bearer token) and uses SSE for live streams. There is no server-side data fetching, so the build is straightforward.

---

## 6. CI

`.github/workflows/ci.yml` runs on push/PR:

1. **Go job** — `go vet`, `gofmt -l` check, `golangci-lint`, `go test -race ./...`, `go test -race -tags integration ./tests/...`.
2. **Docker smoke job** — builds the `api` and `broker` images, then boots the `api` image with a SQLite DB and asserts `GET /health` returns 200.
3. **Web job** — `npm ci` + `npm run build` in `apps/web`.

---

## 7. Code layout & conventions

```
cmd/<binary>/main.go      thin entrypoints — parse flags/env, wire deps, run
internal/<pkg>            shared internals (not importable outside the module)
sdk/streamx               the Go client SDK (importable; depends only on internal/protocol)
apps/web                  Next.js dashboard
```

Conventions:

- **No global state.** All state lives in structs; all goroutines must exit on context cancel.
- **Dependencies flow inward** per the module boundary map in [CONTRACTS.md §15](CONTRACTS.md#15-module-boundaries-go).
- **Errors** carry a status code (0-18) on the wire; the API maps them to HTTP.
- **Metrics** are registered in `internal/metrics` and exposed on the metrics server.
- **Config** is parsed once in `internal/config` from env; no flags after startup.

---

## 8. Adding a feature (workflow)

1. **Change the contract first** if it touches a cross-module interface — update [CONTRACTS.md](CONTRACTS.md), then the code.
2. **Implement** in the appropriate `internal/` package.
3. **Add tests** — unit for the logic, integration if it crosses a process boundary.
4. **Add metrics** if it's observable behavior.
5. **Document** — update the relevant doc and mark the feature **IMPLEMENTED** in the README status table.
6. **Run** `make test && make integration-test && make lint` before committing.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Dashboard says "cluster unreachable" | API not running on :8080 | `make dev` or `make docker-up`; check `curl localhost:8080/health` |
| Login fails | API down, or wrong creds | Default is `admin`/`admin123`; check `STREAMX_ADMIN_PASSWORD` |
| Brokers not joining cluster | `STREAMX_CLUSTER` mismatch | All three brokers must have the *same* `CLUSTER` value |
| No events in the live stream | No producer, or wrong topic filter | Produce events; clear the topic filter |
| `make dev` falls back to SQLite | `tools/pg/pg-server.mjs` missing | Expected — SQLite is a first-class dev fallback |
| Port already in use | A previous instance is running | `make docker-down` / kill the old process; check the port plan |