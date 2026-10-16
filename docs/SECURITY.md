# STREAMX — Security

This document describes the security model of the **control plane** (the API), the **data plane** (broker TCP), and — critically — what is **NOT** implemented.

> **Honest summary:** the control plane has real auth (roles, tokens, rate limiting, audit). The **data plane is plaintext TCP with no authentication** — a deliberate simplification for a local/educational deployment. Do not expose broker ports to an untrusted network.

---

## 1. Authentication (control plane)

- **Login:** `POST /api/v1/auth/login` with `{username, password}` → `{token, role, user}`.
- **Tokens:** 32 random bytes, hex-encoded, stored in the `tokens` table with a **24 h expiry** and a role. Sent as `Authorization: Bearer <token>`.
- **Password hashing:** **bcrypt**.
- **Bootstrap users:** `ADMIN_USER`/`ADMIN_PASSWORD` (default `admin`/`admin123`) and `OPERATOR_USER`/`OPERATOR_PASSWORD` (default `operator`/`operator123`) are created on first run **if absent**. **These are dev credentials — change them (or don't run with defaults) in any real deployment.**
- **Login rate limit:** 5 attempts/min/IP.

**IMPLEMENTED.**

### Roles

| Role | Capabilities |
|---|---|
| `viewer` | Read-only: topics, groups, brokers, DLQ list, benchmarks, live streams |
| `operator` | viewer + create/patch/delete topics, produce events, run benchmarks, replay DLQ |
| `admin` | operator + failure injection, user management, audit log |

Admin-only routes live under `/api/v1/admin/*`.

**IMPLEMENTED.**

---

## 2. Authorization

Every authenticated route checks the caller's role against the route's minimum role. Insufficient role → **403** with the standard error envelope. The dashboard hides admin-only UI (Failures page, user management, audit log) for non-admins, but the **API is the enforcement point** — the UI is not a security boundary.

**IMPLEMENTED.**

---

## 3. Rate limiting

| Scope | Limit | Backend |
|---|---|---|
| API per-token | 500 req/s (token bucket) | Redis when `REDIS_URL` set, in-memory otherwise |
| Login per-IP | 5/min | in-memory |
| Producer per-id (data plane) | 50k ev/s | in-broker token bucket |
| Topic (data plane) | 200k ev/s | in-broker token bucket |

Exceeding a limit → **HTTP 429** (API) or **status 9** with `retry_after_ms` (data plane).

**IMPLEMENTED.**

---

## 4. Secure headers & CORS

API responses set:

- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Cache-Control: no-store` on API routes
- **CORS restricted to the web origin** (the dashboard origin).

In local dev, the Next.js server **rewrites** `/api/*` to the API, so the browser sees a same-origin request and CORS is a non-issue. In a split deployment, set the web origin in the API's CORS config.

**IMPLEMENTED.**

---

## 5. Audit log

Every sensitive action is recorded:

- login (success + failure),
- topic create/patch/delete,
- admin actions (failure injection, user create),
- DLQ replay,
- token mint.

`GET /api/v1/admin/audit?limit=` (admin) returns the tail; the dashboard's Settings page shows it.

**IMPLEMENTED.**

---

## 6. Data plane security — **NOT implemented** (SIMPLIFIED)

The broker TCP protocol (`:9092-9094`) is **plaintext JSON with no authentication, no TLS, and no per-client identity.** Anyone who can reach the port can produce, fetch, and join groups.

**Why:** adding mTLS/SASL to a hand-rolled TCP protocol is a large project, and for a local single-machine cluster it's not the point. The system is designed to run behind a trusted network boundary.

**Consequences & mitigations:**
- **Do not expose broker ports** to untrusted networks. Put the API (which *is* authenticated) in front, and keep brokers on a private network.
- The API's produce/fetch paths go through the authenticated control plane, so *dashboard-driven* access is authorized.
- **FUTURE WORK:** mTLS between brokers and clients, SASL/SCRAM authentication, per-topic ACLs.

---

## 7. Secrets & configuration

- All configuration is via `STREAMX_*` env vars (see [.env.example](../.env.example)).
- `.env` is git-ignored; only `.env.example` is committed.
- **Do not commit real credentials.** The committed defaults (`admin123`, etc.) are for local dev only.

**IMPLEMENTED (convention).**

---

## 8. Threat model (what this system protects against)

| Threat | Protected? | How |
|---|---|---|
| Unauthenticated API access | **yes** | bearer tokens, role checks |
| Privilege escalation via API | **yes** | role-per-route, admin-only `/admin/*` |
| API abuse / DoS via control plane | **partially** | per-token + login rate limits |
| Data-plane abuse (unauthenticated produce/fetch) | **no** | plaintext TCP — **do not expose ports** |
| Credential stuffing on login | **partially** | 5/min/IP + bcrypt |
| Token theft | **mitigated** | 24 h expiry, revocable (`RevokeToken`) |
| Audit tampering | **no** | audit log is append-only in the DB, not signed |

---

## 9. FUTURE WORK (security)

1. **mTLS + SASL/SCRAM** on the data plane.
2. **Per-topic / per-group ACLs.**
3. **OIDC / SSO** and SCIM user provisioning.
4. **Signed, tamper-evident audit log.**
5. **Secrets management** (Vault/KMS) instead of env vars.
6. **Network policies** (Kubernetes NetworkPolicy) as a first-class deployment artifact.