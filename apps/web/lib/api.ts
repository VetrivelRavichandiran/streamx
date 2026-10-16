// Typed client for the STREAMX control-plane REST API (CONTRACTS.md §3).
//
// - Auth: Bearer token stored in localStorage (set by the login page).
// - Base URL: same-origin in dev (next.config.ts rewrites /api → :8080);
//   NEXT_PUBLIC_API_URL when deployed separately.
// - SSE endpoints (EventSource cannot set headers) pass the token as a
//   `token` query parameter; the API should accept it for /api/v1/stream/*.
// - List endpoints: the contract does not fix an envelope, so every list
//   method accepts a bare array OR {items: []} / {data: []}.

import type {
  AuditEntry,
  BenchmarkRun,
  BrokerDetail,
  BrokerInfo,
  ClusterInfo,
  ConsumerGroup,
  DLQEntry,
  FailureEvent,
  GroupDetail,
  GroupOffset,
  LoginResponse,
  MeResponse,
  MetricsSummary,
  PartitionInfo,
  ProduceResult,
  StreamEvent,
  Topic,
  TopicSchema,
  UserRecord,
} from "./types";

export class ApiError extends Error {
  status: number;
  code: string;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

export const TOKEN_KEY = "streamx_token";
export const USER_KEY = "streamx_user";

export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(TOKEN_KEY);
}

export function setSession(token: string, user?: string, role?: string): void {
  window.localStorage.setItem(TOKEN_KEY, token);
  if (user) window.localStorage.setItem(USER_KEY, JSON.stringify({ user, role }));
}

export function clearSession(): void {
  window.localStorage.removeItem(TOKEN_KEY);
  window.localStorage.removeItem(USER_KEY);
}

export function getStoredUser(): { user: string; role: string } | null {
  if (typeof window === "undefined") return null;
  const raw = window.localStorage.getItem(USER_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as { user: string; role: string };
  } catch {
    return null;
  }
}

function baseUrl(): string {
  if (typeof window !== "undefined") {
    const env = (window as unknown as { __env?: { API_URL?: string } }).__env;
    const fromEnv = process.env.NEXT_PUBLIC_API_URL;
    if (fromEnv) return fromEnv;
    if (env?.API_URL) return env.API_URL;
  }
  return ""; // same origin (dev rewrite / prod same-origin proxy)
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  opts: { auth?: boolean } = { auth: true },
): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.auth !== false) {
    const token = getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
  }
  let res: Response;
  try {
    res = await fetch(baseUrl() + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      cache: "no-store",
    });
  } catch {
    throw new ApiError(0, "network", "Cluster unreachable — is the API running on :8080?");
  }
  if (res.status === 401 && opts.auth !== false) {
    clearSession();
    if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
    throw new ApiError(401, "auth", "Session expired — please sign in again.");
  }
  if (!res.ok) {
    let code = "error";
    let message = res.statusText;
    try {
      const j = (await res.json()) as { error?: { code?: string; message?: string } };
      if (j.error) {
        code = j.error.code ?? code;
        message = j.error.message ?? message;
      }
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, code, message);
  }
  if (res.status === 204) return undefined as T;
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("application/json")) return (await res.json()) as T;
  return (await res.text()) as unknown as T;
}

/** Accept a bare array or a {items|data: []} envelope. */
function asList<T>(v: unknown): T[] {
  if (Array.isArray(v)) return v as T[];
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (Array.isArray(o.items)) return o.items as T[];
    if (Array.isArray(o.data)) return o.data as T[];
  }
  return [];
}

export interface CreateTopicInput {
  name: string;
  partitions: number;
  replication_factor: number;
  retention?: { max_age_ms: number; max_bytes: number };
  schema?: TopicSchema;
}

export const api = {
  // ---- auth ----
  login: (username: string, password: string) =>
    request<LoginResponse>("POST", "/api/v1/auth/login", { username, password }, { auth: false }),
  me: () => request<MeResponse>("GET", "/api/v1/auth/me"),

  // ---- topics ----
  listTopics: () => request<unknown>("GET", "/api/v1/topics").then((v) => asList<Topic>(v)),
  getTopic: (name: string) => request<Topic>("GET", `/api/v1/topics/${encodeURIComponent(name)}`),
  createTopic: (input: CreateTopicInput) => request<Topic>("POST", "/api/v1/topics", input),
  patchTopic: (name: string, input: { retention?: { max_age_ms: number; max_bytes: number }; schema?: TopicSchema }) =>
    request<Topic>("PATCH", `/api/v1/topics/${encodeURIComponent(name)}`, input),
  deleteTopic: (name: string) => request<void>("DELETE", `/api/v1/topics/${encodeURIComponent(name)}`),
  topicPartitions: (name: string) =>
    request<unknown>("GET", `/api/v1/topics/${encodeURIComponent(name)}/partitions`).then((v) =>
      asList<PartitionInfo>(v),
    ),
  topicEvents: (name: string, partition: number, offset?: number, max = 100) => {
    const q = new URLSearchParams({ max: String(max) });
    if (offset !== undefined) q.set("offset", String(offset));
    return request<unknown>(
      "GET",
      `/api/v1/topics/${encodeURIComponent(name)}/partitions/${partition}/events?${q.toString()}`,
    ).then((v) => asList<StreamEvent>(v));
  },
  produceEvent: (
    name: string,
    input: { key?: string; payload: string; headers?: Record<string, string>; acks?: string },
  ) => request<ProduceResult>("POST", `/api/v1/topics/${encodeURIComponent(name)}/events`, input),
  produceBatch: (name: string, input: { events: { key?: string; payload: string; headers?: Record<string, string> }[]; acks?: string }) =>
    request<ProduceResult[]>("POST", `/api/v1/topics/${encodeURIComponent(name)}/events/batch`, input),

  // ---- consumer groups ----
  listGroups: () => request<unknown>("GET", "/api/v1/consumer-groups").then((v) => asList<ConsumerGroup>(v)),
  getGroup: (group: string) => request<GroupDetail>("GET", `/api/v1/consumer-groups/${encodeURIComponent(group)}`),
  groupOffsets: (group: string) =>
    request<unknown>("GET", `/api/v1/consumer-groups/${encodeURIComponent(group)}/offsets`).then((v) =>
      asList<GroupOffset>(v),
    ),
  deleteGroup: (group: string) => request<void>("DELETE", `/api/v1/consumer-groups/${encodeURIComponent(group)}`),

  // ---- cluster / brokers ----
  cluster: () => request<ClusterInfo>("GET", "/api/v1/cluster"),
  listBrokers: () => request<unknown>("GET", "/api/v1/brokers").then((v) => asList<BrokerInfo>(v)),
  getBroker: (id: string) => request<BrokerDetail>("GET", `/api/v1/brokers/${encodeURIComponent(id)}`),
  metricsSummary: () => request<MetricsSummary>("GET", "/api/v1/metrics/summary"),

  // ---- DLQ ----
  listDLQ: (params: { topic?: string; group?: string; limit?: number } = {}) => {
    const q = new URLSearchParams();
    if (params.topic) q.set("topic", params.topic);
    if (params.group) q.set("group", params.group);
    if (params.limit) q.set("limit", String(params.limit));
    const qs = q.toString();
    return request<unknown>("GET", `/api/v1/dlq${qs ? `?${qs}` : ""}`).then((v) => asList<DLQEntry>(v));
  },
  getDLQ: (id: string) => request<DLQEntry>("GET", `/api/v1/dlq/${encodeURIComponent(id)}`),
  replayDLQ: (id: string) => request<void>("POST", `/api/v1/dlq/${encodeURIComponent(id)}/replay`),
  // Not in CONTRACTS.md §3 (gap flagged for the API implementation) — the
  // DLQ status model `pending → replayed | dropped` implies a drop action.
  dropDLQ: (id: string) => request<void>("POST", `/api/v1/dlq/${encodeURIComponent(id)}/drop`),

  // ---- benchmarks ----
  createBenchmark: (input: {
    scenario: string;
    events: number;
    payload_bytes: number;
    producers: number;
    consumers: number;
    partitions: number;
    concurrency: number;
  }) => request<{ run_id: string }>("POST", "/api/v1/benchmarks", input),
  listBenchmarks: () => request<unknown>("GET", "/api/v1/benchmarks").then((v) => asList<BenchmarkRun>(v)),
  getBenchmark: (runId: string) => request<BenchmarkRun>("GET", `/api/v1/benchmarks/${encodeURIComponent(runId)}`),
  compareBenchmarks: (ids: string[]) =>
    request<BenchmarkRun[]>("GET", `/api/v1/benchmarks/compare?ids=${ids.map(encodeURIComponent).join(",")}`),

  // ---- admin: failures ----
  failBroker: (broker_id: string, action: "stop" | "restart" | "kill") =>
    request<unknown>("POST", "/api/v1/admin/failures/broker", { broker_id, action }),
  failConsumer: (consumer_id: string, action: "stop" | "restart" | "slow" | "pause") =>
    request<unknown>("POST", "/api/v1/admin/failures/consumer", { consumer_id, action }),
  failNetwork: (target: string, delay_ms: number, duration_ms: number) =>
    request<unknown>("POST", "/api/v1/admin/failures/network", { target, delay_ms, duration_ms }),
  listFailures: () => request<unknown>("GET", "/api/v1/admin/failures").then((v) => asList<FailureEvent>(v)),

  // ---- admin: users / audit ----
  listUsers: () => request<unknown>("GET", "/api/v1/admin/users").then((v) => asList<UserRecord>(v)),
  createUser: (input: { username: string; password: string; role: string }) =>
    request<UserRecord>("POST", "/api/v1/admin/users", input),
  listAudit: (limit = 100) =>
    request<unknown>("GET", `/api/v1/admin/audit?limit=${limit}`).then((v) => asList<AuditEntry>(v)),
};

// ---------------------------------------------------------------------------
// SSE helpers
// ---------------------------------------------------------------------------

export interface SSEHandlers {
  onEvent: (data: string) => void;
  onError?: (err: Event) => void;
}

/**
 * Open an EventSource against a /api/v1/stream/* endpoint.
 * EventSource cannot set Authorization headers, so the token is passed as a
 * `token` query parameter (documented requirement for the Go API).
 */
export function openSSE(path: string, handlers: SSEHandlers): EventSource | null {
  const token = getToken();
  const q = new URLSearchParams();
  if (token) q.set("token", token);
  const qs = q.toString();
  const url = baseUrl() + path + (qs ? `?${qs}` : "");
  const es = new EventSource(url);
  es.onmessage = (e) => handlers.onEvent(e.data);
  es.onerror = (e) => handlers.onError?.(e);
  return es;
}