// Typed interfaces for the STREAMX control-plane API.
// Field names mirror docs/CONTRACTS.md §3 exactly (snake_case).
// Where the contract leaves a list-envelope open (array vs {items: []}),
// the client in lib/api.ts accepts both.

export type Role = "admin" | "operator" | "viewer";

export interface AuthUser {
  id: string;
  username: string;
  org_id?: string;
}

export interface LoginResponse {
  token: string;
  role: Role;
  user: AuthUser;
}

export interface MeResponse {
  user: AuthUser;
  role: Role;
}

export interface Retention {
  max_age_ms: number;
  max_bytes: number;
}

export interface SchemaField {
  name: string;
  type: string;
  required: boolean;
}

export interface TopicSchema {
  fields: SchemaField[];
}

export interface TopicStats {
  events_total?: number;
  bytes_total?: number;
  events_per_sec?: number;
  bytes_per_sec?: number;
}

export interface Topic {
  name: string;
  partitions: number;
  replication_factor: number;
  retention?: Retention;
  schema?: TopicSchema;
  created_at?: number;
  stats?: TopicStats;
}

export interface PartitionInfo {
  partition: number;
  leader: string;
  replicas: string[];
  high_watermark: number;
  log_end_offset: number;
  log_start_offset: number;
  rate_per_sec?: number;
  consumer_lag?: number;
}

export interface StreamEvent {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  key?: string | null;
  payload: string; // base64
  headers?: Record<string, string>;
  ts: number;
  producer_id?: string;
  seq?: number;
}

export interface ProduceResult {
  offset: number;
  partition: number;
  ts: number;
  error?: string | null;
}

export interface ConsumerMember {
  member_id: string;
  assignment: number[];
  joined_at?: number;
  client_id?: string;
  host?: string;
}

export interface ConsumerGroup {
  group: string;
  topic?: string;
  generation?: number;
  member_count?: number;
  members?: ConsumerMember[];
  lag?: number;
  state?: string;
}

export interface GroupDetail {
  group: string;
  topic?: string;
  generation?: number;
  state?: string;
  members: ConsumerMember[];
}

export interface GroupOffset {
  partition: number;
  committed: number;
  latest: number;
  lag: number;
}

export type BrokerStatus = "up" | "down" | "unknown";

export interface BrokerInfo {
  id: string;
  addr?: string;
  status?: BrokerStatus;
  live?: boolean;
  cpu_percent?: number;
  memory_bytes?: number;
  disk_used_bytes?: number;
  connections?: number;
  events_per_sec?: number;
  bytes_per_sec?: number;
  partitions_led?: number;
  partitions_replica?: number;
  version?: string;
  started_at?: number;
}

export interface PartitionLeadership {
  topic: string;
  partition: number;
  role: "leader" | "follower";
  leo?: number;
  hw?: number;
}

export interface ReplicationStatus {
  topic: string;
  partition: number;
  role: "leader" | "follower";
  peer?: string;
  lag?: number;
  in_sync?: boolean;
}

export interface BrokerDetail extends BrokerInfo {
  leadership?: PartitionLeadership[];
  replication?: ReplicationStatus[];
}

export interface ClusterSummary {
  brokers_total?: number;
  brokers_up?: number;
  topics?: number;
  partitions?: number;
  consumer_groups?: number;
  active_consumers?: number;
  events_per_sec?: number;
  bytes_per_sec?: number;
  errors_per_sec?: number;
}

export interface ClusterInfo {
  brokers: BrokerInfo[];
  summary?: ClusterSummary;
  topology?: Record<string, string[]>;
}

export interface MetricsSummary {
  events_per_sec?: number;
  bytes_per_sec?: number;
  errors_per_sec?: number;
  window_ms?: number;
  ts?: number;
  [key: string]: number | undefined;
}

export type DLQStatus = "pending" | "replayed" | "dropped";

export interface DLQEntry {
  id: string;
  topic: string;
  group: string;
  partition: number;
  offset: number;
  key?: string | null;
  payload?: string;
  retry_count?: number;
  reason?: string;
  status: DLQStatus | string;
  ts?: number;
  created_at?: number;
  original_topic?: string;
  original_partition?: number;
  original_offset?: number;
  headers?: Record<string, string>;
}

export interface BenchmarkConfig {
  scenario: string;
  events: number;
  payload_bytes: number;
  producers: number;
  consumers: number;
  partitions: number;
  concurrency: number;
}

export interface BenchmarkLatency {
  p50?: number;
  p95?: number;
  p99?: number;
  avg?: number;
  max?: number;
}

export interface BenchmarkResult {
  throughput_events_per_sec?: number;
  throughput_bytes_per_sec?: number;
  producer_latency_ms?: BenchmarkLatency;
  consumer_latency_ms?: BenchmarkLatency;
  error_rate?: number;
  errors?: number;
  produced?: number;
  consumed?: number;
  duration_ms?: number;
  cpu_percent?: number;
  memory_bytes?: number;
  disk_bytes_per_sec?: number;
}

export type BenchmarkStatus = "pending" | "running" | "completed" | "failed";

export interface BenchmarkRun {
  run_id: string;
  status: BenchmarkStatus | string;
  config: BenchmarkConfig;
  created_at?: number;
  started_at?: number;
  finished_at?: number;
  result?: BenchmarkResult;
  error?: string;
}

export interface FailureEvent {
  id?: string;
  ts?: number;
  created_at?: number;
  kind?: string; // broker | consumer | network
  target?: string;
  action?: string;
  detail?: string;
  status?: string;
  broker_id?: string;
  consumer_id?: string;
  delay_ms?: number;
  duration_ms?: number;
}

export interface UserRecord {
  id: string;
  username: string;
  org_id?: string;
  role?: Role;
  created_at?: number;
}

export interface AuditEntry {
  id?: string;
  ts?: number;
  user?: string;
  username?: string;
  action?: string;
  detail?: string;
  ip?: string;
}