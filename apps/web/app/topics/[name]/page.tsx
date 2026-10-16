"use client";

import { Send } from "lucide-react";
import { useParams } from "next/navigation";
import { useState } from "react";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import type { PartitionInfo } from "@/lib/types";
import {
  Badge,
  Button,
  Card,
  EmptyState,
  ErrorState,
  Field,
  Input,
  KV,
  Loading,
  PageHeader,
  Select,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtBytes, fmtInt, fmtRate, fmtTime } from "@/lib/format";

const BROKER_COLORS = [
  "bg-sky-500/20 text-sky-300 border-sky-800",
  "bg-emerald-500/20 text-emerald-300 border-emerald-800",
  "bg-violet-500/20 text-violet-300 border-violet-800",
  "bg-amber-500/20 text-amber-300 border-amber-800",
  "bg-rose-500/20 text-rose-300 border-rose-800",
];

function brokerColor(brokers: string[], id: string): string {
  const i = brokers.indexOf(id);
  return BROKER_COLORS[(i >= 0 ? i : 0) % BROKER_COLORS.length];
}

function PartitionColumn({ p, brokers }: { p: PartitionInfo; brokers: string[] }) {
  const maxOffset = Math.max(p.log_end_offset, 1);
  const leoPct = Math.min(100, (p.log_end_offset / maxOffset) * 100);
  const hwPct = Math.min(100, (p.high_watermark / maxOffset) * 100);
  const lag = p.consumer_lag ?? 0;
  return (
    <div className="flex min-w-[120px] flex-1 flex-col rounded-md border border-slate-800 bg-slate-950/50 p-2.5">
      <div className="mb-2 flex items-center justify-between">
        <span className="font-mono text-xs font-semibold text-slate-200">p{p.partition}</span>
        <span className="font-mono text-[10px] text-slate-500">{fmtRate(p.rate_per_sec)}</span>
      </div>
      {/* offset bar */}
      <div className="relative h-2 overflow-hidden rounded bg-slate-800" aria-hidden>
        <div className="absolute inset-y-0 left-0 bg-sky-600/50" style={{ width: `${leoPct}%` }} />
        <div className="absolute inset-y-0 w-0.5 bg-slate-100" style={{ left: `${hwPct}%` }} title="high watermark" />
      </div>
      <div className="mt-1 flex justify-between font-mono text-[10px] text-slate-500">
        <span>L{p.log_start_offset}</span>
        <span>HW {p.high_watermark}</span>
        <span>LEO {p.log_end_offset}</span>
      </div>
      {/* replicas */}
      <div className="mt-2 space-y-1">
        {p.replicas.map((r) => (
          <div key={r} className={`flex items-center justify-between rounded border px-1.5 py-0.5 text-[10px] font-medium ${brokerColor(brokers, r)}`}>
            <span className="font-mono">{r}</span>
            {r === p.leader ? <span className="uppercase tracking-wider">leader</span> : <span className="opacity-70">follower</span>}
          </div>
        ))}
      </div>
      {lag > 0 && (
        <div className="mt-2 rounded bg-amber-950/50 px-1.5 py-0.5 text-center font-mono text-[10px] text-amber-300">
          lag {fmtInt(lag)}
        </div>
      )}
    </div>
  );
}

function ProduceForm({ topic }: { topic: string }) {
  const [key, setKey] = useState("");
  const [payload, setPayload] = useState('{"demo": true}');
  const [acks, setAcks] = useState("leader");
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit() {
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      const r = await api.produceEvent(topic, {
        key: key || undefined,
        payload,
        acks,
        headers: { type: "dashboard.test" },
      });
      setResult(`offset ${r.offset} · partition ${r.partition} · ${fmtTime(r.ts)}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Produce failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-[1fr_1fr_140px_auto] md:items-end">
      <Field label="Key (optional)">
        <Input value={key} onChange={(e) => setKey(e.target.value)} placeholder="cust-123" />
      </Field>
      <Field label="Payload (JSON or text)">
        <Input value={payload} onChange={(e) => setPayload(e.target.value)} />
      </Field>
      <Field label="acks">
        <Select value={acks} onChange={(e) => setAcks(e.target.value)}>
          <option value="none">none</option>
          <option value="leader">leader</option>
          <option value="all">all</option>
        </Select>
      </Field>
      <Button onClick={submit} disabled={busy || !payload}>
        <Send className="h-3.5 w-3.5" aria-hidden /> {busy ? "Producing…" : "Produce"}
      </Button>
      {(result || error) && (
        <p className={`col-span-full text-xs ${error ? "text-rose-400" : "text-emerald-400"}`}>
          {error ?? `Produced: ${result}`}
        </p>
      )}
    </div>
  );
}

export default function TopicDetailPage() {
  const params = useParams<{ name: string }>();
  const name = decodeURIComponent(params.name);

  const topic = usePoll(() => api.getTopic(name), 3000);
  const partitions = usePoll(() => api.topicPartitions(name), 2000);
  const brokers = usePoll(() => api.listBrokers(), 10000);

  const allBrokers = (brokers.data ?? []).map((b) => b.id);

  if (topic.loading && partitions.loading) return <Loading label={`Loading ${name}…`} />;
  if (topic.unreachable || partitions.unreachable) return <UnreachableState />;
  if (topic.notFound) {
    return (
      <div>
        <PageHeader title={name} back={{ href: "/topics", label: "Topics" }} />
        <EmptyState title="Topic not found" hint={`No topic named “${name}” exists.`} />
      </div>
    );
  }
  if (topic.error) return <ErrorState message={topic.error} onRetry={topic.reload} />;

  const t = topic.data;
  const parts = partitions.data ?? [];

  return (
    <div>
      <PageHeader
        title={name}
        back={{ href: "/topics", label: "Topics" }}
        subtitle={
          <span>
            {t?.partitions} partitions · RF {t?.replication_factor} ·{" "}
            {t?.stats?.events_per_sec != null ? `${fmtRate(t.stats.events_per_sec)} · ` : ""}
            {fmtInt(t?.stats?.events_total)} events total
          </span>
        }
        actions={
          <Button variant="ghost" onClick={() => { topic.reload(); partitions.reload(); }}>
            Refresh
          </Button>
        }
      />

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <div className="space-y-4 xl:col-span-2">
          {/* Partition visualization */}
          <Card
            title="Partitions"
            actions={
              <span className="text-[10px] text-slate-500">
                bar: LSO → LEO · white tick: HW · green: leader
              </span>
            }
          >
            {partitions.loading ? (
              <Loading label="Loading partitions…" />
            ) : partitions.unreachable ? (
              <UnreachableState />
            ) : partitions.error ? (
              <ErrorState message={partitions.error} onRetry={partitions.reload} />
            ) : parts.length === 0 ? (
              <EmptyState title="No partitions" />
            ) : (
              <div className="flex gap-2 overflow-x-auto pb-1">
                {parts.map((p) => (
                  <PartitionColumn key={p.partition} p={p} brokers={allBrokers} />
                ))}
              </div>
            )}
          </Card>

          {/* Partition table */}
          <Card title="Partition details" pad={false}>
            {parts.length === 0 ? (
              <div className="p-4"><EmptyState title="No partition data" /></div>
            ) : (
              <Table headers={["Partition", "Leader", "Replicas", "LEO", "HW", "Log start", "Rate", "Consumer lag"]}>
                {parts.map((p) => (
                  <Tr key={p.partition}>
                    <Td className="font-mono text-slate-200">p{p.partition}</Td>
                    <Td><Badge tone="blue">{p.leader}</Badge></Td>
                    <Td className="font-mono text-slate-400">{p.replicas.join(", ")}</Td>
                    <Td className="font-mono tabular-nums">{fmtInt(p.log_end_offset)}</Td>
                    <Td className="font-mono tabular-nums">{fmtInt(p.high_watermark)}</Td>
                    <Td className="font-mono tabular-nums">{fmtInt(p.log_start_offset)}</Td>
                    <Td className="font-mono tabular-nums">{fmtRate(p.rate_per_sec)}</Td>
                    <Td className="font-mono tabular-nums">
                      {p.consumer_lag != null && p.consumer_lag > 0 ? (
                        <span className="text-amber-300">{fmtInt(p.consumer_lag)}</span>
                      ) : (
                        <span className="text-slate-500">0</span>
                      )}
                    </Td>
                  </Tr>
                ))}
              </Table>
            )}
          </Card>
        </div>

        <div className="space-y-4">
          <Card title="Configuration">
            <KV k="Partitions" v={fmtInt(t?.partitions)} />
            <KV k="Replication factor" v={fmtInt(t?.replication_factor)} />
            <KV
              k="Retention max age"
              v={t?.retention?.max_age_ms ? `${(t.retention.max_age_ms / 86400000).toFixed(1)} days` : "—"}
            />
            <KV k="Retention max bytes" v={fmtBytes(t?.retention?.max_bytes)} />
            <KV k="Created" v={t?.created_at ? fmtTime(t.created_at) : "—"} />
          </Card>

          <Card title="Schema">
            {t?.schema?.fields?.length ? (
              <div className="space-y-1">
                {t.schema.fields.map((f) => (
                  <div key={f.name} className="flex items-center justify-between rounded border border-slate-800 bg-slate-950/50 px-2 py-1">
                    <span className="font-mono text-xs text-slate-200">
                      {f.name}
                      {f.required && <span className="ml-1 text-rose-400">*</span>}
                    </span>
                    <Badge tone="violet">{f.type}</Badge>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-xs text-slate-500">No schema registered. Events are accepted as opaque payloads.</p>
            )}
          </Card>

          <Card title="Produce test event">
            <ProduceForm topic={name} />
          </Card>
        </div>
      </div>
    </div>
  );
}