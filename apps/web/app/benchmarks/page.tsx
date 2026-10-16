"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Play } from "lucide-react";
import { useState } from "react";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import type { BenchmarkRun } from "@/lib/types";
import {
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
  StatusBadge,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtBytes, fmtBytesRate, fmtDuration, fmtInt, fmtMs, fmtPct, fmtRate } from "@/lib/format";

const SCENARIOS = ["throughput", "latency", "mixed", "soak"];

function RunForm({ onCreated }: { onCreated: () => void }) {
  const [scenario, setScenario] = useState("throughput");
  const [events, setEvents] = useState(100000);
  const [payload, setPayload] = useState(512);
  const [producers, setProducers] = useState(4);
  const [consumers, setConsumers] = useState(2);
  const [partitions, setPartitions] = useState(6);
  const [concurrency, setConcurrency] = useState(8);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [created, setCreated] = useState<string | null>(null);

  async function submit() {
    setBusy(true);
    setError(null);
    try {
      const r = await api.createBenchmark({
        scenario,
        events,
        payload_bytes: payload,
        producers,
        consumers,
        partitions,
        concurrency,
      });
      setCreated(r.run_id);
      onCreated();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to start benchmark");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card title="Run benchmark">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Field label="Scenario">
          <Select value={scenario} onChange={(e) => setScenario(e.target.value)}>
            {SCENARIOS.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </Select>
        </Field>
        <Field label="Events">
          <Input type="number" min={100} value={events} onChange={(e) => setEvents(Number(e.target.value))} />
        </Field>
        <Field label="Payload bytes">
          <Input type="number" min={1} value={payload} onChange={(e) => setPayload(Number(e.target.value))} />
        </Field>
        <Field label="Producers">
          <Input type="number" min={1} value={producers} onChange={(e) => setProducers(Number(e.target.value))} />
        </Field>
        <Field label="Consumers">
          <Input type="number" min={0} value={consumers} onChange={(e) => setConsumers(Number(e.target.value))} />
        </Field>
        <Field label="Partitions">
          <Input type="number" min={1} max={64} value={partitions} onChange={(e) => setPartitions(Number(e.target.value))} />
        </Field>
        <Field label="Concurrency">
          <Input type="number" min={1} value={concurrency} onChange={(e) => setConcurrency(Number(e.target.value))} />
        </Field>
        <div className="flex items-end">
          <Button onClick={submit} disabled={busy || events < 100}>
            <Play className="h-3.5 w-3.5" aria-hidden /> {busy ? "Starting…" : "Start run"}
          </Button>
        </div>
      </div>
      {error && <p className="mt-3 text-xs text-rose-400" role="alert">{error}</p>}
      {created && <p className="mt-3 text-xs text-emerald-400">Started run <span className="font-mono">{created}</span></p>}
    </Card>
  );
}

function LatencyBars({ run, label }: { run: BenchmarkRun; label: string }) {
  const p = run.result?.producer_latency_ms;
  const c = run.result?.consumer_latency_ms;
  const data = [
    { pct: "p50", producer: p?.p50 ?? 0, consumer: c?.p50 ?? 0 },
    { pct: "p95", producer: p?.p95 ?? 0, consumer: c?.p95 ?? 0 },
    { pct: "p99", producer: p?.p99 ?? 0, consumer: c?.p99 ?? 0 },
  ];
  return (
    <Card title={`${label} — latency percentiles (ms)`}>
      <div className="h-52">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid vertical={false} strokeDasharray="3 3" />
            <XAxis dataKey="pct" tickLine={false} axisLine={false} />
            <YAxis tickLine={false} axisLine={false} width={48} />
            <Tooltip formatter={(v) => fmtMs(Number(v))} />
            <Legend />
            <Bar dataKey="producer" name="producer" fill="#0ea5e9" isAnimationActive={false} />
            <Bar dataKey="consumer" name="consumer" fill="#8b5cf6" isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

function RunDetail({ run, onBack }: { run: BenchmarkRun; onBack: () => void }) {
  const r = run.result;
  return (
    <div>
      <PageHeader
        title={`Run ${run.run_id}`}
        back={{ href: "/benchmarks", label: "Benchmarks" }}
        subtitle={
          <span>
            {run.config.scenario} · {fmtInt(run.config.events)} events · {fmtBytes(run.config.payload_bytes)} payload ·{" "}
            {run.config.producers}P/{run.config.consumers}C · {run.config.partitions} partitions
          </span>
        }
        actions={<StatusBadge status={run.status} />}
      />
      {run.status === "failed" && run.error && (
        <div className="mb-4 rounded border border-rose-900/60 bg-rose-950/20 px-4 py-3 text-xs text-rose-300" role="alert">
          {run.error}
        </div>
      )}
      {!r ? (
        <EmptyState
          title={run.status === "running" || run.status === "pending" ? "Run in progress…" : "No result yet"}
          hint="This page refreshes automatically; results appear when the run completes."
        />
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-6">
            <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
              <div className="text-[11px] uppercase tracking-wider text-slate-500">Throughput</div>
              <div className="mt-1 font-mono text-xl text-sky-300 tabular-nums">{fmtRate(r.throughput_events_per_sec)}</div>
            </div>
            <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
              <div className="text-[11px] uppercase tracking-wider text-slate-500">Bytes/s</div>
              <div className="mt-1 font-mono text-xl text-slate-100 tabular-nums">{fmtBytesRate(r.throughput_bytes_per_sec)}</div>
            </div>
            <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
              <div className="text-[11px] uppercase tracking-wider text-slate-500">Error rate</div>
              <div className={`mt-1 font-mono text-xl tabular-nums ${(r.error_rate ?? 0) > 0 ? "text-rose-300" : "text-emerald-300"}`}>
                {fmtPct((r.error_rate ?? 0) * 100)}
              </div>
            </div>
            <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
              <div className="text-[11px] uppercase tracking-wider text-slate-500">Duration</div>
              <div className="mt-1 font-mono text-xl text-slate-100 tabular-nums">{fmtDuration(r.duration_ms)}</div>
            </div>
            <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
              <div className="text-[11px] uppercase tracking-wider text-slate-500">CPU</div>
              <div className="mt-1 font-mono text-xl text-slate-100 tabular-nums">{fmtPct(r.cpu_percent)}</div>
            </div>
            <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
              <div className="text-[11px] uppercase tracking-wider text-slate-500">Memory</div>
              <div className="mt-1 font-mono text-xl text-slate-100 tabular-nums">{fmtBytes(r.memory_bytes)}</div>
            </div>
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
            <LatencyBars run={run} label="Latency" />
            <Card title="Resource usage">
              <KV k="Produced" v={fmtInt(r.produced)} />
              <KV k="Consumed" v={fmtInt(r.consumed)} />
              <KV k="Errors" v={fmtInt(r.errors)} />
              <KV k="CPU" v={fmtPct(r.cpu_percent)} />
              <KV k="Peak memory" v={fmtBytes(r.memory_bytes)} />
              <KV k="Disk write" v={fmtBytesRate(r.disk_bytes_per_sec)} />
              <KV k="Producer p50/p95/p99" v={`${fmtMs(run.result?.producer_latency_ms?.p50)} / ${fmtMs(run.result?.producer_latency_ms?.p95)} / ${fmtMs(run.result?.producer_latency_ms?.p99)}`} />
              <KV k="Consumer p50/p95/p99" v={`${fmtMs(run.result?.consumer_latency_ms?.p50)} / ${fmtMs(run.result?.consumer_latency_ms?.p95)} / ${fmtMs(run.result?.consumer_latency_ms?.p99)}`} />
            </Card>
          </div>
          <div className="mt-4">
            <Button variant="ghost" onClick={onBack}>← Back to runs</Button>
          </div>
        </>
      )}
    </div>
  );
}

function Compare({ a, b, onBack }: { a: BenchmarkRun; b: BenchmarkRun; onBack: () => void }) {
  const ra = a.result;
  const rb = b.result;
  const data = [
    {
      metric: "Throughput (k ev/s)",
      a: (ra?.throughput_events_per_sec ?? 0) / 1000,
      b: (rb?.throughput_events_per_sec ?? 0) / 1000,
    },
    { metric: "p50 producer (ms)", a: ra?.producer_latency_ms?.p50 ?? 0, b: rb?.producer_latency_ms?.p50 ?? 0 },
    { metric: "p95 producer (ms)", a: ra?.producer_latency_ms?.p95 ?? 0, b: rb?.producer_latency_ms?.p95 ?? 0 },
    { metric: "p99 producer (ms)", a: ra?.producer_latency_ms?.p99 ?? 0, b: rb?.producer_latency_ms?.p99 ?? 0 },
    { metric: "p95 consumer (ms)", a: ra?.consumer_latency_ms?.p95 ?? 0, b: rb?.consumer_latency_ms?.p95 ?? 0 },
    { metric: "Error rate (%)", a: (ra?.error_rate ?? 0) * 100, b: (rb?.error_rate ?? 0) * 100 },
  ];
  return (
    <div>
      <PageHeader
        title="Compare runs"
        back={{ href: "/benchmarks", label: "Benchmarks" }}
        subtitle={<span><span className="font-mono text-sky-300">{a.run_id}</span> vs <span className="font-mono text-violet-300">{b.run_id}</span></span>}
      />
      <Card title="Side-by-side (log-friendly scale)">
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 4, right: 8, left: 8, bottom: 0 }}>
              <CartesianGrid vertical={false} strokeDasharray="3 3" />
              <XAxis dataKey="metric" tickLine={false} axisLine={false} interval={0} angle={-12} textAnchor="end" height={48} />
              <YAxis tickLine={false} axisLine={false} width={48} />
              <Tooltip />
              <Legend />
              <Bar dataKey="a" name={`A: ${a.config.scenario}`} fill="#0ea5e9" isAnimationActive={false} />
              <Bar dataKey="b" name={`B: ${b.config.scenario}`} fill="#8b5cf6" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Card>
      <div className="mt-4">
        <Button variant="ghost" onClick={onBack}>← Back to runs</Button>
      </div>
    </div>
  );
}

export default function BenchmarksPage() {
  const runs = usePoll(() => api.listBenchmarks(), 3000);
  const [viewing, setViewing] = useState<string | null>(null);
  const [compareA, setCompareA] = useState<string | null>(null);
  const [compareB, setCompareB] = useState<string | null>(null);

  const list: BenchmarkRun[] = runs.data ?? [];
  const viewingRun = viewing ? list.find((r) => r.run_id === viewing) : null;
  const cmpA = compareA ? list.find((r) => r.run_id === compareA) : null;
  const cmpB = compareB ? list.find((r) => r.run_id === compareB) : null;

  if (viewingRun) {
    return <RunDetail run={viewingRun} onBack={() => setViewing(null)} />;
  }
  if (cmpA && cmpB) {
    return <Compare a={cmpA} b={cmpB} onBack={() => { setCompareA(null); setCompareB(null); }} />;
  }

  return (
    <div>
      <PageHeader
        title="Benchmarks"
        subtitle="Runs are executed by the bench binary against the live cluster · results from /api/v1/benchmarks/{id}"
      />

      <div className="mb-4">
        <RunForm onCreated={runs.reload} />
      </div>

      {runs.loading ? (
        <Loading label="Loading benchmark runs…" />
      ) : runs.unreachable ? (
        <UnreachableState />
      ) : runs.error ? (
        <ErrorState message={runs.error} onRetry={runs.reload} />
      ) : list.length === 0 ? (
        <EmptyState
          title="No benchmark runs yet"
          hint="Start a run above, or from the CLI: `./bin/streamx bench --events 100000 --payload 512 --producers 4 --consumers 2`."
        />
      ) : (
        <Table headers={["Run ID", "Status", "Scenario", "Events", "Payload", "P/C", "Partitions", "Throughput", "p99 prod", "Errors", "Started"]}>
          {list.map((r) => (
            <Tr key={r.run_id} onClick={() => setViewing(r.run_id)}>
              <Td className="font-mono text-sky-300">{r.run_id.slice(0, 12)}…</Td>
              <Td><StatusBadge status={r.status} /></Td>
              <Td>{r.config?.scenario ?? "—"}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(r.config?.events)}</Td>
              <Td className="font-mono tabular-nums">{fmtBytes(r.config?.payload_bytes)}</Td>
              <Td className="font-mono tabular-nums">{r.config?.producers}/{r.config?.consumers}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(r.config?.partitions)}</Td>
              <Td className="font-mono tabular-nums text-sky-300">{r.result ? fmtRate(r.result.throughput_events_per_sec) : "—"}</Td>
              <Td className="font-mono tabular-nums">{r.result ? fmtMs(r.result.producer_latency_ms?.p99) : "—"}</Td>
              <Td className="font-mono tabular-nums">{r.result ? fmtPct((r.result.error_rate ?? 0) * 100) : "—"}</Td>
              <Td className="font-mono text-slate-400">{r.created_at ? new Date(r.created_at).toLocaleString("en-US", { hour12: false }) : "—"}</Td>
            </Tr>
          ))}
        </Table>
      )}

      {list.length >= 2 && (
        <Card className="mt-4" title="Compare two runs">
          <div className="flex flex-wrap items-end gap-3">
            <Field label="Run A">
              <Select value={compareA ?? ""} onChange={(e) => setCompareA(e.target.value || null)} className="w-56">
                <option value="">— select —</option>
                {list.map((r) => (
                  <option key={r.run_id} value={r.run_id}>{r.run_id.slice(0, 12)}… ({r.config?.scenario})</option>
                ))}
              </Select>
            </Field>
            <Field label="Run B">
              <Select value={compareB ?? ""} onChange={(e) => setCompareB(e.target.value || null)} className="w-56">
                <option value="">— select —</option>
                {list.map((r) => (
                  <option key={r.run_id} value={r.run_id}>{r.run_id.slice(0, 12)}… ({r.config?.scenario})</option>
                ))}
              </Select>
            </Field>
            <Button disabled={!compareA || !compareB || compareA === compareB}>Compare</Button>
          </div>
        </Card>
      )}
    </div>
  );
}