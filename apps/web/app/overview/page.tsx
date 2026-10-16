"use client";

import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useEffect, useMemo, useState } from "react";
import { api } from "@/lib/api";
import { usePoll, useSeries } from "@/lib/hooks";
import {
  Card,
  EmptyState,
  ErrorState,
  Loading,
  PageHeader,
  StatCard,
  StatusBadge,
  StatusDot,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtBytesRate, fmtInt, fmtRate, timeLabel } from "@/lib/format";

export default function OverviewPage() {
  const cluster = usePoll(() => api.cluster(), 5000);
  const metrics = usePoll(() => api.metricsSummary(), 2000);
  const series = useSeries(
    2000,
    60,
    async () => {
      const m = await api.metricsSummary();
      return {
        ts: m.ts ?? Date.now(),
        eps: m.events_per_sec ?? 0,
        bps: m.bytes_per_sec ?? 0,
        err: m.errors_per_sec ?? 0,
      };
    },
  );

  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  const s = cluster.data?.summary;
  const brokers = cluster.data?.brokers ?? [];
  const up = brokers.filter((b) => b.status === "up" || b.live === true).length;

  const chartData = useMemo(
    () =>
      series.map((p) => ({
        t: timeLabel(p.ts),
        "events/s": Math.round(p.eps),
        "errors/s": Math.round(p.err),
      })),
    [series],
  );

  return (
    <div>
      <PageHeader
        title="Cluster overview"
        subtitle={
          <span>
            Live metrics polled every 2 s from <code className="font-mono text-slate-500">/api/v1/metrics/summary</code> · last update{" "}
            {new Date(now).toLocaleTimeString("en-US", { hour12: false })}
          </span>
        }
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        <StatCard
          label="Brokers up"
          value={`${up} / ${s?.brokers_total ?? brokers.length ?? 0}`}
          tone={up === (s?.brokers_total ?? brokers.length) && up > 0 ? "good" : up > 0 ? "warn" : "bad"}
          loading={cluster.loading}
        />
        <StatCard label="Topics" value={fmtInt(s?.topics)} loading={cluster.loading} />
        <StatCard label="Partitions" value={fmtInt(s?.partitions)} loading={cluster.loading} />
        <StatCard label="Consumer groups" value={fmtInt(s?.consumer_groups)} loading={cluster.loading} />
        <StatCard label="Active consumers" value={fmtInt(s?.active_consumers)} loading={cluster.loading} />
        <StatCard
          label="Errors /s"
          value={metrics.data?.errors_per_sec != null ? metrics.data.errors_per_sec.toFixed(2) : "—"}
          tone={(metrics.data?.errors_per_sec ?? 0) > 0 ? "bad" : "good"}
          loading={metrics.loading}
        />
      </div>

      {/* Throughput cards + chart */}
      <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-3">
        <div className="grid grid-cols-2 gap-3 lg:col-span-1">
          <StatCard
            label="Events / s"
            value={metrics.data?.events_per_sec != null ? fmtRate(metrics.data.events_per_sec) : "—"}
            sub="windowed rate"
            loading={metrics.loading}
          />
          <StatCard
            label="Bytes / s"
            value={metrics.data?.bytes_per_sec != null ? fmtBytesRate(metrics.data.bytes_per_sec) : "—"}
            loading={metrics.loading}
          />
        </div>
        <Card title="Live event rate (60 s window)" className="lg:col-span-2">
          {metrics.unreachable ? (
            <UnreachableState />
          ) : chartData.length === 0 ? (
            <EmptyState title="Waiting for metrics…" hint="Points appear every 2 s while the API is reachable." />
          ) : (
            <div className="h-56">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id="eps" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#0ea5e9" stopOpacity={0.5} />
                      <stop offset="100%" stopColor="#0ea5e9" stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" />
                  <XAxis dataKey="t" tickLine={false} axisLine={false} minTickGap={40} />
                  <YAxis tickLine={false} axisLine={false} width={52} allowDecimals={false} />
                  <Tooltip />
                  <Area type="monotone" dataKey="events/s" stroke="#0ea5e9" strokeWidth={1.5} fill="url(#eps)" isAnimationActive={false} />
                  <Area type="monotone" dataKey="errors/s" stroke="#f43f5e" strokeWidth={1} fill="none" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}
        </Card>
      </div>

      {/* Broker health */}
      <div className="mt-4">
        <Card title="Broker health" pad={false}>
          {cluster.loading ? (
            <div className="p-4"><Loading label="Loading cluster…" /></div>
          ) : cluster.unreachable ? (
            <div className="p-4"><UnreachableState /></div>
          ) : cluster.error ? (
            <div className="p-4"><ErrorState message={cluster.error} onRetry={cluster.reload} /></div>
          ) : brokers.length === 0 ? (
            <div className="p-4"><EmptyState title="No brokers registered" hint="Brokers register themselves on startup via the meta store." /></div>
          ) : (
            <Table headers={["Broker", "Status", "CPU", "Memory", "Disk", "Connections", "Events/s", "Bytes/s", "Partitions led"]}>
              {brokers.map((b) => (
                <Tr key={b.id}>
                  <Td className="font-mono text-slate-200">
                    <span className="mr-2 inline-flex align-middle"><StatusDot ok={b.status === "up" || b.live === true} unknown={b.status === undefined && b.live === undefined} /></span>
                    {b.id}
                  </Td>
                  <Td><StatusBadge status={b.status ?? (b.live ? "up" : "down")} /></Td>
                  <Td className="font-mono tabular-nums">{b.cpu_percent != null ? `${b.cpu_percent.toFixed(1)}%` : "—"}</Td>
                  <Td className="font-mono tabular-nums">{b.memory_bytes != null ? `${(b.memory_bytes / 1048576).toFixed(0)} MiB` : "—"}</Td>
                  <Td className="font-mono tabular-nums">{b.disk_used_bytes != null ? `${(b.disk_used_bytes / 1073741824).toFixed(2)} GiB` : "—"}</Td>
                  <Td className="font-mono tabular-nums">{fmtInt(b.connections)}</Td>
                  <Td className="font-mono tabular-nums">{fmtRate(b.events_per_sec)}</Td>
                  <Td className="font-mono tabular-nums">{fmtBytesRate(b.bytes_per_sec)}</Td>
                  <Td className="font-mono tabular-nums">{fmtInt(b.partitions_led)}</Td>
                </Tr>
              ))}
            </Table>
          )}
        </Card>
      </div>
    </div>
  );
}