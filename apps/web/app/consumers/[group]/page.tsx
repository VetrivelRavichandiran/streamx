"use client";

import { Line, LineChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { api } from "@/lib/api";
import { usePoll, useSeries } from "@/lib/hooks";
import type { GroupOffset } from "@/lib/types";
import {
  Badge,
  Card,
  EmptyState,
  ErrorState,
  KV,
  Loading,
  PageHeader,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtInt, timeLabel } from "@/lib/format";

export default function GroupDetailPage() {
  const params = useParams<{ group: string }>();
  const group = decodeURIComponent(params.group);

  const detail = usePoll(() => api.getGroup(group), 3000);
  const offsets = usePoll(() => api.groupOffsets(group), 2000);

  const series = useSeries(
    2000,
    60,
    async () => {
      const list = await api.groupOffsets(group);
      const total = list.reduce((a, o) => a + (o.lag ?? 0), 0);
      return { ts: Date.now(), lag: total };
    },
  );

  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  const chartData = useMemo(() => series.map((p) => ({ t: timeLabel(p.ts), lag: p.lag })), [series]);

  if (detail.loading && offsets.loading) return <Loading label={`Loading ${group}…`} />;
  if (detail.unreachable || offsets.unreachable) return <UnreachableState />;
  if (detail.notFound) {
    return (
      <div>
        <PageHeader title={group} back={{ href: "/consumers", label: "Consumer groups" }} />
        <EmptyState title="Group not found" hint={`No consumer group named “${group}”.`} />
      </div>
    );
  }
  if (detail.error) return <ErrorState message={detail.error} onRetry={detail.reload} />;

  const d = detail.data;
  const offs: GroupOffset[] = offsets.data ?? [];
  const totalLag = offs.reduce((a, o) => a + (o.lag ?? 0), 0);

  return (
    <div>
      <PageHeader
        title={group}
        back={{ href: "/consumers", label: "Consumer groups" }}
        subtitle={
          <span>
            {d?.topic ? `topic ${d.topic} · ` : ""}generation {fmtInt(d?.generation)} · {d?.members?.length ?? 0} members · updated{" "}
            {new Date(now).toLocaleTimeString("en-US", { hour12: false })}
          </span>
        }
      />

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <div className="space-y-4 xl:col-span-2">
          <Card title="Per-partition offsets" pad={false}>
            {offsets.loading ? (
              <div className="p-4"><Loading label="Loading offsets…" /></div>
            ) : offsets.error ? (
              <div className="p-4"><ErrorState message={offsets.error} onRetry={offsets.reload} /></div>
            ) : offs.length === 0 ? (
              <div className="p-4"><EmptyState title="No committed offsets" hint="Offsets appear once the group commits (every 1 s or 100 events)." /></div>
            ) : (
              <Table headers={["Partition", "Committed", "Latest", "Lag"]}>
                {offs.map((o) => (
                  <Tr key={o.partition}>
                    <Td className="font-mono text-slate-200">p{o.partition}</Td>
                    <Td className="font-mono tabular-nums">{fmtInt(o.committed)}</Td>
                    <Td className="font-mono tabular-nums">{fmtInt(o.latest)}</Td>
                    <Td className="font-mono tabular-nums">
                      {o.lag > 0 ? <span className="text-amber-300">{fmtInt(o.lag)}</span> : <span className="text-emerald-400">0</span>}
                    </Td>
                  </Tr>
                ))}
                <Tr className="bg-slate-900/60">
                  <Td className="font-semibold text-slate-300">Total</Td>
                  <Td />
                  <Td />
                  <Td className="font-mono tabular-nums font-semibold text-slate-200">{fmtInt(totalLag)}</Td>
                </Tr>
              </Table>
            )}
          </Card>

          <Card title="Live lag (60 s window)">
            {chartData.length === 0 ? (
              <EmptyState title="Waiting for samples…" hint="Lag samples every 2 s." />
            ) : (
              <div className="h-48">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" />
                    <XAxis dataKey="t" tickLine={false} axisLine={false} minTickGap={40} />
                    <YAxis tickLine={false} axisLine={false} width={48} allowDecimals={false} />
                    <Tooltip />
                    <Line type="monotone" dataKey="lag" stroke="#f59e0b" strokeWidth={1.5} dot={false} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}
          </Card>
        </div>

        <div className="space-y-4">
          <Card title="Group info">
            <KV k="Group" v={group} />
            <KV k="Topic" v={d?.topic ?? "—"} />
            <KV k="Generation" v={fmtInt(d?.generation)} />
            <KV k="State" v={d?.state ?? "—"} />
            <KV k="Total lag" v={fmtInt(totalLag)} />
          </Card>

          <Card title={`Members (${d?.members?.length ?? 0})`}>
            {(d?.members ?? []).length === 0 ? (
              <p className="text-xs text-slate-500">No live members. Workers join via group_join and are expired after 10 s without heartbeat.</p>
            ) : (
              <div className="space-y-2">
                {d?.members.map((m) => (
                  <div key={m.member_id} className="rounded border border-slate-800 bg-slate-950/50 p-2">
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-xs text-slate-200">{m.member_id}</span>
                      <Badge tone="blue">{m.assignment?.length ?? 0} partitions</Badge>
                    </div>
                    {m.assignment && m.assignment.length > 0 && (
                      <div className="mt-1.5 flex flex-wrap gap-1">
                        {m.assignment.map((p) => (
                          <span key={p} className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[10px] text-slate-300">
                            p{p}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}