"use client";

import { useState } from "react";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import type { BrokerDetail, BrokerInfo } from "@/lib/types";
import {
  Badge,
  Card,
  EmptyState,
  ErrorState,
  KV,
  Loading,
  PageHeader,
  StatusBadge,
  StatusDot,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtBytes, fmtBytesRate, fmtInt, fmtRate } from "@/lib/format";

function BrokerDetail({ id }: { id: string }) {
  const detail = usePoll<BrokerDetail>(() => api.getBroker(id), 2000);

  if (detail.loading) return <Loading label={`Loading ${id}…`} />;
  if (detail.unreachable) return <UnreachableState />;
  if (detail.error) return <ErrorState message={detail.error} onRetry={detail.reload} />;
  const b = detail.data;
  if (!b) return <EmptyState title="No data" />;

  return (
    <div className="space-y-4">
      <Card title="Runtime">
        <div className="grid grid-cols-2 gap-x-6">
          <KV k="Status" v={<StatusBadge status={b.status ?? (b.live ? "up" : "down")} />} />
          <KV k="Address" v={b.addr ?? "—"} />
          <KV k="CPU" v={b.cpu_percent != null ? `${b.cpu_percent.toFixed(1)}%` : "—"} />
          <KV k="Memory" v={fmtBytes(b.memory_bytes)} />
          <KV k="Disk used" v={fmtBytes(b.disk_used_bytes)} />
          <KV k="Connections" v={fmtInt(b.connections)} />
          <KV k="Events/s" v={fmtRate(b.events_per_sec)} />
          <KV k="Bytes/s" v={fmtBytesRate(b.bytes_per_sec)} />
          <KV k="Partitions led" v={fmtInt(b.partitions_led)} />
          <KV k="Replica partitions" v={fmtInt(b.partitions_replica)} />
        </div>
      </Card>

      <Card title="Leadership" pad={false}>
        {(b.leadership ?? []).length === 0 ? (
          <p className="p-4 text-xs text-slate-500">No partitions led by this broker.</p>
        ) : (
          <Table headers={["Topic", "Partition", "Role", "LEO", "HW"]}>
            {(b.leadership ?? []).map((l) => (
              <Tr key={`${l.topic}-${l.partition}`}>
                <Td className="font-mono text-slate-300">{l.topic}</Td>
                <Td className="font-mono tabular-nums">p{l.partition}</Td>
                <Td><Badge tone={l.role === "leader" ? "blue" : "slate"}>{l.role}</Badge></Td>
                <Td className="font-mono tabular-nums">{fmtInt(l.leo)}</Td>
                <Td className="font-mono tabular-nums">{fmtInt(l.hw)}</Td>
              </Tr>
            ))}
          </Table>
        )}
      </Card>

      <Card title="Replication" pad={false}>
        {(b.replication ?? []).length === 0 ? (
          <p className="p-4 text-xs text-slate-500">No replication peers reported.</p>
        ) : (
          <Table headers={["Topic", "Partition", "Role", "Peer", "Lag", "In sync"]}>
            {(b.replication ?? []).map((r, i) => (
              <Tr key={i}>
                <Td className="font-mono text-slate-300">{r.topic}</Td>
                <Td className="font-mono tabular-nums">p{r.partition}</Td>
                <Td><Badge tone={r.role === "leader" ? "blue" : "slate"}>{r.role}</Badge></Td>
                <Td className="font-mono text-slate-400">{r.peer ?? "—"}</Td>
                <Td className="font-mono tabular-nums">
                  {r.lag != null && r.lag > 0 ? <span className="text-amber-300">{fmtInt(r.lag)}</span> : <span className="text-slate-500">0</span>}
                </Td>
                <Td>
                  {r.in_sync === undefined ? (
                    <span className="text-slate-600">—</span>
                  ) : r.in_sync ? (
                    <Badge tone="green">in sync</Badge>
                  ) : (
                    <Badge tone="red">lagging</Badge>
                  )}
                </Td>
              </Tr>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}

export default function BrokersPage() {
  const brokers = usePoll(() => api.listBrokers(), 3000);
  const [selected, setSelected] = useState<string | null>(null);

  const list: BrokerInfo[] = brokers.data ?? [];
  const up = list.filter((b) => b.status === "up" || b.live === true).length;

  return (
    <div>
      <PageHeader
        title="Brokers"
        subtitle={`${up}/${list.length} up · stats refreshed every 3 s`}
      />

      {brokers.loading ? (
        <Loading label="Loading brokers…" />
      ) : brokers.unreachable ? (
        <UnreachableState />
      ) : brokers.error ? (
        <ErrorState message={brokers.error} onRetry={brokers.reload} />
      ) : list.length === 0 ? (
        <EmptyState title="No brokers registered" hint="Brokers register on startup. Start the cluster with `make dev`." />
      ) : (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-5">
          <div className="xl:col-span-2">
            <Table headers={["Broker", "Status", "CPU", "Memory", "Disk", "Conns", "Events/s"]}>
              {list.map((b) => (
                <Tr
                  key={b.id}
                  className={selected === b.id ? "bg-sky-950/40" : ""}
                  onClick={() => setSelected(b.id)}
                >
                  <Td className="font-mono text-slate-200">
                    <span className="mr-2 inline-flex align-middle">
                      <StatusDot ok={b.status === "up" || b.live === true} unknown={b.status === undefined && b.live === undefined} />
                    </span>
                    {b.id}
                  </Td>
                  <Td><StatusBadge status={b.status ?? (b.live ? "up" : "down")} /></Td>
                  <Td className="font-mono tabular-nums">{b.cpu_percent != null ? `${b.cpu_percent.toFixed(1)}%` : "—"}</Td>
                  <Td className="font-mono tabular-nums">{fmtBytes(b.memory_bytes)}</Td>
                  <Td className="font-mono tabular-nums">{fmtBytes(b.disk_used_bytes)}</Td>
                  <Td className="font-mono tabular-nums">{fmtInt(b.connections)}</Td>
                  <Td className="font-mono tabular-nums">{fmtRate(b.events_per_sec)}</Td>
                </Tr>
              ))}
            </Table>
            <p className="mt-2 text-[11px] text-slate-600">Select a broker to inspect leadership and replication.</p>
          </div>
          <div className="xl:col-span-3">
            {selected ? (
              <BrokerDetail id={selected} />
            ) : (
              <EmptyState title="No broker selected" hint="Click a broker row on the left." />
            )}
          </div>
        </div>
      )}
    </div>
  );
}