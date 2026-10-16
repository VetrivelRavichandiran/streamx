"use client";

import { Pause, Play, Radio, Trash2 } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { api, openSSE } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import type { StreamEvent } from "@/lib/types";
import {
  Badge,
  Button,
  Card,
  EmptyState,
  Field,
  Input,
  PageHeader,
  PayloadView,
  Select,
  Table,
  Td,
  Tr,
} from "@/components/ui";
import { fmtInt, fmtTime, truncate } from "@/lib/format";

const MAX_ROWS = 300;

type ConnState = "connecting" | "open" | "closed";

export default function EventsPage() {
  const topics = usePoll(() => api.listTopics(), 5000);
  const [topic, setTopic] = useState<string>("");
  const [partition, setPartition] = useState<string>("");
  const [paused, setPaused] = useState(false);
  const [rows, setRows] = useState<StreamEvent[]>([]);
  const [conn, setConn] = useState<ConnState>("connecting");
  const [connError, setConnError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [received, setReceived] = useState(0);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  // Open SSE when topic/partition selection changes.
  useEffect(() => {
    let es: EventSource | null = null;
    let cancelled = false;
    setConn("connecting");
    setConnError(null);

    const q = new URLSearchParams();
    if (topic) q.set("topic", topic);
    if (partition !== "") q.set("partition", partition);
    const qs = q.toString();

    es = openSSE(`/api/v1/stream/events${qs ? `?${qs}` : ""}`, {
      onEvent: (data) => {
        if (pausedRef.current) return;
        let ev: StreamEvent;
        try {
          const j = JSON.parse(data) as StreamEvent | StreamEvent[];
          ev = Array.isArray(j) ? j[j.length - 1] : j;
        } catch {
          return; // ignore non-JSON keepalives
        }
        if (!ev || typeof ev.offset !== "number") return;
        setReceived((n) => n + 1);
        setRows((r) => {
          const next = [...r, ev];
          return next.length > MAX_ROWS ? next.slice(next.length - MAX_ROWS) : next;
        });
        setConn("open");
      },
      onError: () => {
        if (cancelled) return;
        setConn("closed");
        setConnError("SSE connection lost — EventSource will retry automatically.");
      },
    });

    return () => {
      cancelled = true;
      es?.close();
    };
  }, [topic, partition]);

  const topicNames = useMemo(() => (topics.data ?? []).map((t) => t.name), [topics.data]);

  function clear() {
    setRows([]);
    setReceived(0);
  }

  return (
    <div>
      <PageHeader
        title="Live events"
        subtitle={
          <span>
            <Radio className="mr-1 inline h-3 w-3 text-sky-400" aria-hidden />
            Server-Sent Events from <code className="font-mono text-slate-500">/api/v1/stream/events</code> ·{" "}
            {fmtInt(received)} received this session
          </span>
        }
        actions={
          <>
            <Button variant="ghost" onClick={clear}>
              <Trash2 className="h-3.5 w-3.5" aria-hidden /> Clear
            </Button>
            <Button variant={paused ? "primary" : "ghost"} onClick={() => setPaused((p) => !p)}>
              {paused ? <Play className="h-3.5 w-3.5" aria-hidden /> : <Pause className="h-3.5 w-3.5" aria-hidden />}
              {paused ? "Resume" : "Pause"}
            </Button>
          </>
        }
      />

      {/* Filters + connection status */}
      <Card className="mb-4">
        <div className="flex flex-wrap items-end gap-3">
          <Field label="Topic (all if empty)">
            <Select value={topic} onChange={(e) => { setTopic(e.target.value); setPartition(""); }} className="w-52">
              <option value="">— all topics —</option>
              {topicNames.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </Select>
          </Field>
          <Field label="Partition (all if empty)">
            <Input
              type="number"
              min={0}
              value={partition}
              onChange={(e) => setPartition(e.target.value)}
              placeholder="any"
              className="w-28"
            />
          </Field>
          <div className="ml-auto flex items-center gap-2 text-xs">
            {conn === "open" && <Badge tone="green">streaming</Badge>}
            {conn === "connecting" && <Badge tone="amber">connecting…</Badge>}
            {conn === "closed" && <Badge tone="red">disconnected</Badge>}
            {paused && <Badge tone="slate">paused</Badge>}
          </div>
        </div>
        {connError && <p className="mt-2 text-[11px] text-amber-400">{connError}</p>}
      </Card>

      {rows.length === 0 ? (
        <EmptyState
          title={paused ? "Paused" : "Waiting for events…"}
          hint={
            paused
              ? "The stream is paused. Press Resume to keep receiving events."
              : "Produce events (e.g. `./bin/streamx produce orders --key k1 --payload '{...}'`) and they will appear here in real time."
          }
        />
      ) : (
        <Table headers={["Time", "Topic", "Part", "Offset", "Key", "Payload", "Status"]}>
          {[...rows].reverse().map((ev, i) => {
            const id = `${ev.id ?? ""}-${ev.offset}-${i}`;
            const isNew = Date.now() - (ev.ts ?? 0) < 5000;
            const isOpen = expanded === id;
            return (
              <Tr key={id} className={isNew ? "bg-sky-950/20" : ""}>
                <Td className="font-mono tabular-nums text-slate-400">{fmtTime(ev.ts)}</Td>
                <Td className="font-mono text-slate-300">{ev.topic}</Td>
                <Td className="font-mono tabular-nums">p{ev.partition}</Td>
                <Td className="font-mono tabular-nums text-slate-200">{fmtInt(ev.offset)}</Td>
                <Td className="font-mono text-slate-300">{ev.key ?? <span className="text-slate-600">—</span>}</Td>
                <Td className="max-w-md">
                  {isOpen ? (
                    <PayloadView b64={ev.payload} />
                  ) : (
                    <button
                      type="button"
                      onClick={() => setExpanded(id)}
                      className="block max-w-md truncate font-mono text-[11px] text-slate-400 hover:text-sky-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 rounded text-left"
                      title="Click to expand"
                    >
                      {truncate(atobSafe(ev.payload), 80) || "—"}
                    </button>
                  )}
                </Td>
                <Td>
                  {ev.headers?.type ? <Badge tone="violet">{ev.headers.type}</Badge> : <Badge tone="green">live</Badge>}
                </Td>
              </Tr>
            );
          })}
        </Table>
      )}

      {rows.length > 0 && (
        <p className="mt-2 text-[11px] text-slate-600">
          Showing the most recent {rows.length} events (buffered client-side). Click a payload to expand it.
        </p>
      )}
    </div>
  );
}

/** Decode base64 for the truncated preview; never throws. */
function atobSafe(b64: string | null | undefined): string {
  if (!b64) return "";
  try {
    const bin = atob(b64);
    return bin;
  } catch {
    return `<binary>`;
  }
}