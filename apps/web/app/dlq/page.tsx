"use client";

import { Eye, RotateCcw, Trash2 } from "lucide-react";
import { useState } from "react";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import type { DLQEntry } from "@/lib/types";
import {
  Button,
  Card,
  ConfirmButton,
  EmptyState,
  ErrorState,
  Field,
  Input,
  KV,
  Loading,
  PageHeader,
  PayloadView,
  StatusBadge,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtDateTime, fmtInt } from "@/lib/format";

function DLQDetail({ entry, onClosed, onChanged }: { entry: DLQEntry; onClosed: () => void; onChanged: () => void }) {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function act(kind: "replay" | "drop") {
    setBusy(kind);
    setError(null);
    try {
      if (kind === "replay") {
        await api.replayDLQ(entry.id);
      } else {
        // NOTE: CONTRACTS.md defines replay but not drop; the DLQ status
        // model (pending → replayed | dropped) implies one. We call the
        // natural endpoint and flag it for the API implementation.
        await api.dropDLQ(entry.id);
      }
      onClosed();
      onChanged();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Action failed");
    } finally {
      setBusy(null);
    }
  }

  return (
    <Card
      title={`DLQ entry ${entry.id}`}
      actions={<Button variant="ghost" onClick={onClosed}>Close</Button>}
    >
      <div className="mb-3 grid grid-cols-2 gap-x-6">
        <KV k="Topic" v={entry.topic} />
        <KV k="Group" v={entry.group} />
        <KV k="Original partition" v={`p${entry.partition}`} />
        <KV k="Original offset" v={fmtInt(entry.offset)} />
        <KV k="Key" v={entry.key ?? "—"} />
        <KV k="Retry count" v={fmtInt(entry.retry_count)} />
        <KV k="Status" v={<StatusBadge status={entry.status} />} />
        <KV k="Received" v={fmtDateTime(entry.ts ?? entry.created_at)} />
      </div>
      {entry.reason && (
        <div className="mb-3 rounded border border-rose-900/50 bg-rose-950/20 px-3 py-2 text-xs text-rose-300">
          <span className="font-semibold">Reason:</span> {entry.reason}
        </div>
      )}
      <div className="mb-3">
        <div className="mb-1 text-xs font-medium text-slate-400">Payload</div>
        <PayloadView b64={entry.payload} maxLines={20} />
      </div>
      {error && <p className="mb-3 text-xs text-rose-400" role="alert">{error}</p>}
      <div className="flex gap-2">
        <Button onClick={() => act("replay")} disabled={busy !== null || entry.status !== "pending"}>
          <RotateCcw className="h-3.5 w-3.5" aria-hidden /> {busy === "replay" ? "Replaying…" : "Replay to original topic"}
        </Button>
        <ConfirmButton
          label={
            <span className="inline-flex items-center gap-1.5">
              <Trash2 className="h-3.5 w-3.5" aria-hidden /> Drop
            </span>
          }
          confirmLabel="Confirm drop?"
          disabled={busy !== null || entry.status !== "pending"}
          onConfirm={() => act("drop")}
        />
      </div>
    </Card>
  );
}

export default function DLQPage() {
  const [topic, setTopic] = useState("");
  const [group, setGroup] = useState("");
  const [selected, setSelected] = useState<DLQEntry | null>(null);

  const dlq = usePoll(
    () => api.listDLQ({ topic: topic || undefined, group: group || undefined, limit: 200 }),
    3000,
    [topic, group],
  );

  return (
    <div>
      <PageHeader
        title="Dead-letter queue"
        subtitle="Events that exhausted their retry budget (default 3) are published to <topic>.dlq and recorded here"
      />

      <Card className="mb-4">
        <div className="flex flex-wrap items-end gap-3">
          <Field label="Filter by topic">
            <Input value={topic} onChange={(e) => setTopic(e.target.value)} placeholder="orders" className="w-48" />
          </Field>
          <Field label="Filter by group">
            <Input value={group} onChange={(e) => setGroup(e.target.value)} placeholder="orders-group" className="w-48" />
          </Field>
          <Button variant="ghost" onClick={dlq.reload}>
            Refresh
          </Button>
        </div>
      </Card>

      {selected && (
        <div className="mb-4">
          <DLQDetail entry={selected} onClosed={() => setSelected(null)} onChanged={dlq.reload} />
        </div>
      )}

      {dlq.loading ? (
        <Loading label="Loading DLQ…" />
      ) : dlq.unreachable ? (
        <UnreachableState />
      ) : dlq.error ? (
        <ErrorState message={dlq.error} onRetry={dlq.reload} />
      ) : (dlq.data ?? []).length === 0 ? (
        <EmptyState
          title="DLQ is empty"
          hint="Failed events land here after max_retries (default 3). Run the demo with a failing handler to see entries."
        />
      ) : (
        <Table headers={["Topic", "Group", "Partition", "Offset", "Key", "Retries", "Reason", "Status", "Received", ""]}>
          {(dlq.data ?? []).map((e) => (
            <Tr key={e.id} onClick={() => setSelected(e)}>
              <Td className="font-mono text-slate-300">{e.topic}</Td>
              <Td className="font-mono text-slate-400">{e.group}</Td>
              <Td className="font-mono tabular-nums">p{e.partition}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(e.offset)}</Td>
              <Td className="font-mono text-slate-300">{e.key ?? "—"}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(e.retry_count)}</Td>
              <Td className="max-w-[220px] truncate text-slate-400" >{e.reason ?? "—"}</Td>
              <Td><StatusBadge status={e.status} /></Td>
              <Td className="font-mono text-slate-400">{fmtDateTime(e.ts ?? e.created_at)}</Td>
              <Td>
                <Eye className="h-3.5 w-3.5 text-slate-500" aria-label={`View ${e.id}`} />
              </Td>
            </Tr>
          ))}
        </Table>
      )}
    </div>
  );
}