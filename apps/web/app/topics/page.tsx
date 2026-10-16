"use client";

import { Plus, Trash2 } from "lucide-react";
import Link from "next/link";
import { useState } from "react";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import type { Topic } from "@/lib/types";
import { validateTopicForm } from "@/lib/validate";
import {
  Badge,
  Button,
  Card,
  ConfirmButton,
  EmptyState,
  ErrorState,
  Field,
  Input,
  Loading,
  PageHeader,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtBytes, fmtInt, fmtRate } from "@/lib/format";

function CreateTopicForm({ onCreated, busy }: { onCreated: () => void; busy: boolean }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState(3);
  const [rf, setRf] = useState(3);
  const [maxAgeMs, setMaxAgeMs] = useState(604800000); // 7d
  const [maxBytes, setMaxBytes] = useState(1073741824); // 1 GiB
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const validation = validateTopicForm(name, partitions, rf);

  async function submit() {
    if (validation) {
      setError(validation);
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await api.createTopic({
        name: name.trim(),
        partitions,
        replication_factor: rf,
        retention: { max_age_ms: maxAgeMs, max_bytes: maxBytes },
      });
      setName("");
      setOpen(false);
      onCreated();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create topic");
    } finally {
      setSubmitting(false);
    }
  }

  if (!open) {
    return (
      <Button onClick={() => setOpen(true)} disabled={busy}>
        <Plus className="h-3.5 w-3.5" aria-hidden /> Create topic
      </Button>
    );
  }

  return (
    <Card title="Create topic" className="mb-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Field label="Name">
          <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="orders" autoFocus />
        </Field>
        <Field label="Partitions (1-64)">
          <Input type="number" min={1} max={64} value={partitions} onChange={(e) => setPartitions(Number(e.target.value))} />
        </Field>
        <Field label="Replication (1-3)">
          <Input type="number" min={1} max={3} value={rf} onChange={(e) => setRf(Number(e.target.value))} />
        </Field>
        <Field label="Retention max age (ms)">
          <Input type="number" min={0} value={maxAgeMs} onChange={(e) => setMaxAgeMs(Number(e.target.value))} />
        </Field>
        <Field label="Retention max bytes">
          <Input type="number" min={0} value={maxBytes} onChange={(e) => setMaxBytes(Number(e.target.value))} />
        </Field>
      </div>
      {error && <p className="mt-3 text-xs text-rose-400" role="alert">{error}</p>}
      <div className="mt-3 flex gap-2">
        <Button onClick={submit} disabled={submitting || !!validation}>
          {submitting ? "Creating…" : "Create"}
        </Button>
        <Button variant="ghost" onClick={() => setOpen(false)}>Cancel</Button>
      </div>
    </Card>
  );
}

export default function TopicsPage() {
  const topics = usePoll(() => api.listTopics(), 3000);
  const [deleting, setDeleting] = useState<string | null>(null);

  async function del(t: Topic) {
    setDeleting(t.name);
    try {
      await api.deleteTopic(t.name);
      topics.reload();
    } finally {
      setDeleting(null);
    }
  }

  return (
    <div>
      <PageHeader
        title="Topics"
        subtitle={`${topics.data?.length ?? 0} topics · stats refreshed every 3 s`}
        actions={<CreateTopicForm onCreated={topics.reload} busy={topics.loading} />}
      />

      {topics.loading ? (
        <Loading label="Loading topics…" />
      ) : topics.unreachable ? (
        <UnreachableState />
      ) : topics.error ? (
        <ErrorState message={topics.error} onRetry={topics.reload} />
      ) : (topics.data ?? []).length === 0 ? (
        <EmptyState
          title="No topics yet"
          hint="Create a topic to start streaming events, or run `make demo` which creates the `orders` topic."
        />
      ) : (
        <Table
          headers={["Topic", "Partitions", "RF", "Events total", "Bytes", "Events/s", "Bytes/s", "Retention", "Schema", ""]}
        >
          {(topics.data ?? []).map((t) => (
            <Tr key={t.name}>
              <Td>
                <Link
                  href={`/topics/${encodeURIComponent(t.name)}`}
                  className="font-mono text-slate-100 hover:text-sky-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 rounded"
                >
                  {t.name}
                </Link>
              </Td>
              <Td className="font-mono tabular-nums">{t.partitions}</Td>
              <Td className="font-mono tabular-nums">{t.replication_factor}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(t.stats?.events_total)}</Td>
              <Td className="font-mono tabular-nums">{fmtBytes(t.stats?.bytes_total)}</Td>
              <Td className="font-mono tabular-nums">{fmtRate(t.stats?.events_per_sec)}</Td>
              <Td className="font-mono tabular-nums">{t.stats?.bytes_per_sec != null ? `${fmtBytes(t.stats.bytes_per_sec)}/s` : "—"}</Td>
              <Td className="text-slate-400">
                {t.retention
                  ? `${t.retention.max_age_ms > 0 ? `${(t.retention.max_age_ms / 86400000).toFixed(1)}d` : "∞"} / ${fmtBytes(t.retention.max_bytes)}`
                  : "—"}
              </Td>
              <Td>{t.schema?.fields?.length ? <Badge tone="violet">{t.schema.fields.length} fields</Badge> : <span className="text-slate-600">none</span>}</Td>
              <Td>
                <ConfirmButton
                  confirmLabel="Delete?"
                  variant="ghost"
                  disabled={deleting === t.name}
                  onConfirm={() => del(t)}
                >
                  <Trash2 className="h-3.5 w-3.5 text-slate-500" aria-label={`Delete ${t.name}`} />
                </ConfirmButton>
              </Td>
            </Tr>
          ))}
        </Table>
      )}
    </div>
  );
}