"use client";

import { Skull, Square, Timer, Zap } from "lucide-react";
import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
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
  Select,
  StatusBadge,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtDateTime, fmtInt } from "@/lib/format";

export default function FailuresPage() {
  const brokers = usePoll(() => api.listBrokers(), 2000);
  const groups = usePoll(() => api.listGroups(), 5000);
  const failures = usePoll(() => api.listFailures(), 3000);

  const [brokerId, setBrokerId] = useState("");
  const [consumerId, setConsumerId] = useState("");
  const [target, setTarget] = useState("broker-1");
  const [delayMs, setDelayMs] = useState(500);
  const [durationMs, setDurationMs] = useState(10000);
  const [busy, setBusy] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [forbidden, setForbidden] = useState(false);

  // Detect admin-only 403s.
  useEffect(() => {
    if (failures.error && /403|forbidden|role/i.test(failures.error)) setForbidden(true);
  }, [failures.error]);

  async function run(kind: string, fn: () => Promise<unknown>) {
    setBusy(kind);
    setFeedback(null);
    try {
      await fn();
      setFeedback(`✓ ${kind} accepted`);
      failures.reload();
      setTimeout(() => brokers.reload(), 500);
    } catch (e) {
      setFeedback(`✗ ${e instanceof Error ? e.message : "failed"}`);
    } finally {
      setBusy(null);
    }
  }

  const brokerList = brokers.data ?? [];
  const groupList = groups.data ?? [];

  return (
    <div>
      <PageHeader
        title="Failure injection"
        subtitle="Admin-only chaos controls — every action is audited. Watch brokers flip status and groups rebalance in real time."
        actions={<Badge tone="red">admin</Badge>}
      />

      {forbidden && (
        <div className="mb-4 rounded border border-amber-900/60 bg-amber-950/20 px-4 py-3 text-xs text-amber-300" role="alert">
          You need the <span className="font-semibold">admin</span> role to use failure injection. Sign in as admin.
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Card title="Broker failures">
          <div className="space-y-3">
            <Field label="Broker">
              <Select value={brokerId} onChange={(e) => setBrokerId(e.target.value)}>
                <option value="">— select —</option>
                {brokerList.map((b) => (
                  <option key={b.id} value={b.id}>
                    {b.id} ({b.status ?? (b.live ? "up" : "down")})
                  </option>
                ))}
              </Select>
            </Field>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="ghost"
                disabled={!brokerId || busy !== null}
                onClick={() => run(`stop ${brokerId}`, () => api.failBroker(brokerId, "stop"))}
              >
                <Square className="h-3.5 w-3.5" aria-hidden /> Stop
              </Button>
              <Button
                variant="ghost"
                disabled={!brokerId || busy !== null}
                onClick={() => run(`restart ${brokerId}`, () => api.failBroker(brokerId, "restart"))}
              >
                Restart
              </Button>
              <ConfirmButton
                label={<span className="inline-flex items-center gap-1.5"><Skull className="h-3.5 w-3.5" aria-hidden /> Kill</span>}
                confirmLabel="SIGKILL?"
                disabled={!brokerId || busy !== null}
                onConfirm={() => run(`kill ${brokerId}`, () => api.failBroker(brokerId, "kill"))}
              />
            </div>
            <p className="text-[11px] leading-relaxed text-slate-600">
              <span className="text-slate-400">Stop</span> = graceful shutdown · <span className="text-slate-400">Restart</span> = stop + start ·{" "}
              <span className="text-slate-400">Kill</span> = SIGKILL (no clean shutdown — tests crash recovery).
            </p>
          </div>
        </Card>

        <Card title="Consumer failures">
          <div className="space-y-3">
            <Field label="Consumer (member id)">
              <Select value={consumerId} onChange={(e) => setConsumerId(e.target.value)}>
                <option value="">— select —</option>
                {groupList.flatMap((g) =>
                  (g.members ?? []).map((m) => (
                    <option key={`${g.group}:${m.member_id}`} value={m.member_id}>
                      {m.member_id} ({g.group})
                    </option>
                  )),
                )}
              </Select>
            </Field>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="ghost"
                disabled={!consumerId || busy !== null}
                onClick={() => run(`stop ${consumerId}`, () => api.failConsumer(consumerId, "stop"))}
              >
                <Square className="h-3.5 w-3.5" aria-hidden /> Stop
              </Button>
              <Button
                variant="ghost"
                disabled={!consumerId || busy !== null}
                onClick={() => run(`restart ${consumerId}`, () => api.failConsumer(consumerId, "restart"))}
              >
                Restart
              </Button>
              <Button
                variant="ghost"
                disabled={!consumerId || busy !== null}
                onClick={() => run(`slow ${consumerId}`, () => api.failConsumer(consumerId, "slow"))}
              >
                <Timer className="h-3.5 w-3.5" aria-hidden /> Slow
              </Button>
              <Button
                variant="ghost"
                disabled={!consumerId || busy !== null}
                onClick={() => run(`pause ${consumerId}`, () => api.failConsumer(consumerId, "pause"))}
              >
                Pause
              </Button>
            </div>
            <p className="text-[11px] leading-relaxed text-slate-600">
              <span className="text-slate-400">Slow</span> adds handler latency to build lag · <span className="text-slate-400">Pause</span> stops
              processing (lag grows) · stopping a member triggers a group rebalance.
            </p>
          </div>
        </Card>

        <Card title="Network faults">
          <div className="space-y-3">
            <Field label="Target (broker id or host:port)">
              <Input value={target} onChange={(e) => setTarget(e.target.value)} placeholder="broker-2" />
            </Field>
            <div className="grid grid-cols-2 gap-3">
              <Field label="Delay (ms)">
                <Input type="number" min={0} value={delayMs} onChange={(e) => setDelayMs(Number(e.target.value))} />
              </Field>
              <Field label="Duration (ms)">
                <Input type="number" min={0} value={durationMs} onChange={(e) => setDurationMs(Number(e.target.value))} />
              </Field>
            </div>
            <Button
              disabled={!target || busy !== null}
              onClick={() => run(`delay ${target}`, () => api.failNetwork(target, delayMs, durationMs))}
            >
              <Zap className="h-3.5 w-3.5" aria-hidden /> Inject delay
            </Button>
          </div>
        </Card>
      </div>

      {feedback && (
        <p className={`mt-3 text-xs ${feedback.startsWith("✓") ? "text-emerald-400" : "text-rose-400"}`} role="status">
          {feedback}
        </p>
      )}

      <div className="mt-4">
        <Card title="Failure history" pad={false}>
          {failures.loading ? (
            <div className="p-4"><Loading label="Loading failure history…" /></div>
          ) : failures.unreachable ? (
            <div className="p-4"><UnreachableState /></div>
          ) : failures.error ? (
            <div className="p-4"><ErrorState message={failures.error} onRetry={failures.reload} /></div>
          ) : (failures.data ?? []).length === 0 ? (
            <div className="p-4"><EmptyState title="No failures injected yet" hint="Injected failures and their recovery appear here with timestamps." /></div>
          ) : (
            <Table headers={["Time", "Kind", "Target", "Action", "Detail", "Status"]}>
              {(failures.data ?? []).map((f, i) => (
                <Tr key={f.id ?? i}>
                  <Td className="font-mono text-slate-400">{fmtDateTime(f.ts ?? f.created_at)}</Td>
                  <Td><Badge tone={f.kind === "broker" ? "red" : f.kind === "consumer" ? "amber" : "blue"}>{f.kind ?? "—"}</Badge></Td>
                  <Td className="font-mono text-slate-300">{f.target ?? f.broker_id ?? f.consumer_id ?? "—"}</Td>
                  <Td className="font-mono text-slate-200">{f.action ?? "—"}</Td>
                  <Td className="max-w-[280px] truncate text-slate-400">
                    {f.detail ?? (f.delay_ms != null ? `delay ${fmtInt(f.delay_ms)}ms for ${fmtInt(f.duration_ms)}ms` : "")}
                  </Td>
                  <Td><StatusBadge status={f.status} /></Td>
                </Tr>
              ))}
            </Table>
          )}
        </Card>
      </div>

      <p className="mt-3 text-[11px] text-slate-600">
        Recovery to watch: broker status flips to <span className="font-mono">down</span> within ~1.5 s (3 missed heartbeats), partition
        leadership moves to the next lowest live broker id, and consumer groups rebalance (generation bumps).
      </p>
    </div>
  );
}