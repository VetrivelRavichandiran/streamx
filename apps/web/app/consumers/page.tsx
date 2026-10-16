"use client";

import Link from "next/link";
import { api } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
import {
  Badge,
  EmptyState,
  ErrorState,
  Loading,
  PageHeader,
  Table,
  Td,
  Tr,
  UnreachableState,
} from "@/components/ui";
import { fmtInt } from "@/lib/format";

export default function ConsumersPage() {
  const groups = usePoll(() => api.listGroups(), 3000);

  return (
    <div>
      <PageHeader
        title="Consumer groups"
        subtitle={`${groups.data?.length ?? 0} groups · membership refreshed every 3 s`}
      />

      {groups.loading ? (
        <Loading label="Loading consumer groups…" />
      ) : groups.unreachable ? (
        <UnreachableState />
      ) : groups.error ? (
        <ErrorState message={groups.error} onRetry={groups.reload} />
      ) : (groups.data ?? []).length === 0 ? (
        <EmptyState
          title="No consumer groups"
          hint="Start a worker to create a group, e.g. `WORKER_GROUP=orders-group WORKER_TOPIC=orders ./bin/streamx-worker`."
        />
      ) : (
        <Table headers={["Group", "Topic", "Generation", "Members", "State", "Lag"]}>
          {(groups.data ?? []).map((g) => (
            <Tr key={g.group}>
              <Td>
                <Link
                  href={`/consumers/${encodeURIComponent(g.group)}`}
                  className="font-mono text-slate-100 hover:text-sky-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 rounded"
                >
                  {g.group}
                </Link>
              </Td>
              <Td className="font-mono text-slate-400">{g.topic ?? "—"}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(g.generation)}</Td>
              <Td className="font-mono tabular-nums">{fmtInt(g.member_count ?? g.members?.length)}</Td>
              <Td>{g.state ? <Badge tone={g.state === "stable" ? "green" : "amber"}>{g.state}</Badge> : <span className="text-slate-600">—</span>}</Td>
              <Td className="font-mono tabular-nums">
                {g.lag != null && g.lag > 0 ? <span className="text-amber-300">{fmtInt(g.lag)}</span> : <span className="text-slate-500">0</span>}
              </Td>
            </Tr>
          ))}
        </Table>
      )}
    </div>
  );
}