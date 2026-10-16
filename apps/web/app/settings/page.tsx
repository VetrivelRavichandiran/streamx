"use client";

import { UserPlus } from "lucide-react";
import { useState } from "react";
import { api } from "@/lib/api";
import { getStoredUser } from "@/lib/api";
import { usePoll } from "@/lib/hooks";
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
import { fmtDateTime } from "@/lib/format";

function UsersPanel() {
  const users = usePoll(() => api.listUsers(), 5000);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState("viewer");
  const [error, setError] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function create() {
    setBusy(true);
    setError(null);
    setOk(null);
    try {
      await api.createUser({ username: username.trim(), password, role });
      setOk(`User ${username} created`);
      setUsername("");
      setPassword("");
      users.reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create user");
    } finally {
      setBusy(false);
    }
  }

  if (users.error && /403|forbidden|role/i.test(users.error)) {
    return (
      <Card title="Users">
        <p className="text-xs text-amber-300">User management requires the <span className="font-semibold">admin</span> role.</p>
      </Card>
    );
  }

  return (
    <Card title="Users" pad={false}>
      {users.loading ? (
        <div className="p-4"><Loading label="Loading users…" /></div>
      ) : users.unreachable ? (
        <div className="p-4"><UnreachableState /></div>
      ) : (users.data ?? []).length === 0 ? (
        <div className="p-4"><EmptyState title="No users" /></div>
      ) : (
        <Table headers={["Username", "Role", "ID", "Created"]}>
          {(users.data ?? []).map((u) => (
            <Tr key={u.id}>
              <Td className="font-mono text-slate-200">{u.username}</Td>
              <Td><Badge tone={u.role === "admin" ? "red" : u.role === "operator" ? "blue" : "slate"}>{u.role ?? "—"}</Badge></Td>
              <Td className="font-mono text-slate-500">{u.id?.slice(0, 8)}…</Td>
              <Td className="font-mono text-slate-400">{fmtDateTime(u.created_at)}</Td>
            </Tr>
          ))}
        </Table>
      )}
      <div className="border-t border-slate-800 p-4">
        <div className="grid grid-cols-3 gap-3">
          <Field label="New username">
            <Input value={username} onChange={(e) => setUsername(e.target.value)} placeholder="analyst" />
          </Field>
          <Field label="Password">
            <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="min 8 chars" />
          </Field>
          <Field label="Role">
            <Select value={role} onChange={(e) => setRole(e.target.value)}>
              <option value="viewer">viewer</option>
              <option value="operator">operator</option>
              <option value="admin">admin</option>
            </Select>
          </Field>
        </div>
        {error && <p className="mt-2 text-xs text-rose-400" role="alert">{error}</p>}
        {ok && <p className="mt-2 text-xs text-emerald-400">{ok}</p>}
        <Button className="mt-3" onClick={create} disabled={busy || !username || password.length < 8}>
          <UserPlus className="h-3.5 w-3.5" aria-hidden /> {busy ? "Creating…" : "Create user"}
        </Button>
      </div>
    </Card>
  );
}

function AuditPanel() {
  const audit = usePoll(() => api.listAudit(100), 5000);
  if (audit.error && /403|forbidden|role/i.test(audit.error)) {
    return (
      <Card title="Audit log">
        <p className="text-xs text-amber-300">The audit log requires the <span className="font-semibold">admin</span> role.</p>
      </Card>
    );
  }
  return (
    <Card title="Audit log (latest 100)" pad={false}>
      {audit.loading ? (
        <div className="p-4"><Loading label="Loading audit log…" /></div>
      ) : audit.unreachable ? (
        <div className="p-4"><UnreachableState /></div>
      ) : audit.error ? (
        <div className="p-4"><ErrorState message={audit.error} onRetry={audit.reload} /></div>
      ) : (audit.data ?? []).length === 0 ? (
        <div className="p-4"><EmptyState title="No audit entries" hint="Logins, topic CRUD, admin actions, DLQ replays and token mints are recorded." /></div>
      ) : (
        <div className="max-h-96 overflow-y-auto">
          <Table headers={["Time", "User", "Action", "Detail", "IP"]}>
            {(audit.data ?? []).map((a, i) => (
              <Tr key={a.id ?? i}>
                <Td className="font-mono text-slate-400">{fmtDateTime(a.ts)}</Td>
                <Td className="font-mono text-slate-300">{a.user ?? a.username ?? "—"}</Td>
                <Td className="font-mono text-sky-300">{a.action ?? "—"}</Td>
                <Td className="max-w-[320px] truncate text-slate-400">{a.detail ?? "—"}</Td>
                <Td className="font-mono text-slate-500">{a.ip ?? "—"}</Td>
              </Tr>
            ))}
          </Table>
        </div>
      )}
    </Card>
  );
}

export default function SettingsPage() {
  const me = getStoredUser();
  const isAdmin = me?.role === "admin";

  return (
    <div>
      <PageHeader title="Settings" subtitle="Session, rate limits, users and audit" />

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <div className="space-y-4">
          <Card title="Session">
            <KV k="Signed in as" v={me?.user ?? "—"} />
            <KV k="Role" v={<Badge tone={me?.role === "admin" ? "red" : "blue"}>{me?.role ?? "—"}</Badge>} />
            <KV k="Token" v="24 h expiry, bearer (32 random bytes, hex)" />
            <KV k="API base" v={process.env.NEXT_PUBLIC_API_URL || "same-origin (dev rewrite → :8080)"} />
          </Card>

          <Card title="Rate limits">
            <KV k="API per-token" v="500 req/s (token bucket)" />
            <KV k="Backend" v="Redis when REDIS_URL set, in-memory otherwise" />
            <KV k="Producer per-id" v="50,000 ev/s (token bucket)" />
            <KV k="Topic" v="200,000 ev/s (token bucket)" />
            <KV k="Login" v="5/min/IP" />
            <KV k="Exceeding" v="HTTP 429 / status 9 with retry_after_ms" />
          </Card>

          {isAdmin ? <UsersPanel /> : (
            <Card title="Users">
              <p className="text-xs text-slate-500">Visible to admins only.</p>
            </Card>
          )}
        </div>

        <div className="space-y-4">
          {isAdmin ? <AuditPanel /> : (
            <Card title="Audit log">
              <p className="text-xs text-slate-500">Visible to admins only.</p>
            </Card>
          )}

          <Card title="Roles & permissions">
            <Table headers={["Role", "Capabilities"]}>
              <Tr>
                <Td><Badge>viewer</Badge></Td>
                <Td className="text-slate-400">Read-only: topics, groups, brokers, DLQ list, benchmarks, live streams</Td>
              </Tr>
              <Tr>
                <Td><Badge tone="blue">operator</Badge></Td>
                <Td className="text-slate-400">viewer + create/patch/delete topics, produce events, run benchmarks, replay DLQ</Td>
              </Tr>
              <Tr>
                <Td><Badge tone="red">admin</Badge></Td>
                <Td className="text-slate-400">operator + failure injection, user management, audit log</Td>
              </Tr>
            </Table>
          </Card>
        </div>
      </div>
    </div>
  );
}