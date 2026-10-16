"use client";

import { Gauge } from "lucide-react";
import { useRouter } from "next/navigation";
import { FormEvent, useState } from "react";
import { api, setSession } from "@/lib/api";
import { Button, Field, Input } from "@/components/ui";

export default function LoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const res = await api.login(username.trim(), password);
      setSession(res.token, res.user?.username ?? username, res.role);
      router.push("/overview");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
      setBusy(false);
    }
  }

  return (
    <main className="flex min-h-screen items-center justify-center px-4">
      <div className="w-full max-w-sm">
        <div className="mb-6 flex items-center justify-center gap-2.5">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-sky-600/20 ring-1 ring-sky-600/40">
            <Gauge className="h-5 w-5 text-sky-400" aria-hidden />
          </div>
          <div className="leading-tight">
            <div className="text-lg font-bold tracking-wide text-slate-100">STREAMX</div>
            <div className="text-[10px] uppercase tracking-widest text-slate-500">control plane</div>
          </div>
        </div>

        <form
          onSubmit={submit}
          className="rounded-lg border border-slate-800 bg-slate-900/50 p-5"
          aria-label="Sign in"
        >
          <h1 className="mb-4 text-sm font-semibold text-slate-200">Sign in</h1>
          <div className="space-y-3">
            <Field label="Username">
              <Input
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoComplete="username"
                autoFocus
                required
                placeholder="admin"
              />
            </Field>
            <Field label="Password">
              <Input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="current-password"
                required
                placeholder="••••••••"
              />
            </Field>
          </div>
          {error && (
            <p className="mt-3 rounded border border-rose-900/60 bg-rose-950/30 px-3 py-2 text-xs text-rose-300" role="alert">
              {error}
            </p>
          )}
          <Button type="submit" className="mt-4 w-full" disabled={busy}>
            {busy ? "Signing in…" : "Sign in"}
          </Button>
          <div className="mt-4 rounded border border-slate-800 bg-slate-950/60 px-3 py-2 text-[11px] leading-relaxed text-slate-500">
            <span className="font-semibold text-slate-400">Dev credentials</span>
            <br />
            admin / admin123 · operator / operator123
          </div>
        </form>
        <p className="mt-4 text-center text-[11px] text-slate-600">
          Bearer tokens expire after 24 h. Login is rate-limited to 5/min/IP.
        </p>
      </div>
    </main>
  );
}