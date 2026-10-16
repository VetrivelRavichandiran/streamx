"use client";

import {
  Activity,
  BarChart3,
  Boxes,
  Gauge,
  LayoutDashboard,
  LogOut,
  Radio,
  Server,
  Settings,
  ShieldAlert,
  Users,
  Zap,
} from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { ReactNode, useEffect, useState } from "react";
import { api, clearSession, getStoredUser } from "@/lib/api";
import { useAuthed } from "@/lib/hooks";

const NAV = [
  { href: "/overview", label: "Overview", icon: LayoutDashboard, adminOnly: false },
  { href: "/topics", label: "Topics", icon: Boxes, adminOnly: false },
  { href: "/consumers", label: "Consumers", icon: Users, adminOnly: false },
  { href: "/brokers", label: "Brokers", icon: Server, adminOnly: false },
  { href: "/events", label: "Events (live)", icon: Radio, adminOnly: false },
  { href: "/dlq", label: "DLQ", icon: ShieldAlert, adminOnly: false },
  { href: "/benchmarks", label: "Benchmarks", icon: BarChart3, adminOnly: false },
  { href: "/failures", label: "Failures", icon: Zap, adminOnly: true },
  { href: "/settings", label: "Settings", icon: Settings, adminOnly: false },
] as const;

function Sidebar({ role, onLogout }: { role: string | null; onLogout: () => void }) {
  const pathname = usePathname();
  const items = NAV.filter((n) => !n.adminOnly || role === "admin");
  return (
    <aside className="fixed inset-y-0 left-0 z-30 flex w-52 flex-col border-r border-slate-800 bg-slate-950/80 backdrop-blur">
      <div className="flex items-center gap-2 border-b border-slate-800 px-4 py-3.5">
        <div className="flex h-7 w-7 items-center justify-center rounded-md bg-sky-600/20 ring-1 ring-sky-600/40">
          <Gauge className="h-4 w-4 text-sky-400" aria-hidden />
        </div>
        <div className="leading-tight">
          <div className="text-sm font-bold tracking-wide text-slate-100">STREAMX</div>
          <div className="text-[10px] uppercase tracking-widest text-slate-500">control plane</div>
        </div>
      </div>
      <nav className="flex-1 overflow-y-auto px-2 py-3" aria-label="Main navigation">
        <ul className="space-y-0.5">
          {items.map((n) => {
            const active = pathname === n.href || pathname.startsWith(n.href + "/");
            const Icon = n.icon;
            return (
              <li key={n.href}>
                <Link
                  href={n.href}
                  aria-current={active ? "page" : undefined}
                  className={`group flex items-center gap-2.5 rounded-md px-2.5 py-2 text-[13px] font-medium transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 ${
                    active
                      ? "bg-sky-950/60 text-sky-300 ring-1 ring-sky-900"
                      : "text-slate-400 hover:bg-slate-900 hover:text-slate-200"
                  }`}
                >
                  <Icon className={`h-4 w-4 shrink-0 ${active ? "text-sky-400" : "text-slate-500 group-hover:text-slate-400"}`} aria-hidden />
                  {n.label}
                  {n.adminOnly && <span className="ml-auto text-[9px] uppercase tracking-wider text-slate-600">admin</span>}
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>
      <div className="border-t border-slate-800 px-3 py-3">
        <div className="mb-2 flex items-center gap-2 px-1">
          <div className="flex h-6 w-6 items-center justify-center rounded-full bg-slate-800 text-[10px] font-bold text-slate-300">
            {(getStoredUser()?.user ?? "?").slice(0, 1).toUpperCase()}
          </div>
          <div className="min-w-0 leading-tight">
            <div className="truncate text-xs font-medium text-slate-300">{getStoredUser()?.user ?? "—"}</div>
            <div className="text-[10px] uppercase tracking-wider text-slate-600">{role ?? "unknown"}</div>
          </div>
        </div>
        <button
          type="button"
          onClick={onLogout}
          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs text-slate-500 hover:bg-slate-900 hover:text-slate-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
        >
          <LogOut className="h-3.5 w-3.5" aria-hidden /> Sign out
        </button>
      </div>
    </aside>
  );
}

function TopBar() {
  const [clock, setClock] = useState("");
  useEffect(() => {
    const f = () => setClock(new Date().toLocaleTimeString("en-US", { hour12: false }));
    f();
    const t = setInterval(f, 1000);
    return () => clearInterval(t);
  }, []);
  return (
    <header className="sticky top-0 z-20 flex h-11 items-center justify-between border-b border-slate-800 bg-slate-950/70 px-5 backdrop-blur">
      <div className="flex items-center gap-2 text-xs text-slate-500">
        <Activity className="h-3.5 w-3.5" aria-hidden />
        <span className="font-mono tabular-nums">{clock || "—"}</span>
        <span className="text-slate-700">·</span>
        <span className="hidden sm:inline">
          API <span className="font-mono text-slate-400">{process.env.NEXT_PUBLIC_API_URL || "same-origin (:8080)"}</span>
        </span>
      </div>
      <div className="text-[10px] uppercase tracking-widest text-slate-600">v0.1 · educational</div>
    </header>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const authed = useAuthed();
  const router = useRouter();
  const pathname = usePathname();
  const [role, setRole] = useState<string | null>(null);
  const [checking, setChecking] = useState(false);

  // Verify session on mount / when authed flips; refresh role from /auth/me.
  useEffect(() => {
    if (!authed) return;
    let cancelled = false;
    (async () => {
      try {
        const me = await api.me();
        if (!cancelled) setRole(me.role);
      } catch {
        /* token invalid → api.me redirects to /login */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [authed, pathname]);

  // Unauthenticated → redirect to /login (except on the login page itself).
  useEffect(() => {
    if (!authed && pathname !== "/login") router.replace("/login");
  }, [authed, pathname, router]);

  const logout = () => {
    clearSession();
    router.push("/login");
  };

  if (!authed && pathname !== "/login") {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-slate-500">
        Redirecting to sign in…
      </div>
    );
  }

  if (pathname === "/login") {
    return <>{children}</>;
  }

  return (
    <div className="min-h-screen">
      <Sidebar role={role} onLogout={logout} />
      <div className="pl-52">
        <TopBar />
        <main className="mx-auto max-w-[1400px] px-5 py-5">{children}</main>
      </div>
    </div>
  );
}