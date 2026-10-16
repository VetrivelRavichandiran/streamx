"use client";

import Link from "next/link";
import { AlertTriangle, Inbox, Loader2, RefreshCw, WifiOff } from "lucide-react";
import { ReactNode, useEffect, useState } from "react";

// ---------------------------------------------------------------------------
// Page scaffolding
// ---------------------------------------------------------------------------

export function PageHeader({
  title,
  subtitle,
  actions,
  back,
}: {
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
  back?: { href: string; label: string };
}) {
  return (
    <div className="mb-5 flex flex-wrap items-start justify-between gap-3">
      <div className="min-w-0">
        {back && (
          <Link
            href={back.href}
            className="mb-1 inline-flex items-center gap-1 text-xs text-slate-400 hover:text-slate-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 rounded"
          >
            ← {back.label}
          </Link>
        )}
        <h1 className="text-lg font-semibold tracking-tight text-slate-100">{title}</h1>
        {subtitle && <p className="mt-0.5 text-xs text-slate-400">{subtitle}</p>}
      </div>
      {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Status indicators
// ---------------------------------------------------------------------------

export function StatusDot({ ok, unknown }: { ok: boolean; unknown?: boolean }) {
  const color = unknown ? "bg-slate-500" : ok ? "bg-emerald-500" : "bg-rose-500";
  return (
    <span className="relative inline-flex h-2 w-2" aria-hidden>
      {ok && !unknown && (
        <span className={`absolute inline-flex h-full w-full animate-ping rounded-full ${color} opacity-40`} />
      )}
      <span className={`relative inline-flex h-2 w-2 rounded-full ${color}`} />
    </span>
  );
}

export function Badge({
  tone = "slate",
  children,
  title,
}: {
  tone?: "slate" | "green" | "red" | "amber" | "blue" | "violet";
  children: ReactNode;
  title?: string;
}) {
  const tones: Record<string, string> = {
    slate: "bg-slate-800 text-slate-300 border-slate-700",
    green: "bg-emerald-950 text-emerald-300 border-emerald-800",
    red: "bg-rose-950 text-rose-300 border-rose-800",
    amber: "bg-amber-950 text-amber-300 border-amber-800",
    blue: "bg-sky-950 text-sky-300 border-sky-800",
    violet: "bg-violet-950 text-violet-300 border-violet-800",
  };
  return (
    <span
      title={title}
      className={`inline-flex items-center rounded border px-1.5 py-0.5 text-[11px] font-medium leading-none ${tones[tone]}`}
    >
      {children}
    </span>
  );
}

export function StatusBadge({ status }: { status: string | undefined }) {
  const s = (status ?? "unknown").toLowerCase();
  const tone =
    s === "up" || s === "ok" || s === "live" || s === "completed" || s === "pending"
      ? s === "pending"
        ? "amber"
        : "green"
      : s === "down" || s === "failed" || s === "dead" || s === "dropped"
        ? "red"
        : s === "running" || s === "replayed"
          ? "blue"
          : "slate";
  return <Badge tone={tone}>{s}</Badge>;
}

// ---------------------------------------------------------------------------
// Loading / error / empty states
// ---------------------------------------------------------------------------

export function Loading({ label = "Loading…" }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-2 rounded-lg border border-slate-800 bg-slate-900/40 py-14 text-sm text-slate-400" role="status">
      <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
      {label}
    </div>
  );
}

export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 rounded-lg border border-rose-900/60 bg-rose-950/20 py-12 text-center" role="alert">
      <AlertTriangle className="h-5 w-5 text-rose-400" aria-hidden />
      <div>
        <p className="text-sm font-medium text-rose-300">Something went wrong</p>
        <p className="mt-1 max-w-md text-xs text-rose-400/80">{message}</p>
      </div>
      {onRetry && (
        <button type="button" onClick={onRetry} className="btn btn-ghost">
          <RefreshCw className="h-3.5 w-3.5" aria-hidden /> Retry
        </button>
      )}
    </div>
  );
}

export function UnreachableState({ hint }: { hint?: string }) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 rounded-lg border border-amber-900/60 bg-amber-950/20 py-12 text-center" role="alert">
      <WifiOff className="h-5 w-5 text-amber-400" aria-hidden />
      <div>
        <p className="text-sm font-medium text-amber-300">Cluster unreachable</p>
        <p className="mt-1 max-w-md text-xs text-amber-400/80">
          {hint ?? "The STREAMX API is not responding. Start the cluster with `make dev` and make sure the API is listening on :8080."}
        </p>
      </div>
    </div>
  );
}

export function EmptyState({
  title,
  hint,
  action,
}: {
  title: string;
  hint?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-slate-800 bg-slate-900/30 py-12 text-center">
      <Inbox className="h-5 w-5 text-slate-600" aria-hidden />
      <p className="text-sm font-medium text-slate-300">{title}</p>
      {hint && <p className="max-w-md text-xs text-slate-500">{hint}</p>}
      {action}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Cards / panels
// ---------------------------------------------------------------------------

export function Card({
  title,
  actions,
  children,
  className = "",
  pad = true,
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  pad?: boolean;
}) {
  return (
    <section className={`rounded-lg border border-slate-800 bg-slate-900/50 ${className}`}>
      {(title || actions) && (
        <header className="flex items-center justify-between gap-2 border-b border-slate-800 px-4 py-2.5">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400">{title}</h2>
          {actions}
        </header>
      )}
      <div className={pad ? "p-4" : ""}>{children}</div>
    </section>
  );
}

export function StatCard({
  label,
  value,
  sub,
  tone = "default",
  loading,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  tone?: "default" | "good" | "bad" | "warn";
  loading?: boolean;
}) {
  const toneCls =
    tone === "good"
      ? "text-emerald-300"
      : tone === "bad"
        ? "text-rose-300"
        : tone === "warn"
          ? "text-amber-300"
          : "text-slate-100";
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-4 py-3">
      <div className="text-[11px] font-medium uppercase tracking-wider text-slate-500">{label}</div>
      <div className={`mt-1 font-mono text-xl tabular-nums ${loading ? "text-slate-600" : toneCls}`}>
        {loading ? "…" : value}
      </div>
      {sub && <div className="mt-0.5 text-[11px] text-slate-500">{sub}</div>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

export function Table({
  headers,
  children,
  dense = true,
}: {
  headers: ReactNode[];
  children: ReactNode;
  dense?: boolean;
}) {
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-800">
      <table className={`w-full border-collapse text-left text-xs ${dense ? "" : "text-sm"}`}>
        <thead>
          <tr className="border-b border-slate-800 bg-slate-900/80">
            {headers.map((h, i) => (
              <th key={i} className="whitespace-nowrap px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800/70">{children}</tbody>
      </table>
    </div>
  );
}

export function Td({ children, className = "" }: { children?: ReactNode; className?: string }) {
  return <td className={`px-3 py-2 align-middle ${className}`}>{children}</td>;
}

export function Tr({ children, className = "", onClick }: { children: ReactNode; className?: string; onClick?: () => void }) {
  return (
    <tr
      className={`bg-slate-900/20 transition-colors hover:bg-slate-800/40 ${onClick ? "cursor-pointer" : ""} ${className}`}
      onClick={onClick}
    >
      {children}
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Forms
// ---------------------------------------------------------------------------

export function Field({
  label,
  children,
  hint,
  error,
}: {
  label: string;
  children: ReactNode;
  hint?: string;
  error?: string | null;
}) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs font-medium text-slate-400">{label}</span>
      {children}
      {hint && !error && <span className="mt-1 block text-[11px] text-slate-600">{hint}</span>}
      {error && <span className="mt-1 block text-[11px] text-rose-400">{error}</span>}
    </label>
  );
}

export function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={`input ${props.className ?? ""}`} />;
}

export function Select(props: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return <select {...props} className={`input ${props.className ?? ""}`} />;
}

export function Button({
  children,
  variant = "primary",
  className,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "ghost" | "danger" }) {
  const styles =
    variant === "primary"
      ? "bg-sky-600 text-white hover:bg-sky-500 disabled:bg-slate-700"
      : variant === "danger"
        ? "bg-rose-950 text-rose-300 border border-rose-800 hover:bg-rose-900/60"
        : "bg-slate-800 text-slate-200 border border-slate-700 hover:bg-slate-700";
  return (
    <button
      {...rest}
      className={`inline-flex items-center justify-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-medium transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 disabled:cursor-not-allowed disabled:opacity-60 ${styles} ${className ?? ""}`}
    >
      {children}
    </button>
  );
}

export function ConfirmButton({
  label,
  children,
  confirmLabel = "Confirm?",
  onConfirm,
  variant = "danger",
  disabled,
}: {
  label?: ReactNode;
  children?: ReactNode;
  confirmLabel?: ReactNode;
  onConfirm: () => void;
  variant?: "primary" | "ghost" | "danger";
  disabled?: boolean;
}) {
  const [arming, setArming] = useState(false);
  useEffect(() => {
    if (!arming) return;
    const t = setTimeout(() => setArming(false), 3000);
    return () => clearTimeout(t);
  }, [arming]);
  return (
    <Button
      variant={arming ? "danger" : variant}
      disabled={disabled}
      onClick={() => {
        if (arming) {
          setArming(false);
          onConfirm();
        } else {
          setArming(true);
        }
      }}
    >
      {arming ? confirmLabel : (label ?? children)}
    </Button>
  );
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

export function KV({ k, v }: { k: string; v: ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-4 border-b border-slate-800/60 py-1.5 last:border-0">
      <span className="text-xs text-slate-500">{k}</span>
      <span className="text-right font-mono text-xs text-slate-200">{v}</span>
    </div>
  );
}

export function PayloadView({ b64, maxLines = 12 }: { b64: string | null | undefined; maxLines?: number }) {
  const [expanded, setExpanded] = useState(false);
  if (!b64) return <span className="text-slate-600">—</span>;
  let text: string;
  let binary = false;
  try {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    let printable = 0;
    for (let i = 0; i < bytes.length; i++) {
      const c = bytes[i];
      if (c === 9 || c === 10 || c === 13 || (c >= 32 && c < 127)) printable++;
    }
    if (bytes.length > 0 && printable / bytes.length < 0.9) {
      text = `<binary, ${bytes.length} bytes>`;
      binary = true;
    } else {
      text = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
      try {
        text = JSON.stringify(JSON.parse(text), null, 2);
      } catch {
        /* keep raw */
      }
    }
  } catch {
    text = "<invalid base64>";
    binary = true;
  }
  const lines = text.split("\n");
  const clipped = !expanded && lines.length > maxLines;
  return (
    <div className="min-w-0">
      <pre className={`max-h-64 overflow-auto whitespace-pre-wrap break-all rounded bg-slate-950/80 p-2 font-mono text-[11px] leading-relaxed text-slate-300 ${clipped ? "max-h-40" : ""}`}>
        {clipped ? lines.slice(0, maxLines).join("\n") + "\n…" : text}
      </pre>
      {lines.length > maxLines && (
        <button
          type="button"
          onClick={() => setExpanded((e) => !e)}
          className="mt-1 text-[11px] text-sky-400 hover:text-sky-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 rounded"
        >
          {expanded ? "collapse" : `show all ${lines.length} lines`}
        </button>
      )}
      {binary && <span className="ml-2 text-[10px] text-slate-600">(binary)</span>}
    </div>
  );
}