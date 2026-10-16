// Formatting helpers. All display values in the UI pass through these so
// numbers are rendered consistently (monospace, SI units, ms epoch aware).

export function fmtInt(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return Math.trunc(n).toLocaleString("en-US");
}

export function fmtNum(n: number | null | undefined, digits = 1): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  if (Math.abs(n) >= 1000) return fmtInt(n);
  return n.toLocaleString("en-US", { maximumFractionDigits: digits });
}

const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB"];

export function fmtBytes(n: number | null | undefined, digits = 1): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  if (n < 0) return "—";
  let v = n;
  let u = 0;
  while (v >= 1024 && u < BYTE_UNITS.length - 1) {
    v /= 1024;
    u++;
  }
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(digits)} ${BYTE_UNITS[u]}`;
}

export function fmtRate(n: number | null | undefined, unit = "/s"): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  if (Math.abs(n) >= 1000) return `${fmtInt(n)}${unit}`;
  return `${n.toFixed(1)}${unit}`;
}

export function fmtBytesRate(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return `${fmtBytes(n)}/s`;
}

export function fmtPct(n: number | null | undefined, digits = 0): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return `${n.toFixed(digits)}%`;
}

export function fmtMs(n: number | null | undefined, digits = 1): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  if (n < 1) return `${(n * 1000).toFixed(0)}µs`;
  return `${n.toFixed(digits)}ms`;
}

/** ts is ms epoch (contract: broker assign time, ms). */
export function fmtTime(ts: number | null | undefined): string {
  if (!ts) return "—";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleTimeString("en-US", { hour12: false });
}

export function fmtDateTime(ts: number | null | undefined): string {
  if (!ts) return "—";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("en-US", { hour12: false });
}

export function fmtAgo(ts: number | null | undefined): string {
  if (!ts) return "—";
  const s = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

export function fmtDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  const rs = Math.round(s % 60);
  return `${m}m ${rs}s`;
}

/** Decode a base64 payload for display. Returns text or a <binary> marker. */
export function decodePayload(b64: string | null | undefined): { text: string; binary: boolean } {
  if (!b64) return { text: "", binary: false };
  try {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    // Heuristic: printable ASCII ratio.
    let printable = 0;
    for (let i = 0; i < bytes.length; i++) {
      const c = bytes[i];
      if (c === 9 || c === 10 || c === 13 || (c >= 32 && c < 127)) printable++;
    }
    if (bytes.length > 0 && printable / bytes.length < 0.9) {
      return { text: `<binary, ${bytes.length} bytes>`, binary: true };
    }
    const text = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
    try {
      const j = JSON.parse(text);
      return { text: JSON.stringify(j, null, 2), binary: false };
    } catch {
      return { text, binary: false };
    }
  } catch {
    return { text: `<invalid base64>`, binary: true };
  }
}

export function truncate(s: string, n: number): string {
  if (!s) return "";
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

export function timeLabel(ts: number): string {
  return new Date(ts).toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}