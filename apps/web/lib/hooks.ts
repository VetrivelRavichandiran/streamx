"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, getToken } from "./api";

export interface AsyncState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  /** true when the API is up but the resource does not exist / is empty */
  notFound: boolean;
  /** true when the API itself is unreachable (network error / 5xx) */
  unreachable: boolean;
  lastUpdated: number | null;
  reload: () => void;
}

/**
 * Polling data hook. Refetches every `intervalMs` while the tab is visible.
 * `fetcher` should return the parsed payload; throw ApiError for API errors.
 */
export function usePoll<T>(fetcher: () => Promise<T>, intervalMs: number, deps: unknown[] = []): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [notFound, setNotFound] = useState(false);
  const [unreachable, setUnreachable] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const reload = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const run = async () => {
      if (!getToken()) {
        // Not signed in yet — the layout redirects to /login; stay quiet.
        setLoading(false);
        return;
      }
      try {
        const d = await fetcherRef.current();
        if (cancelled) return;
        setData(d);
        setError(null);
        setNotFound(false);
        setUnreachable(false);
        setLastUpdated(Date.now());
      } catch (e) {
        if (cancelled) return;
        const ae = e instanceof ApiError ? e : new ApiError(0, "error", String(e));
        if (ae.status === 404) {
          setNotFound(true);
          setError(null);
        } else if (ae.status === 0 || ae.status >= 500) {
          setUnreachable(true);
          setError(ae.message);
        } else {
          setError(ae.message);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    const schedule = () => {
      timer = setTimeout(async () => {
        await run();
        if (!cancelled && document.visibilityState === "visible") schedule();
      }, intervalMs);
    };

    void run();
    schedule();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, tick, ...deps]);

  return { data, error, loading, notFound, unreachable, lastUpdated, reload };
}

/** True once the browser has a token (client-side only). */
export function useAuthed(): boolean {
  const [authed, setAuthed] = useState(false);
  useEffect(() => {
    setAuthed(Boolean(getToken()));
  }, []);
  return authed;
}

/** Keep a rolling window of {ts, ...point} samples for live charts. */
export function useSeries<T extends { ts: number }>(intervalMs: number, maxPoints: number, fetcher: () => Promise<T>): T[] {
  const [series, setSeries] = useState<T[]>([]);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const run = async () => {
      try {
        const p = await fetcherRef.current();
        if (!cancelled) {
          setSeries((s) => [...s, p].slice(-maxPoints));
        }
      } catch {
        // keep last known points; the page-level hook surfaces the error
      }
      if (!cancelled) {
        timer = setTimeout(run, intervalMs);
      }
    };
    void run();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [intervalMs, maxPoints]);

  return series;
}