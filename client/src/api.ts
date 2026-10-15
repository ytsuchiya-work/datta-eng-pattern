export type Run = {
  run_id: number; job: "ingest" | "silver"; trigger: string; start: number; end: number | null;
  life: string; result: string | null; message: string | null; url: string; params: Record<string, string>;
  tasks: { key: string; start: number; end: number | null; life: string; result: string | null }[];
};
export type Hist = { version: number; ts: string; operation: string; metrics: Record<string, string>; params: Record<string, string> };
export type Ev = { ts: string; pattern: string; event: string; detail: string };
export type Cmp = {
  order_id: number; customer: string | null; item: string | null; qty: number | null; amount: number | null;
  status: string | null; updated_at: string | null; _file: string | null; processed_at: string | null; dup: number | null;
  src_status: string | null; src_qty: number | null; sync: "same" | "stale" | "missing" | "deleted_at_source";
};
export type TimingState = {
  timing: string; enabled_at: string | null; schedule: unknown; trigger: unknown; job_id: number; silver_job_id: number;
};
export type State = {
  timing: TimingState | null; runs: Run[]; hist_b: Hist[] | null; hist_s: Hist[] | null; events: Ev[] | null;
  compare: Cmp[] | null; bronze_files: { _file: string; rows: number; ingested_at: string; ops: string }[] | null;
  bronze_rows: Record<string, string | number | null>[] | null;
  landing: { name: string; size: number; modified: number }[] | null;
  now: number; host: string; errors?: Record<string, string>;
};
export type Overview = {
  patterns: Record<string, { total: number; same: number; bronze_rows: number; silver_rows: number; timing: string; active: boolean }>;
};
export type Boot = { status: string; error: string | null; catalog: string; schema: string; auto_off_min: number; stream_min: number };

async function call<T>(method: string, url: string, body?: unknown): Promise<T> {
  const r = await fetch(url, {
    method, headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!r.ok) {
    let msg = `${r.status}`;
    try { msg = (await r.json()).detail ?? msg; } catch { /* noop */ }
    throw new Error(msg);
  }
  return r.json();
}

export const api = {
  boot: () => call<Boot>("GET", "/api/boot"),
  overview: () => call<Overview>("GET", "/api/overview"),
  state: (p: string, minutes: number) => call<State>("GET", `/api/state/${p}?minutes=${minutes}`),
  code: (p: string) => call<{ bronze: string; bronze_lang: string; silver: string }>("GET", `/api/code/${p}`),
  change: (b: { inserts: number; updates: number; deletes: number }) =>
    call<{ file: string; ops: Record<string, number>; source_rows: number }>("POST", "/api/source/change", b),
  timing: (p: string, timing: string) => call<TimingState>("POST", `/api/timing/${p}`, { timing }),
  run: (p: string, stage: "all" | "bronze") => call<{ run_id: number }>("POST", `/api/run/${p}`, { stage }),
  stop: () => call("POST", "/api/stop"),
  reset: () => call("POST", "/api/reset"),
};

// Statement API の TIMESTAMP 文字列（UTC, タイムゾーン表記なし）をエポック ms に
export const tsMs = (s: string | null | undefined) =>
  s ? Date.parse(s.includes("T") ? (s.endsWith("Z") ? s : s + "Z") : s.replace(" ", "T") + "Z") : NaN;

export const hms = (ms: number) =>
  new Date(ms).toLocaleTimeString("ja-JP", { timeZone: "Asia/Tokyo", hour12: false });
