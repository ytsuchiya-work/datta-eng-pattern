import { useState } from "react";
import type { CEvent } from "./concept";
import { SPAN } from "./concept";
import type { Hist, State } from "./api";
import { hms, tsMs } from "./api";

const W = 900, L = 118;

// ───────────── イメージ（概念タイムライン） ─────────────
export function ConceptTimeline({ events, t }: { events: CEvent[]; t: number }) {
  const lanes = ["ファイル着地", "起動 / 検知", "取り込み実行", "silver 反映"];
  const H = 26, top = 18;
  const x = (s: number) => L + (s / SPAN) * (W - L - 10);
  const y = (i: number) => top + i * H;
  return (
    <svg viewBox={`0 0 ${W} ${top + lanes.length * H + 6}`} className="tl concept">
      {[0, 30, 60, 90, 120, 150].map((s) => (
        <g key={s}><line x1={x(s)} x2={x(s)} y1={top - 4} y2={top + lanes.length * H} className="grid" />
          <text x={x(s)} y={11} className="tick">{s}s</text></g>
      ))}
      {lanes.map((l, i) => (
        <g key={l}><rect x={0} y={y(i)} width={W} height={H - 2} className={`lane l${i % 2}`} />
          <text x={8} y={y(i) + 16} className="lname">{l}</text></g>
      ))}
      {events.map((e, k) => {
        const past = e.t <= t;
        if (e.kind === "file")
          return <g key={k} className={`m file ${past ? "on" : ""}`} transform={`translate(${x(e.t)},${y(0) + 12})`}>
            <rect x={-6} y={-8} width={12} height={15} rx={2} /><text y={4}>{e.id + 1}</text></g>;
        if (e.kind === "poll")
          return <circle key={k} cx={x(e.t)} cy={y(1) + 12} r={3.5} className={`m poll ${past ? "on" : ""}`} />;
        if (e.kind === "trigger")
          return <g key={k} className={`m trig ${past ? "on" : ""}`} transform={`translate(${x(e.t)},${y(1) + 12})`}>
            <path d="M0,-8 L7,0 L0,8 L-7,0 Z" /><text x={10} y={4}>{e.label}</text></g>;
        if (e.kind === "run") {
          const lane = e.lane === "silver" ? 3 : 2;
          return <g key={k} className={`m run ${e.lane} ${past ? "on" : ""}`}>
            {e.startup > 0 && <rect x={x(e.t)} y={y(lane) + 5} width={x(e.t + e.startup) - x(e.t)} height={14} rx={3} className="startup" />}
            <rect x={x(e.t + e.startup)} y={y(lane) + 5} width={Math.max(3, x(e.end) - x(e.t + e.startup))} height={14} rx={3} />
          </g>;
        }
        if (e.kind === "bronze")
          return <g key={k} className={`m bz ${past ? "on" : ""}`} transform={`translate(${x(e.t)},${y(2) + 12})`}>
            <circle r={4} /></g>;
        if (e.kind === "silver")
          return <g key={k} className={`m sv ${past ? "on" : ""}`} transform={`translate(${x(e.t)},${y(3) + 12})`}>
            <circle r={6} /><text x={9} y={4}>{e.files.map((f) => f + 1).join(",") || ""}</text></g>;
        return null;
      })}
      <line x1={x(t)} x2={x(t)} y1={top - 6} y2={top + lanes.length * H} className="playhead" />
    </svg>
  );
}

// ───────────── 実測タイムライン ─────────────
const TRIG: Record<string, string> = { ONE_TIME: "手動", PERIODIC: "スケジュール", FILE_ARRIVAL: "ファイル到着", TABLE: "テーブル更新", CONTINUOUS: "連続", RETRY: "再試行" };

type Tip = { x: number; y: number; lines: string[] } | null;

export function RealTimeline({ st, minutes }: { st: State; minutes: number }) {
  const [tip, setTip] = useState<Tip>(null);
  const now = st.now, t0 = now - minutes * 60_000;
  const lanes = ["ファイル着地", "トリガー / 起動", "取り込みジョブ", "silver 反映ジョブ", "bronze コミット", "silver コミット"];
  const H = 25, top = 18;
  const x = (ms: number) => L + ((Math.max(ms, t0) - t0) / (now - t0)) * (W - L - 14);
  const y = (i: number) => top + i * H;
  const show = (e: React.MouseEvent, lines: string[]) => {
    const r = ((e.currentTarget as SVGGraphicsElement).ownerSVGElement as SVGSVGElement).getBoundingClientRect();
    setTip({ x: e.clientX - r.left, y: e.clientY - r.top, lines });
  };
  const hide = () => setTip(null);
  const ticks: number[] = [];
  const step = minutes <= 10 ? 60_000 : minutes <= 30 ? 300_000 : 600_000;
  for (let s = Math.ceil(t0 / step) * step; s <= now; s += step) ticks.push(s);

  const commits = (h: Hist[] | null, lane: number, cls: string) =>
    (h ?? []).filter((c) => tsMs(c.ts) >= t0).map((c) => {
      const minor = ["OPTIMIZE", "CREATE TABLE", "VACUUM START", "VACUUM END"].includes(c.operation);
      const m = c.metrics;
      const desc = Object.entries(m).filter(([k]) => /num(Output|TargetRows|Files|Copied|Removed|Added)/.test(k)).slice(0, 6).map(([k, v]) => `${k}: ${v}`);
      return (
        <g key={`${cls}${c.version}`} className={`m ${cls} ${minor ? "minor" : "on"}`} transform={`translate(${x(tsMs(c.ts))},${y(lane) + 12})`}
          onMouseEnter={(e) => show(e, [`v${c.version} ${c.operation}`, hms(tsMs(c.ts)), ...desc])} onMouseLeave={hide}>
          {minor ? <rect x={-3} y={-3} width={6} height={6} /> : <circle r={6} />}
          {!minor && <text x={8} y={4}>{c.operation.replace("STREAMING UPDATE", "STREAM").replace("COPY INTO", "COPY")}</text>}
        </g>
      );
    });

  return (
    <div className="tlwrap">
      <svg viewBox={`0 0 ${W} ${top + lanes.length * H + 6}`} className="tl real">
        {ticks.map((s) => (
          <g key={s}><line x1={x(s)} x2={x(s)} y1={top - 4} y2={top + lanes.length * H} className="grid" />
            <text x={x(s)} y={11} className="tick">{hms(s).slice(0, 5)}</text></g>
        ))}
        {lanes.map((l, i) => (
          <g key={l}><rect x={0} y={y(i)} width={W} height={H - 2} className={`lane l${i % 2}`} />
            <text x={8} y={y(i) + 16} className="lname">{l}</text></g>
        ))}
        {(st.events ?? []).map((e, k) => {
          const ms = tsMs(e.ts);
          if (ms < t0) return null;
          const d = JSON.parse(e.detail || "{}");
          if (e.event === "file_landed")
            return <g key={k} className="m file on" transform={`translate(${x(ms)},${y(0) + 12})`}
              onMouseEnter={(ev) => show(ev, ["ファイル着地", hms(ms), d.file, `${d.rows} 行`])} onMouseLeave={hide}>
              <rect x={-6} y={-8} width={12} height={15} rx={2} /></g>;
          if (["timing_set", "all_stopped", "auto_off", "reset"].includes(e.event))
            return <g key={k} className="m flag" transform={`translate(${x(ms)},${y(1) + 4})`}
              onMouseEnter={(ev) => show(ev, [e.event === "timing_set" ? `タイミング変更: ${d.timing}` : e.event, hms(ms)])} onMouseLeave={hide}>
              <path d="M0,0 L0,16 M0,0 L8,3 L0,6" /></g>;
          return null;
        })}
        {st.runs.map((r) => {
          const lane = r.job === "silver" ? 3 : 2;
          const end = r.end ?? now;
          const label = r.params?.mode === "stream" ? "Streaming" : r.params?.origin === "app_watcher" ? "ファイル到着（アプリ代行）" : TRIG[r.trigger] ?? r.trigger;
          const firstTask = Math.min(...r.tasks.map((t) => t.start || end).filter(Boolean), end);
          const tip = [`run ${r.run_id}（${label}）`, `${hms(r.start)} → ${r.end ? hms(r.end) : "実行中"}`,
            `${Math.round((end - r.start) / 1000)} 秒 · ${r.result ?? r.life}`,
            ...r.tasks.filter((t) => t.start).map((t) => `${t.key}: ${Math.round(((t.end ?? now) - t.start) / 1000)}s ${t.result ?? t.life}`)];
          return (
            <g key={r.run_id} onMouseEnter={(e) => show(e, tip)} onMouseLeave={hide} onClick={() => window.open(r.url, "_blank")} className="clk">
              <g className="m trig on" transform={`translate(${x(r.start)},${y(1) + 12})`}>
                <path d="M0,-8 L7,0 L0,8 L-7,0 Z" />
                {x(r.start) > W - 170 ? <text x={-10} y={4} textAnchor="end">{label}</text> : <text x={10} y={4}>{label}</text>}
              </g>
              <rect x={x(r.start)} y={y(lane) + 5} width={Math.max(2, x(firstTask) - x(r.start))} height={14} rx={3} className="bar startup" />
              {r.tasks.filter((t) => t.start && t.key !== "check_stage").map((t) => (
                <rect key={t.key} x={x(t.start)} y={y(lane) + 5} width={Math.max(3, x(t.end ?? now) - x(t.start))} height={14} rx={3}
                  className={`bar ${t.key} ${t.result === "FAILED" ? "failed" : ""} ${t.end ? "" : "live"}`} />
              ))}
            </g>
          );
        })}
        {commits(st.hist_b, 4, "bz")}
        {commits(st.hist_s, 5, "sv")}
        <line x1={x(now)} x2={x(now)} y1={top - 6} y2={top + lanes.length * H} className="nowline" />
      </svg>
      {tip && <div className="tip" style={{ left: Math.min(tip.x + 12, 700), top: tip.y + 12 }}>{tip.lines.map((l, i) => <div key={i}>{l}</div>)}</div>}
    </div>
  );
}

// ファイルごとの「着地 → bronze → silver」レイテンシ
export function latencies(st: State) {
  const landed = (st.events ?? []).filter((e) => e.event === "file_landed").map((e) => ({ file: JSON.parse(e.detail).file as string, at: tsMs(e.ts) }));
  const bz = new Map((st.bronze_files ?? []).map((b) => [b._file, tsMs(b.ingested_at)]));
  const sv = (st.hist_s ?? []).filter((h) => !["OPTIMIZE", "CREATE TABLE"].includes(h.operation)).map((h) => tsMs(h.ts)).sort((a, b) => a - b);
  return landed.map((l) => {
    const b = bz.get(l.file);
    const s = b != null ? sv.find((t) => t >= b - 1500) : undefined;
    return { file: l.file, landed: l.at, bronze: b ?? null, silver: s ?? null };
  }).reverse();
}
