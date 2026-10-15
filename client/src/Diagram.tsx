import type { CEvent } from "./concept";
import { snapshotAt } from "./concept";
import type { Ingest, Method, Timing } from "./knowledge";
import { TIMINGS } from "./knowledge";

export type Real = {
  source: number | null; landing: number | null; unprocessed: number | null;
  bronze: number | null; silver: number | null; same: number | null; total: number | null;
  running: "starting" | "bronze" | "silver" | null;
};

const X = { src: 62, land: 202, trig: 338, eng: 474, brz: 614, apply: 754, slv: 894 };
const Y = 92;

function Node({ x, label, sub, active, pulse, kind, real, concept }: {
  x: number; label: string; sub: string; active?: boolean; pulse?: boolean; kind: string;
  real?: string | null; concept?: string | null;
}) {
  return (
    <g className={`node ${kind} ${active ? "active" : ""} ${pulse ? "pulse" : ""}`} transform={`translate(${x - 56},${Y - 38})`}>
      <rect width={112} height={76} rx={10} />
      <text x={56} y={24} className="nl">{label}</text>
      <text x={56} y={42} className="ns">{sub}</text>
      {concept != null && <text x={56} y={62} className="nc">{concept}</text>}
      {real != null && (
        <g transform="translate(6,84)">
          <rect width={100} height={22} rx={11} className="realpill" />
          <text x={50} y={15} className="nr">{real}</text>
        </g>
      )}
    </g>
  );
}

function Edge({ from, to, active, label }: { from: number; to: number; active?: boolean; label?: string }) {
  const x1 = from + (from === X.trig ? 38 : 54), x2 = to - (to === X.trig ? 38 : 54);
  return (
    <g className={`edge ${active ? "active" : ""}`}>
      <line x1={x1} y1={Y} x2={x2 - 6} y2={Y} />
      <path d={`M${x2 - 8},${Y - 5} L${x2},${Y} L${x2 - 8},${Y + 5}`} />
      {label && <text x={(x1 + x2) / 2} y={Y - 10}>{label}</text>}
    </g>
  );
}

export default function Diagram({ ingest, method, timing, events, t, real, conceptOn }: {
  ingest: Ingest; method: Method; timing: Timing; events: CEvent[]; t: number; real: Real; conceptOn: boolean;
}) {
  const s = snapshotAt(events, t);
  const tm = TIMINGS.find((x) => x.id === timing)!;
  const disabled = ingest === "ci" && timing === "streaming";
  const ingestRun = conceptOn && s.running && s.running.lane === "ingest" && !s.starting;
  const silverRun = conceptOn && s.running && s.running.lane === "silver";
  const streaming = timing === "streaming";
  const silverOp = method === "append" ? "INSERT" : method === "full" ? "OVERWRITE" : "MERGE";
  const engine = ingest === "al" ? "Auto Loader" : "COPY INTO";
  const engineSub = ingest === "al" ? (streaming ? "常時起動" : "availableNow") : "SQL Warehouse";
  const fileExport = method === "append" ? "新規分のみ" : method === "full" ? "全件スナップショット" : "変更レコード";
  const fileOn = conceptOn && s.pulse.file;
  const pct = real.total ? Math.round(((real.same ?? 0) / real.total) * 100) : null;

  return (
    <svg viewBox="0 0 958 190" className={`diagram ${disabled ? "disabled" : ""}`}>
      <defs>
        <linearGradient id="gSilver" x1="0" x2="1"><stop offset="0" stopColor="#e8f0fb" /><stop offset="1" stopColor="#dbe8f9" /></linearGradient>
      </defs>
      <Edge from={X.src} to={X.land} active={fileOn} />
      <Edge from={X.land} to={X.trig} active={conceptOn && (s.pulse.trigger || !!ingestRun)} />
      <Edge from={X.trig} to={X.eng} active={conceptOn && (s.pulse.trigger || !!ingestRun)} />
      <Edge from={X.eng} to={X.brz} active={!!ingestRun || (conceptOn && s.pulse.bronze)} />
      <Edge from={X.brz} to={X.apply} active={!!silverRun || (conceptOn && (s.pulse.silver || (timing === "table_update" && s.pulse.trigger)))} />
      <Edge from={X.apply} to={X.slv} active={conceptOn && s.pulse.silver} />

      <Node x={X.src} kind="src" label="ソースDB" sub="source_orders" pulse={fileOn}
        concept={null} real={real.source != null ? `${real.source} 件` : null} />
      <Node x={X.land} kind="land" label="ランディング" sub={fileExport} pulse={fileOn}
        concept={conceptOn ? `待機 ${s.waiting} ファイル` : null}
        real={real.landing != null ? `${real.landing} 件 / 未処理 ${real.unprocessed ?? "-"}` : null} />
      <g className={`node trig ${conceptOn && s.pulse.trigger ? "pulse" : ""} ${conceptOn && s.running ? "active" : ""}`}
        transform={`translate(${X.trig},${Y})`}>
        <circle r={36} />
        <text y={-4} className="ti">{tm.icon}</text>
        <text y={16} className="ns">{tm.label.replace("トリガー", "")}</text>
        {real.running && (
          <g transform="translate(-50,46)">
            <rect width={100} height={22} rx={11} className="realpill live" />
            <text x={50} y={15} className="nr">{real.running === "starting" ? "起動中…" : "実行中"}</text>
          </g>
        )}
      </g>
      <Node x={X.eng} kind="eng" label={engine} sub={engineSub} active={!!ingestRun}
        concept={conceptOn && s.running?.lane === "ingest" ? (s.starting ? "起動中…" : "取り込み中") : conceptOn ? "停止" : null} />
      <Node x={X.brz} kind="brz" label="bronze" sub="生データ（追記）" pulse={conceptOn && s.pulse.bronze}
        concept={conceptOn ? `${s.bronze} ファイル分` : null} real={real.bronze != null ? `${real.bronze} 行` : null} />
      <Node x={X.apply} kind="apply" label={silverOp} sub={method === "cdc" ? "キー + _seq" : method === "full" ? "最新スナップショット" : "未処理ファイル分"} active={!!silverRun}
        concept={null} />
      <Node x={X.slv} kind="slv" label="silver" sub="利用者が見るテーブル" pulse={conceptOn && s.pulse.silver}
        concept={conceptOn ? `${s.silver} ファイル反映` : null}
        real={real.silver != null ? `${real.silver} 行 · 一致 ${pct ?? "-"}%` : null} />
      {disabled && (
        <g className="ng">
          <rect x={300} y={60} width={320} height={64} rx={12} />
          <text x={460} y={88}>✕ COPY INTO は Streaming 不可</text>
          <text x={460} y={108} className="sub">バッチ専用の SQL コマンドのため</text>
        </g>
      )}
    </svg>
  );
}
