// 「イメージ」用の概念タイムライン（実データではなく、各タイミングの典型的な動きを再現する）
import type { Ingest, Timing } from "./knowledge";

export type CEvent =
  | { kind: "file"; t: number; id: number }
  | { kind: "trigger"; t: number; label: string }
  | { kind: "poll"; t: number }
  | { kind: "run"; t: number; end: number; lane: "ingest" | "silver"; files: number[]; startup: number }
  | { kind: "bronze"; t: number; files: number[] }
  | { kind: "silver"; t: number; files: number[] };

export const SPAN = 150; // 秒
export const FILES = [6, 19, 41, 50, 84, 103, 128];

export function conceptEvents(ingest: Ingest, timing: Timing): CEvent[] {
  const ev: CEvent[] = FILES.map((t, id) => ({ kind: "file", t, id }));
  const startup = ingest === "al" ? 22 : 2;
  const work = ingest === "al" ? 10 : 6;
  const pending = new Set(FILES.map((_, i) => i));
  const take = (upTo: number) => {
    const got = FILES.map((t, i) => [t, i]).filter(([t, i]) => t <= upTo && pending.has(i)).map(([, i]) => i);
    got.forEach((i) => pending.delete(i));
    return got;
  };
  const batch = (start: number, label: string) => {
    const end = start + startup + work;
    const files = take(start + startup);
    ev.push({ kind: "trigger", t: start, label });
    ev.push({ kind: "run", t: start, end, lane: "ingest", files, startup });
    ev.push({ kind: "bronze", t: start + startup + work * 0.6, files });
    ev.push({ kind: "silver", t: end, files });
    return end;
  };

  if (timing === "manual") {
    batch(70, "手動実行");
  } else if (timing === "schedule") {
    for (const s of [0, 60, 120]) batch(s, "cron");
  } else if (timing === "file_arrival") {
    // 約 60 秒ごとにポーリングし、新しいファイルがあれば起動（最小間隔 60 秒）
    let last = -999;
    let busyUntil = -1;
    for (let p = 15; p < SPAN; p += 60) {
      ev.push({ kind: "poll", t: p });
      const has = FILES.some((t, i) => t <= p && pending.has(i));
      if (has && p - last >= 60 && p >= busyUntil) {
        busyUntil = batch(p, "ファイル検知");
        last = p;
      }
    }
  } else if (timing === "table_update") {
    // 上流（bronze 取り込み）を 2 回実行 → bronze 更新を検知して silver ジョブが起動
    for (const s of [25, 95]) {
      const bEnd = s + startup + work;
      const files = take(s + startup);
      ev.push({ kind: "trigger", t: s, label: "上流ジョブ" });
      ev.push({ kind: "run", t: s, end: bEnd, lane: "ingest", files, startup });
      ev.push({ kind: "bronze", t: bEnd, files });
      const sStart = bEnd + 8;
      ev.push({ kind: "trigger", t: sStart, label: "テーブル更新を検知" });
      ev.push({ kind: "run", t: sStart, end: sStart + 6, lane: "silver", files, startup: 2 });
      ev.push({ kind: "silver", t: sStart + 6, files });
    }
  } else if (timing === "streaming") {
    ev.push({ kind: "trigger", t: 0, label: "常時起動" });
    ev.push({ kind: "run", t: 0, end: SPAN, lane: "ingest", files: [], startup: 0 });
    for (let tick = 10; tick < SPAN; tick += 10) {
      const files = take(tick);
      if (files.length) {
        ev.push({ kind: "bronze", t: tick + 1, files });
        ev.push({ kind: "silver", t: tick + 2.5, files });
      }
    }
  }
  return ev.sort((a, b) => a.t - b.t);
}

// 時刻 t における各ノードの状態（図のアニメーション用）
export function snapshotAt(ev: CEvent[], t: number) {
  const landed = ev.filter((e) => e.kind === "file" && e.t <= t).length;
  const inBronze = new Set<number>();
  const inSilver = new Set<number>();
  for (const e of ev) {
    if (e.kind === "bronze" && e.t <= t) e.files.forEach((f) => inBronze.add(f));
    if (e.kind === "silver" && e.t <= t) e.files.forEach((f) => inSilver.add(f));
  }
  const running = ev.find((e) => e.kind === "run" && e.t <= t && t < e.end) as Extract<CEvent, { kind: "run" }> | undefined;
  const recent = (k: CEvent["kind"], win = 1.6) => ev.some((e) => e.kind === k && e.t <= t && t - e.t < win);
  return {
    waiting: landed - inBronze.size,
    landed,
    bronze: inBronze.size,
    silver: inSilver.size,
    running,
    starting: running ? t < running.t + running.startup : false,
    pulse: {
      file: recent("file"),
      trigger: recent("trigger", 2.2) || recent("poll", 1.2),
      bronze: recent("bronze"),
      silver: recent("silver"),
    },
  };
}
