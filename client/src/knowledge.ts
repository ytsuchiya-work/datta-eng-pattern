// 取り込み方式 × 更新方法 × 更新タイミング の解説データ
export type Ingest = "al" | "ci";
export type Method = "append" | "full" | "cdc";
export type Timing = "manual" | "schedule" | "file_arrival" | "table_update" | "streaming";
export type Grade = "◎" | "○" | "△" | "✕";

export const INGESTS: { id: Ingest; label: string; short: string; desc: string; points: string[] }[] = [
  {
    id: "al", label: "Auto Loader", short: "AL",
    desc: "cloudFiles ソースの Structured Streaming。チェックポイントで処理済みファイルを管理し、新しいファイルだけを増分で取り込む。",
    points: ["大量ファイル・継続的な到着に強い", "スキーマ推論・スキーマ進化に対応", "availableNow でバッチ的にも、常時起動でも動く"],
  },
  {
    id: "ci", label: "COPY INTO", short: "CI",
    desc: "SQL コマンド。取り込み済みファイルをテーブル側が記録するため、再実行しても二重取り込みしない（冪等）。",
    points: ["SQL Warehouse から実行でき手軽", "起動が速い（このデモで約 10 秒）", "バッチ専用。常時起動のストリーミングはできない"],
  },
];

export const METHODS: { id: Method; label: string; op: string; desc: string; points: string[] }[] = [
  {
    id: "append", label: "追記", op: "INSERT",
    desc: "新しいレコードをそのまま足していく。ソース側の更新・削除は反映されない。",
    points: ["ログ・イベント・明細など、追記のみのデータ向け", "最も軽い処理"],
  },
  {
    id: "full", label: "フルリフレッシュ", op: "INSERT OVERWRITE",
    desc: "毎回、全件スナップショットでテーブル全体を置き換える。",
    points: ["仕組みが単純で、削除も含めてソースと一致する", "データ量に比例して毎回のコストが増える"],
  },
  {
    id: "cdc", label: "CDC", op: "MERGE",
    desc: "変更レコード（INSERT / UPDATE / DELETE と順序キー）をキー単位で MERGE する。",
    points: ["差分だけで更新・削除まで反映できる", "順序キー（_seq）による並べ替え・重複排除が必要", "Lakeflow 宣言型パイプラインなら AUTO CDC で SCD1/2 を宣言的に書ける"],
  },
];

export const TIMINGS: { id: Timing; label: string; icon: string; desc: string; impl: string }[] = [
  {
    id: "manual", label: "手動バッチ", icon: "▶",
    desc: "必要なときに人やほかのシステムがジョブを起動する。",
    impl: "「今すぐ実行」で取り込みジョブを run-now。",
  },
  {
    id: "schedule", label: "定期スケジュール", icon: "⏰",
    desc: "cron で決まった間隔ごとに起動する。遅延は最大でスケジュール間隔ぶん。",
    impl: "ジョブの schedule（Quartz cron: 2 分ごと）を有効化。",
  },
  {
    id: "file_arrival", label: "ファイル到着トリガー", icon: "📄",
    desc: "監視パスに新しいファイルが届いたらジョブを起動する。ポーリングは約 1 分ごと（ベストエフォート）。",
    impl: "ジョブの trigger.file_arrival（監視先: landing/<パターン>/、最小間隔 60 秒）を有効化。※このワークスペースではジョブのファイル到着トリガーが発火しないことを確認済みのため、新しいファイルから 75 秒以内にジョブが起動しなければ、アプリの監視が代わりに起動する（タイムラインに「ファイル到着（アプリ代行）」と表示）。",
  },
  {
    id: "table_update", label: "テーブル更新トリガー", icon: "🔁",
    desc: "上流テーブルが更新されたら下流ジョブを起動する。多段パイプラインの 2 段目以降で使う。",
    impl: "bronze 取り込み（上流）は手動 → bronze の更新を検知した silver 反映ジョブが自動起動（trigger.table_update）。",
  },
  {
    id: "streaming", label: "Streaming", icon: "〰",
    desc: "常時起動し、数秒ごとのマイクロバッチで処理し続ける。遅延は最小だが、起動中はずっとコストがかかる。",
    impl: "Auto Loader を常時起動（10 秒間隔、最大 10 分で自動停止）し、各マイクロバッチで silver まで反映。",
  },
];

const G: Record<Ingest, Record<Method, Record<Timing, Grade>>> = {
  al: {
    append: { manual: "○", schedule: "◎", file_arrival: "◎", table_update: "○", streaming: "◎" },
    full: { manual: "△", schedule: "△", file_arrival: "△", table_update: "△", streaming: "△" },
    cdc: { manual: "○", schedule: "◎", file_arrival: "◎", table_update: "○", streaming: "◎" },
  },
  ci: {
    append: { manual: "◎", schedule: "◎", file_arrival: "○", table_update: "○", streaming: "✕" },
    full: { manual: "○", schedule: "◎", file_arrival: "○", table_update: "○", streaming: "✕" },
    cdc: { manual: "○", schedule: "◎", file_arrival: "○", table_update: "○", streaming: "✕" },
  },
};

export const GRADE_LABEL: Record<Grade, string> = { "◎": "定番", "○": "可能", "△": "注意", "✕": "不可" };

export function grade(i: Ingest, m: Method, t: Timing): Grade {
  return G[i][m][t];
}

export function comboNote(i: Ingest, m: Method, t: Timing): string[] {
  const n: string[] = [];
  if (i === "ci" && t === "streaming")
    n.push("COPY INTO はバッチ専用の SQL コマンドのため常時起動はできない。秒単位の鮮度が必要なら Auto Loader を使う。");
  if (i === "al" && m === "full")
    n.push("Auto Loader は「新しいファイルだけ」を読む増分前提の仕組み。毎回全件を置き換えるなら、read_files + CREATE OR REPLACE TABLE やマテリアライズドビュー、スナップショット比較の AUTO CDC FROM SNAPSHOT のほうが素直。");
  if (i === "al" && m === "full" && t === "streaming")
    n.push("マイクロバッチのたびに全件を上書きするため、処理が重くなりやすい。");
  if (i === "al" && t === "file_arrival")
    n.push("「ファイル到着トリガー + availableNow」は、常時起動せずに到着ベースで増分処理する定番の組み合わせ。");
  if (i === "al" && t === "schedule")
    n.push("availableNow で未処理ファイルだけを処理して終了するので、定期実行でも二重取り込みしない。");
  if (i === "ci" && t === "file_arrival")
    n.push("動作はする。ただしファイル数が多い・到着が頻繁な場合は Auto Loader のほうがスケールする。");
  if (m === "cdc" && t === "streaming")
    n.push("マイクロバッチごとに foreachBatch で MERGE する。宣言型パイプラインなら AUTO CDC が順序の乱れも吸収する。");
  if (m === "append" && t === "table_update")
    n.push("bronze に行が追加されるたびに、silver へ追記するジョブが連鎖して起動する。");
  if (m === "full" && t === "table_update")
    n.push("bronze に新しいスナップショットが入ると、silver を全件置き換えるジョブが起動する。");
  if (t === "table_update")
    n.push("このデモでは bronze 取り込みを「bronze へ取り込み（上流）」ボタンで実行し、silver 側だけをテーブル更新トリガーで動かす。");
  if (i === "ci" && t === "manual" && m === "append") n.push("SQL Warehouse 上で数秒で終わる、もっとも手軽な構成。");
  return n;
}

export function latencyHint(i: Ingest, t: Timing): string {
  const run = i === "al" ? "サーバーレス起動込みで約 1 分" : "約 10 秒";
  switch (t) {
    case "manual": return `実行した時点まで反映されない（処理自体は${run}）`;
    case "schedule": return `最大で間隔（2 分）＋処理時間（${run}）`;
    case "file_arrival": return `検知まで約 1〜1.5 分 ＋ 処理時間（${run}）`;
    case "table_update": return `上流の更新を検知するまで ＋ silver 反映（約 10 秒）`;
    case "streaming": return i === "al" ? "数秒〜数十秒（常時起動中のみ）" : "—";
  }
}

export function costHint(t: Timing): string {
  switch (t) {
    case "manual": return "実行したときだけ";
    case "schedule": return "間隔ごとに毎回起動（データがなくても起動する）";
    case "file_arrival": return "ファイルが届いたときだけ起動";
    case "table_update": return "上流が更新されたときだけ起動";
    case "streaming": return "起動中は常にコンピュートを使う";
  }
}

export const pid = (i: Ingest, m: Method) => `${i}_${m}`;
