import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { Boot, Cmp, Overview, State } from "./api";
import { api, hms, tsMs } from "./api";
import { conceptEvents, SPAN } from "./concept";
import Diagram from "./Diagram";
import type { Ingest, Method, Timing } from "./knowledge";
import { comboNote, costHint, grade, GRADE_LABEL, INGESTS, latencyHint, METHODS, pid, TIMINGS } from "./knowledge";
import { ConceptTimeline, latencies, RealTimeline } from "./Timelines";

const SYNC: Record<Cmp["sync"], [string, string]> = {
  same: ["一致", "ok"], stale: ["古い", "warn"], missing: ["未反映", "miss"], deleted_at_source: ["ソースで削除済", "del"],
};

function useInterval(fn: () => void, ms: number, deps: unknown[]) {
  useEffect(() => {
    fn();
    const id = setInterval(fn, ms);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
}

export default function App() {
  const [ingest, setIngest] = useState<Ingest>("al");
  const [method, setMethod] = useState<Method>("cdc");
  const [timing, setTiming] = useState<Timing>("file_arrival");
  const [boot, setBoot] = useState<Boot | null>(null);
  const [ov, setOv] = useState<Overview | null>(null);
  const [st, setSt] = useState<State | null>(null);
  const [code, setCode] = useState<{ bronze: string; bronze_lang: string; silver: string } | null>(null);
  const [minutes, setMinutes] = useState(15);
  const [tab, setTab] = useState<"silver" | "bronze" | "landing" | "history" | "code">("silver");
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<{ msg: string; err?: boolean } | null>(null);
  const [chg, setChg] = useState({ inserts: 3, updates: 2, deletes: 1 });
  const [playing, setPlaying] = useState(true);
  const [t, setT] = useState(0);
  const p = pid(ingest, method);
  const ready = boot?.status === "ready";

  // 起動待ち
  useInterval(() => { if (!ready) api.boot().then(setBoot).catch(() => {}); }, 3000, [ready]);
  useInterval(() => { if (ready) api.overview().then(setOv).catch(() => {}); }, 12000, [ready]);
  const loadState = useCallback(() => {
    if (ready) api.state(p, minutes).then((s) => setSt((prev) => (prevKey.current === p ? s : prev))).catch(() => {});
  }, [ready, p, minutes]);
  const prevKey = useRef(p);
  useEffect(() => { prevKey.current = p; setSt(null); }, [p]);
  useInterval(loadState, 5000, [loadState]);
  useEffect(() => { if (ready) api.code(p).then(setCode).catch(() => {}); }, [ready, p]);

  // イメージの再生
  const events = useMemo(() => conceptEvents(ingest, timing), [ingest, timing]);
  useEffect(() => { setT(0); }, [ingest, timing]);
  useEffect(() => {
    if (!playing) return;
    let raf = 0, last = performance.now();
    const loop = (now: number) => {
      const dt = (now - last) / 1000; last = now;
      setT((v) => (v + dt * 3) % SPAN); // 3 倍速
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [playing]);

  const flash = (msg: string, err = false) => { setToast({ msg, err }); setTimeout(() => setToast(null), err ? 7000 : 3500); };
  const act = async (label: string, fn: () => Promise<unknown>, okMsg?: string) => {
    setBusy(label);
    try { await fn(); if (okMsg) flash(okMsg); loadState(); api.overview().then(setOv).catch(() => {}); }
    catch (e) { flash(`${label}に失敗: ${(e as Error).message}`, true); }
    finally { setBusy(null); }
  };

  const g = grade(ingest, method, timing);
  const applied = (st?.timing?.timing ?? ov?.patterns[p]?.timing ?? "manual") as Timing;
  const appliedMatches = applied === timing;
  const activeRun = st?.runs.find((r) => !r.end);
  const runningState = activeRun
    ? (activeRun.tasks.some((tk) => tk.start && !tk.end) ? "bronze" : "starting") as "bronze" | "starting"
    : null;
  const cmp = st?.compare ?? [];
  const counts = { same: 0, stale: 0, missing: 0, deleted_at_source: 0 } as Record<Cmp["sync"], number>;
  cmp.forEach((c) => counts[c.sync]++);
  const bronzeSet = new Set((st?.bronze_files ?? []).map((b) => b._file));
  const landing = st?.landing ?? null;
  const unprocessed = landing ? landing.filter((f) => !bronzeSet.has(f.name)).length : null;
  const source = cmp.length ? cmp.filter((c) => c.sync !== "deleted_at_source").length : null;
  const lat = st ? latencies(st).slice(0, 8) : [];
  const enabledAt = st?.timing?.enabled_at ? Number(st.timing.enabled_at) * 1000 : null;
  const autoOffLeft = enabledAt && boot && ["schedule", "file_arrival", "table_update"].includes(applied)
    ? Math.max(0, Math.round((enabledAt + boot.auto_off_min * 60_000 - Date.now()) / 60_000)) : null;
  const jobUrl = (id?: number) => (st && id ? `${st.host}/jobs/${id}` : undefined);

  return (
    <div className="app">
      <header>
        <div className="brand">
          <div className="logo">⇢</div>
          <div>
            <h1>データ取り込みパターン エクスプローラ</h1>
            <div className="subt">取り込み方式 × 更新方法 × 更新タイミング を、イメージと実際の処理（{boot ? `${boot.catalog}.${boot.schema}` : "…"}）で比較する</div>
          </div>
        </div>
        <div className="hactions">
          <div className="srcbox">
            <span className="lbl">ソースを変更</span>
            {(["inserts", "updates", "deletes"] as const).map((k) => (
              <label key={k} className={`step ${k}`}>
                {k === "inserts" ? "追加" : k === "updates" ? "更新" : "削除"}
                <input type="number" min={0} max={10} value={chg[k]} onChange={(e) => setChg({ ...chg, [k]: Number(e.target.value) })} />
              </label>
            ))}
            <button className="primary" disabled={!ready || !!busy}
              onClick={() => act("ファイル投入", async () => {
                const r = await api.change(chg);
                flash(`ソースを変更し、6 パターンの landing に ${r.file} を出力しました（追加 ${r.ops.INSERT} / 更新 ${r.ops.UPDATE} / 削除 ${r.ops.DELETE}）`);
              })}>
              {busy === "ファイル投入" ? "出力中…" : "📤 ファイルを投入"}
            </button>
          </div>
          <button className="ghost" disabled={!ready || !!busy} onClick={() => act("全トリガー停止", api.stop, "すべてのトリガーとストリームを停止しました")}>⏹ 全停止</button>
          <button className="ghost danger" disabled={!ready || !!busy}
            onClick={() => confirm("6 パターンのテーブル・ファイル・チェックポイントを初期化します。よろしいですか？") && act("リセット", api.reset, "初期状態に戻しました")}>
            {busy === "リセット" ? "初期化中…" : "↺ リセット"}
          </button>
        </div>
      </header>

      {!ready && (
        <div className="bootbar">{boot?.status === "error" ? `初期化エラー: ${boot.error}` : "初期化中です（テーブル・Volume・ジョブを準備しています）…"}</div>
      )}

      <main>
        {/* ───────── 左: 組み合わせ ───────── */}
        <section className="col left">
          <div className="card">
            <h2>組み合わせを選ぶ</h2>
            <div className="seg-group">
              <div className="seg-label">取り込み方式</div>
              <div className="seg">{INGESTS.map((x) => <button key={x.id} className={ingest === x.id ? "on" : ""} onClick={() => setIngest(x.id)}>{x.label}</button>)}</div>
            </div>
            <div className="seg-group">
              <div className="seg-label">更新方法</div>
              <div className="seg">{METHODS.map((x) => <button key={x.id} className={method === x.id ? "on" : ""} onClick={() => setMethod(x.id)}>{x.label}</button>)}</div>
            </div>
            <div className="seg-group">
              <div className="seg-label">更新タイミング</div>
              <div className="seg wrap">{TIMINGS.map((x) => <button key={x.id} className={timing === x.id ? "on" : ""} onClick={() => setTiming(x.id)}>{x.icon} {x.label}</button>)}</div>
            </div>
          </div>

          <div className="card matrix-card">
            <h2>全 30 通りの組み合わせ <span className="legend">◎定番 ○可能 △注意 ✕不可 ／ <i className="dot live" />稼働中</span></h2>
            <table className="matrix">
              <colgroup><col className="c-ing" /><col className="c-meth" />{TIMINGS.map((x) => <col key={x.id} />)}<col className="c-sync" /></colgroup>
              <thead><tr><th /><th />{TIMINGS.map((x) => <th key={x.id} title={x.label}>{x.icon}<div>{({ manual: "手動", schedule: "定期", file_arrival: "ファイル", table_update: "テーブル", streaming: "Stream" } as Record<string, string>)[x.id]}</div></th>)}<th className="synch">一致</th></tr></thead>
              <tbody>
                {INGESTS.map((i) => METHODS.map((m, mi) => {
                  const o = ov?.patterns[pid(i.id, m.id)];
                  return (
                    <tr key={pid(i.id, m.id)} className={pid(i.id, m.id) === p ? "selrow" : ""}>
                      {mi === 0 && <th rowSpan={3} className="ing">{i.short}</th>}
                      <th className="meth">{m.label}</th>
                      {TIMINGS.map((tm) => {
                        const gr = grade(i.id, m.id, tm.id);
                        const sel = i.id === ingest && m.id === method && tm.id === timing;
                        const live = o && o.timing === tm.id && (tm.id !== "manual" || o.active);
                        return (
                          <td key={tm.id} className={`g g${"◎○△✕".indexOf(gr)} ${sel ? "sel" : ""}`}
                            onClick={() => { setIngest(i.id); setMethod(m.id); setTiming(tm.id); }} title={`${i.label} × ${m.label} × ${tm.label}: ${GRADE_LABEL[gr]}`}>
                            {gr}{live && <i className="dot live" />}
                          </td>
                        );
                      })}
                      <td className="sync">{o && o.total ? `${Math.round((o.same / o.total) * 100)}%` : "–"}</td>
                    </tr>
                  );
                }))}
              </tbody>
            </table>
          </div>

          <div className="card fit">
            <div className="fit-head">
              <span className={`badge g${"◎○△✕".indexOf(g)}`}>{g} {GRADE_LABEL[g]}</span>
              <span className="combo">{INGESTS.find((x) => x.id === ingest)!.label} × {METHODS.find((x) => x.id === method)!.label} × {TIMINGS.find((x) => x.id === timing)!.label}</span>
            </div>
            <dl>
              <dt>取り込み</dt><dd>{INGESTS.find((x) => x.id === ingest)!.desc}</dd>
              <dt>更新</dt><dd>{METHODS.find((x) => x.id === method)!.desc}</dd>
              <dt>タイミング</dt><dd>{TIMINGS.find((x) => x.id === timing)!.desc}</dd>
              <dt>鮮度の目安</dt><dd>{latencyHint(ingest, timing)}</dd>
              <dt>コスト感</dt><dd>{costHint(timing)}</dd>
            </dl>
            {comboNote(ingest, method, timing).length > 0 && (
              <ul className="notes">{comboNote(ingest, method, timing).map((n) => <li key={n}>{n}</li>)}</ul>
            )}
          </div>
        </section>

        {/* ───────── 中央: 図とタイムライン ───────── */}
        <section className="col center">
          <div className="card">
            <div className="card-head">
              <h2>データの流れ <span className="hint">青い吹き出し = イメージ（概念）、白いピル = 実データの現在値</span></h2>
              <div className="playctl">
                <button className="mini" onClick={() => setPlaying(!playing)}>{playing ? "⏸ 一時停止" : "▶ 再生"}</button>
                <input type="range" min={0} max={SPAN} step={0.5} value={t} onChange={(e) => { setPlaying(false); setT(Number(e.target.value)); }} />
                <span className="tval">{Math.floor(t)}s</span>
              </div>
            </div>
            <Diagram ingest={ingest} method={method} timing={timing} events={events} t={t} conceptOn
              real={{ source, landing: landing?.length ?? null, unprocessed, bronze: ov?.patterns[p]?.bronze_rows ?? null,
                silver: ov?.patterns[p]?.silver_rows ?? null, same: counts.same, total: cmp.length || null, running: runningState }} />
          </div>

          <div className="card">
            <div className="card-head">
              <h2>イメージ：このタイミングだと、いつ処理されるか <span className="hint">ファイル①〜⑦が届いたときの典型的な動き（{ingest === "al" ? "灰色＝サーバーレス起動" : "COPY INTO は起動が速い"}）</span></h2>
            </div>
            <ConceptTimeline events={events} t={t} />
          </div>

          <div className="card grow">
            <div className="card-head">
              <h2>実測：{p} で実際に起きたこと <span className="hint">ジョブ実行・Delta コミットは API から取得（クリックでジョブ実行ページ）</span></h2>
              <div className="seg small">{[10, 15, 30, 60].map((m) => <button key={m} className={minutes === m ? "on" : ""} onClick={() => setMinutes(m)}>{m}分</button>)}</div>
            </div>
            {st ? <RealTimeline st={st} minutes={minutes} /> : <div className="empty">読み込み中…</div>}
            <div className="latency">
              <div className="lat-title">ファイルごとの反映時間（着地 → bronze → silver）</div>
              {lat.length === 0 ? <div className="empty small">この時間帯に着地したファイルはありません。「📤 ファイルを投入」で発生させてください。</div> : (
                <table className="lat">
                  <tbody>{lat.map((l) => (
                    <tr key={l.file}>
                      <td className="mono">{l.file.replace("orders_", "").replace(".json", "")}</td>
                      <td>{hms(l.landed)} 着地</td>
                      <td>{l.bronze ? <b>+{Math.max(0, Math.round((l.bronze - l.landed) / 1000))}s</b> : <span className="wait">bronze 待ち</span>}</td>
                      <td>{l.silver ? <b className="ok">+{Math.max(0, Math.round((l.silver - l.landed) / 1000))}s で silver</b> : l.bronze ? <span className="wait">silver 待ち</span> : ""}</td>
                    </tr>
                  ))}</tbody>
                </table>
              )}
            </div>
          </div>
        </section>

        {/* ───────── 右: 実行と中身 ───────── */}
        <section className="col right">
          <div className="card ctl">
            <h2>実行コントロール <span className="pname">{p}</span></h2>
            <div className="applied">
              <div>適用中のタイミング: <b>{TIMINGS.find((x) => x.id === applied)?.label ?? applied}</b>
                {autoOffLeft != null && <span className="autooff">（あと約 {autoOffLeft} 分で自動的に手動へ戻ります）</span>}</div>
              <div className="links">
                <a href={jobUrl(st?.timing?.job_id)} target="_blank" rel="noreferrer">取り込みジョブ ↗</a>
                <a href={jobUrl(st?.timing?.silver_job_id)} target="_blank" rel="noreferrer">silver 反映ジョブ ↗</a>
              </div>
            </div>
            {g === "✕" ? (
              <div className="ngbox">この組み合わせは実行できません（{comboNote(ingest, method, timing)[0]}）</div>
            ) : (
              <>
                {!appliedMatches && (
                  <button className="primary wide" disabled={!ready || !!busy}
                    onClick={() => act("タイミング適用", () => api.timing(p, timing),
                      timing === "streaming" ? "ストリームを起動しました（サーバーレス起動に約 1 分）" : `${TIMINGS.find((x) => x.id === timing)!.label} を ${p} に適用しました`)}>
                    {busy === "タイミング適用" ? "適用中…" : `「${TIMINGS.find((x) => x.id === timing)!.label}」を ${p} に適用`}
                  </button>
                )}
                <div className="impl">{TIMINGS.find((x) => x.id === timing)!.impl}</div>
                <div className="btnrow">
                  {(timing === "manual" || timing === "schedule" || timing === "file_arrival") && (
                    <button disabled={!ready || !!busy} onClick={() => act("実行", () => api.run(p, "all"), "取り込みジョブを起動しました")}>▶ 今すぐ実行</button>
                  )}
                  {timing === "table_update" && (
                    <button disabled={!ready || !!busy} onClick={() => act("bronze 取り込み", () => api.run(p, "bronze"), "bronze 取り込み（上流）を起動しました")}>▶ bronze へ取り込み（上流）</button>
                  )}
                  {applied !== "manual" && (
                    <button className="ghost" disabled={!ready || !!busy} onClick={() => act("停止", () => api.timing(p, "manual"), "手動に戻しました")}>
                      {applied === "streaming" ? "⏹ ストリーム停止" : "⏹ トリガー解除"}</button>
                  )}
                </div>
                <ol className="howto">
                  {timing === "manual" && <><li>📤 ファイルを投入</li><li>▶ 今すぐ実行 → 約 {ingest === "al" ? "1 分" : "10 秒"}で silver が一致</li></>}
                  {timing === "schedule" && <><li>適用すると 2 分ごとに起動</li><li>📤 ファイル投入後、次のスケジュールで反映</li></>}
                  {timing === "file_arrival" && <><li>適用後に 📤 ファイルを投入</li><li>新しいファイルの検知でジョブが自動起動</li></>}
                  {timing === "table_update" && <><li>適用 → 📤 ファイルを投入</li><li>▶ bronze へ取り込み（上流）</li><li>bronze の更新を検知して silver 反映ジョブが自動起動</li></>}
                  {timing === "streaming" && <><li>適用でストリーム起動（最大 {boot?.stream_min ?? 10} 分）</li><li>📤 ファイルを投入 → 数十秒で silver に反映</li></>}
                </ol>
              </>
            )}
            {activeRun && (
              <div className="live-run">
                <i className="dot live" /> 実行中: run {activeRun.run_id}（{hms(activeRun.start)} 開始・{Math.round((st!.now - activeRun.start) / 1000)} 秒経過）
                <a href={activeRun.url} target="_blank" rel="noreferrer">開く ↗</a>
              </div>
            )}
          </div>

          <div className="card grow data">
            <div className="tabs">
              {([["silver", "silver（ソース突合）"], ["bronze", "bronze"], ["landing", "ランディング"], ["history", "Delta 履歴"], ["code", "コード"]] as const).map(([k, l]) => (
                <button key={k} className={tab === k ? "on" : ""} onClick={() => setTab(k)}>{l}</button>
              ))}
            </div>
            <div className="tabbody">
              {!st && tab !== "code" && <div className="empty">読み込み中…</div>}
              {st && tab === "silver" && (
                <>
                  <div className="syncsum">
                    {(Object.keys(SYNC) as Cmp["sync"][]).map((k) => <span key={k} className={`chip ${SYNC[k][1]}`}>{SYNC[k][0]} {counts[k]}</span>)}
                  </div>
                  <table className="dt">
                    <thead><tr><th>注文</th><th>顧客</th><th>商品</th><th>数</th><th>状態（silver）</th><th>ソース</th><th>突合</th></tr></thead>
                    <tbody>{cmp.map((c) => (
                      <tr key={c.order_id} className={`r-${SYNC[c.sync][1]}`}>
                        <td className="mono">{c.order_id}</td><td>{c.customer ?? "—"}</td><td>{c.item ?? "—"}</td><td>{c.qty ?? "—"}</td>
                        <td>{c.status ?? "—"}</td><td>{c.src_status ?? "（削除）"}</td>
                        <td><span className={`chip ${SYNC[c.sync][1]}`}>{SYNC[c.sync][0]}</span></td>
                      </tr>
                    ))}</tbody>
                  </table>
                </>
              )}
              {st && tab === "bronze" && (
                <table className="dt">
                  <thead><tr><th>取込</th><th>注文</th><th>状態</th><th>_op</th><th>_file</th></tr></thead>
                  <tbody>{(st.bronze_rows ?? []).map((r, i) => (
                    <tr key={i}><td className="mono">{hms(tsMs(r.ingested_at as string))}</td><td className="mono">{r.order_id}</td><td>{r.status}</td>
                      <td><span className={`op op-${r._op}`}>{r._op}</span></td><td className="mono small">{String(r._file).replace("orders_", "")}</td></tr>
                  ))}</tbody>
                </table>
              )}
              {st && tab === "landing" && (
                <table className="dt">
                  <thead><tr><th>ファイル</th><th>サイズ</th><th>着地</th><th>bronze</th></tr></thead>
                  <tbody>{[...(landing ?? [])].reverse().map((f) => (
                    <tr key={f.name}><td className="mono small">{f.name}</td><td>{f.size} B</td><td className="mono">{hms(f.modified)}</td>
                      <td>{bronzeSet.has(f.name) ? <span className="chip ok">取込済</span> : <span className="chip warn">未処理</span>}</td></tr>
                  ))}</tbody>
                </table>
              )}
              {st && tab === "history" && (
                <table className="dt">
                  <thead><tr><th>時刻</th><th>表</th><th>v</th><th>操作</th><th>主な指標</th></tr></thead>
                  <tbody>{[...(st.hist_b ?? []).map((h) => ({ ...h, tb: "bronze" })), ...(st.hist_s ?? []).map((h) => ({ ...h, tb: "silver" }))]
                    .sort((a, b) => tsMs(b.ts) - tsMs(a.ts)).slice(0, 40).map((h) => (
                      <tr key={h.tb + h.version}><td className="mono">{hms(tsMs(h.ts))}</td><td><span className={`chip ${h.tb === "bronze" ? "bz" : "sv"}`}>{h.tb}</span></td>
                        <td>{h.version}</td><td>{h.operation}</td>
                        <td className="small">{Object.entries(h.metrics).filter(([k]) => /^num(OutputRows|TargetRowsInserted|TargetRowsUpdated|TargetRowsDeleted|CopiedRows|RemovedFiles)$/.test(k)).map(([k, v]) => `${k.replace("numTargetRows", "").replace("num", "")}=${v}`).join(" ")}</td></tr>
                    ))}</tbody>
                </table>
              )}
              {tab === "code" && code && (
                <div className="code">
                  <div className="codet">① landing → bronze（{ingest === "al" ? "Auto Loader ノートブック" : "COPY INTO / SQL Warehouse"}）</div>
                  <pre>{code.bronze}</pre>
                  <div className="codet">② bronze → silver（{METHODS.find((x) => x.id === method)!.label}）</div>
                  <pre>{code.silver}</pre>
                  <div className="codet">③ タイミング（ジョブ設定）</div>
                  <pre>{triggerJson(timing, p, boot)}</pre>
                </div>
              )}
            </div>
          </div>
        </section>
      </main>
      {toast && <div className={`toast ${toast.err ? "err" : ""}`}>{toast.msg}</div>}
      {st?.errors && <div className="errs">一部取得エラー: {Object.keys(st.errors).join(", ")}</div>}
    </div>
  );
}

function triggerJson(t: Timing, p: string, boot: Boot | null) {
  const vol = boot ? `/Volumes/${boot.catalog}/${boot.schema}` : "/Volumes/…";
  const ns = boot ? `${boot.catalog}.${boot.schema}` : "…";
  switch (t) {
    case "manual": return `# トリガーなし（必要なときに run-now）\nPOST /api/2.2/jobs/run-now\n{ "job_id": <取り込みジョブ>, "job_parameters": { "stage": "all" } }`;
    case "schedule": return JSON.stringify({ schedule: { quartz_cron_expression: "0 0/2 * * * ?", timezone_id: "Asia/Tokyo", pause_status: "UNPAUSED" } }, null, 2);
    case "file_arrival": return JSON.stringify({ trigger: { pause_status: "UNPAUSED", file_arrival: { url: `${vol}/landing/${p}/`, min_time_between_triggers_seconds: 60 } } }, null, 2);
    case "table_update": return "# silver 反映ジョブ側に設定\n" + JSON.stringify({ trigger: { pause_status: "UNPAUSED", table_update: { table_names: [`${ns}.${p}_bronze`], condition: "ANY_UPDATED", min_time_between_triggers_seconds: 60 } } }, null, 2);
    case "streaming": return p.startsWith("ci") ? "—" : `# 取り込みジョブを mode=stream で起動（ノートブック内で常時ループ）\nPOST /api/2.2/jobs/run-now\n{ "job_id": <取り込みジョブ>, "job_parameters": { "mode": "stream", "stage": "bronze" } }`;
  }
}
