"""データ取り込みパターン・エクスプローラのバックエンド中核。

6 つの実処理パターン（取り込み方式 × 更新方法）を、App のサービスプリンシパルが
自分で作成・所有するテーブル / Volume / ノートブック / ジョブとして構築し、
トリガー（タイミング）の切替・実行・状態の収集を行う。

  landing/{p}/*.json ──(Auto Loader | COPY INTO)──▶ {p}_bronze ──(追記 | 上書き | MERGE)──▶ {p}
"""
from __future__ import annotations

import base64
import io
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient

CATALOG = os.getenv("CATALOG", "classic_stable_ytcy_catalog")
SCHEMA = os.getenv("SCHEMA", "data_eng_pattern")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")
OWNER_USER = os.getenv("OWNER_USER", "")
AUTO_OFF_MIN = int(os.getenv("AUTO_OFF_MIN", "30"))
STREAM_MIN = int(os.getenv("STREAM_MIN", "10"))
APP_TAG = os.getenv("APP_TAG", "data-eng-pattern")

NS = f"{CATALOG}.{SCHEMA}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}"
ASSETS = Path(__file__).parent / "assets"

INGESTS = {"al": "Auto Loader", "ci": "COPY INTO"}
METHODS = {"append": "追記", "full": "フルリフレッシュ", "cdc": "CDC"}
PATTERNS = [f"{i}_{m}" for i in INGESTS for m in METHODS]
TIMINGS = ["manual", "schedule", "file_arrival", "table_update", "streaming"]
SCHEDULE_CRON = "0 0/2 * * * ?"  # 2 分ごと

SRC_COLS = ("order_id INT, customer STRING, item STRING, qty INT, amount DOUBLE, "
            "status STRING, updated_at TIMESTAMP")
BRONZE_COLS = SRC_COLS + ", _op STRING, _seq BIGINT, _file STRING, _ingested_at TIMESTAMP"
SILVER_COLS = SRC_COLS + ", _seq BIGINT, _file STRING, _processed_at TIMESTAMP"
DATA_FIELDS = ["order_id", "customer", "item", "qty", "amount", "status", "updated_at"]

w = WorkspaceClient()
_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=12)
_jobs: dict[str, int] = {}          # "{p}" / "{p}:silver" -> job_id
_ws_root = ""


# ───────────────────────────── SQL ─────────────────────────────

def _convert(v, t):
    if v is None:
        return None
    if t in ("INT", "LONG", "SHORT", "BYTE", "BIGINT"):
        return int(v)
    if t in ("DOUBLE", "FLOAT", "DECIMAL"):
        return float(v)
    if t == "BOOLEAN":
        return v == "true"
    return v


def sql(stmt: str) -> list[dict]:
    r = w.api_client.do("POST", "/api/2.0/sql/statements", body={
        "warehouse_id": WAREHOUSE_ID, "statement": stmt, "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY", "disposition": "INLINE"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(0.5)
        r = w.api_client.do("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"].get("error", {}).get("message", r["status"]["state"]))
    cols = [(c["name"], c["type_name"]) for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = r.get("result", {}).get("data_array") or []
    return [{n: _convert(v, t) for (n, t), v in zip(cols, row)} for row in rows]


def q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


# ─────────────────────── パターン別の処理コード ───────────────────────

def bronze_sql(p: str) -> str:
    """COPY INTO パターンの landing → bronze。"""
    return f"""-- COPY INTO: landing の新しいファイルだけを bronze に取り込む
-- 取り込み済みファイルはテーブル側が記録しているので、再実行しても二重取り込みしない（冪等）
COPY INTO {NS}.{p}_bronze
FROM (
  SELECT CAST(order_id AS INT) AS order_id, customer, item, CAST(qty AS INT) AS qty,
         CAST(amount AS DOUBLE) AS amount, status, CAST(updated_at AS TIMESTAMP) AS updated_at,
         _op, CAST(_seq AS BIGINT) AS _seq,
         _metadata.file_name AS _file, current_timestamp() AS _ingested_at
  FROM '{VOL}/landing/{p}/'
)
FILEFORMAT = JSON
"""


def silver_sql(p: str) -> str:
    """bronze → silver（更新方法ごとに異なる）。"""
    b, s = f"{NS}.{p}_bronze", f"{NS}.{p}"
    cols = ", ".join(DATA_FIELDS)
    method = p.split("_", 1)[1]
    if method == "append":
        return f"""-- 追記: まだ silver に入っていないファイルの行をそのまま追加する
-- 新規レコードのみを前提とするため、ソース側の更新・削除は反映されない
INSERT INTO {s}
SELECT {cols}, _seq, _file, current_timestamp() AS _processed_at
FROM {b}
WHERE _file NOT IN (SELECT DISTINCT _file FROM {s})
"""
    if method == "full":
        return f"""-- フルリフレッシュ: 最新スナップショットのファイルで silver 全体を置き換える
-- 削除も含めてソースと完全一致するが、毎回全件を書き換える
INSERT OVERWRITE {s}
SELECT {cols}, _seq, _file, current_timestamp() AS _processed_at
FROM {b}
WHERE _file = (SELECT max(_file) FROM {b})
"""
    sets = ", ".join(f"t.{c} = c.{c}" for c in DATA_FIELDS[1:])
    return f"""-- CDC: 変更レコード（INSERT/UPDATE/DELETE と順序キー _seq）を MERGE で適用する
-- キーごとに最新の変更だけを取り、_seq が新しい場合のみ反映するので再実行しても安全（冪等）
MERGE INTO {s} AS t
USING (
  SELECT * FROM {b}
  QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _seq DESC) = 1
) AS c
ON t.order_id = c.order_id
WHEN MATCHED AND c._seq > t._seq AND c._op = 'DELETE' THEN DELETE
WHEN MATCHED AND c._seq > t._seq THEN UPDATE SET
  {sets}, t._seq = c._seq, t._file = c._file, t._processed_at = current_timestamp()
WHEN NOT MATCHED AND c._op <> 'DELETE' THEN INSERT ({cols}, _seq, _file, _processed_at)
  VALUES ({", ".join("c." + c for c in DATA_FIELDS)}, c._seq, c._file, current_timestamp())
"""


def notebook_source() -> str:
    return (ASSETS / "autoloader_ingest.txt").read_text()


def code_for(p: str) -> dict:
    ingest = p.split("_")[0]
    return {
        "bronze": notebook_source() if ingest == "al" else bronze_sql(p),
        "bronze_lang": "python" if ingest == "al" else "sql",
        "silver": silver_sql(p),
    }


# ───────────────────────────── Jobs ─────────────────────────────

def _job_name(p: str, silver: bool) -> str:
    return f"[{APP_TAG}] {p}" + (" silver-on-table-update" if silver else "")


def _job_settings(p: str, silver: bool) -> dict:
    root = _ws_root
    sql_task = lambda f: {"warehouse_id": WAREHOUSE_ID, "file": {"path": f"{root}/sql/{f}", "source": "WORKSPACE"}}
    silver_task = {"task_key": "apply_silver", "sql_task": sql_task(f"{p}_silver.sql")}
    base = {"name": _job_name(p, silver), "max_concurrent_runs": 1, "queue": {"enabled": True},
            "tags": {"app": APP_TAG, "pattern": p}}
    if silver:
        return {**base, "tasks": [silver_task]}
    if p.startswith("al_"):
        ingest = {"task_key": "ingest_bronze", "notebook_task": {
            "notebook_path": f"{root}/autoloader_ingest", "source": "WORKSPACE",
            "base_parameters": {"pattern": p, "mode": "{{job.parameters.mode}}",
                                "catalog": CATALOG, "schema": SCHEMA,
                                "duration_min": "{{job.parameters.duration_min}}",
                                "silver_sql": f"{root}/sql/{p}_silver.sql"}}}
    else:
        ingest = {"task_key": "ingest_bronze", "sql_task": sql_task(f"{p}_bronze.sql")}
    return {**base,
            "parameters": [{"name": "mode", "default": "batch"}, {"name": "stage", "default": "all"},
                           {"name": "duration_min", "default": str(STREAM_MIN)}, {"name": "origin", "default": "job"}],
            "tasks": [ingest,
                      {"task_key": "check_stage", "depends_on": [{"task_key": "ingest_bronze"}],
                       "condition_task": {"op": "EQUAL_TO", "left": "{{job.parameters.stage}}", "right": "all"}},
                      {**silver_task, "depends_on": [{"task_key": "check_stage", "outcome": "true"}]}]}


def _my_jobs() -> list[dict]:
    out, token = [], None
    while True:
        r = w.api_client.do("GET", "/api/2.2/jobs/list", query={"limit": 100, **({"page_token": token} if token else {})})
        out += [j for j in r.get("jobs", []) or [] if j["settings"].get("tags", {}).get("app") == APP_TAG]
        token = r.get("next_page_token")
        if not token:
            return out


def _ensure_jobs():
    existing = {j["settings"]["name"]: j["job_id"] for j in _my_jobs()}
    for p in PATTERNS:
        for silver in (False, True):
            name, key = _job_name(p, silver), p + (":silver" if silver else "")
            settings = _job_settings(p, silver)
            if name in existing:
                jid = existing[name]
                w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": jid, "new_settings": settings})
            else:
                jid = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
            _jobs[key] = jid
            if OWNER_USER:
                w.api_client.do("PATCH", f"/api/2.0/permissions/jobs/{jid}", body={
                    "access_control_list": [{"user_name": OWNER_USER, "permission_level": "CAN_MANAGE"}]})


def _ensure_jobs_loaded():
    if len(_jobs) < len(PATTERNS) * 2:
        for j in _my_jobs():
            tags = j["settings"].get("tags", {})
            if tags.get("pattern") in PATTERNS:
                _jobs[tags["pattern"] + (":silver" if "silver-on" in j["settings"]["name"] else "")] = j["job_id"]


def _job_get(jid: int) -> dict:
    return w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": jid})["settings"]


def _job_update(jid: int, new: dict | None = None, remove: list[str] | None = None):
    w.api_client.do("POST", "/api/2.2/jobs/update", body={
        "job_id": jid, "new_settings": new or {}, "fields_to_remove": remove or []})


def _active_runs(jid: int) -> list[dict]:
    return w.api_client.do("GET", "/api/2.2/jobs/runs/list",
                           query={"job_id": jid, "active_only": "true"}).get("runs", []) or []


def _cancel_active(jid: int):
    for r in _active_runs(jid):
        w.api_client.do("POST", "/api/2.2/jobs/runs/cancel", body={"run_id": r["run_id"]})


def current_timing(p: str) -> dict:
    a, b = _job_get(_jobs[p]), _job_get(_jobs[p + ":silver"])
    tags = a.get("tags", {})
    timing = "manual"
    if a.get("schedule", {}).get("pause_status") == "UNPAUSED":
        timing = "schedule"
    elif a.get("trigger", {}).get("file_arrival") and a["trigger"].get("pause_status") != "PAUSED":
        timing = "file_arrival"
    elif b.get("trigger", {}).get("table_update") and b["trigger"].get("pause_status") != "PAUSED":
        timing = "table_update"
    if tags.get("timing") == "streaming":
        timing = "streaming"
    return {"timing": timing, "enabled_at": tags.get("enabled_at"),
            "schedule": a.get("schedule"), "trigger": a.get("trigger") or b.get("trigger"),
            "job_id": _jobs[p], "silver_job_id": _jobs[p + ":silver"]}


def set_timing(p: str, timing: str) -> dict:
    if timing not in TIMINGS:
        raise ValueError(timing)
    if timing == "streaming" and not p.startswith("al_"):
        raise ValueError("COPY INTO はバッチ専用のため Streaming は選べません")
    a, b = _jobs[p], _jobs[p + ":silver"]
    tags = {"app": APP_TAG, "pattern": p, "timing": timing, "enabled_at": str(int(time.time()))}
    # いったん全トリガーを外してから、選ばれたタイミングだけを有効化する
    _job_update(b, remove=["trigger"])
    _job_update(a, {"tags": tags}, remove=["schedule", "trigger"])
    _cancel_active(a)
    if timing == "schedule":
        _job_update(a, {"schedule": {"quartz_cron_expression": SCHEDULE_CRON,
                                     "timezone_id": "Asia/Tokyo", "pause_status": "UNPAUSED"}})
    elif timing == "file_arrival":
        _watch[p] = {"seen": _landing_names(p), "since": None}
        _job_update(a, {"trigger": {"pause_status": "UNPAUSED", "file_arrival": {
            "url": f"{VOL}/landing/{p}/", "min_time_between_triggers_seconds": 60}}})
    elif timing == "table_update":
        _job_update(b, {"trigger": {"pause_status": "UNPAUSED", "table_update": {
            "table_names": [f"{NS}.{p}_bronze"], "condition": "ANY_UPDATED",
            "min_time_between_triggers_seconds": 60}}})
    elif timing == "streaming":
        run_now(p, mode="stream", stage="bronze")
    log_events([(p, "timing_set", {"timing": timing})])
    return current_timing(p)


def run_now(p: str, mode: str = "batch", stage: str = "all", origin: str = "app") -> int:
    rid = w.api_client.do("POST", "/api/2.2/jobs/run-now", body={
        "job_id": _jobs[p], "job_parameters": {"mode": mode, "stage": stage, "duration_min": str(STREAM_MIN),
                                               "origin": origin}})["run_id"]
    log_events([(p, "run_requested", {"mode": mode, "stage": stage, "run_id": rid, "origin": origin})])
    return rid


# ファイル到着の代行監視: ジョブの file_arrival トリガーが一定時間内に起動しなかった場合だけ、アプリが代わりに起動する
FA_GRACE_SEC = int(os.getenv("FA_GRACE_SEC", "75"))
_watch: dict[str, dict] = {}


def _landing_names(p: str) -> set[str]:
    return {e.name for e in w.files.list_directory_contents(f"{VOL}/landing/{p}") if not e.is_directory}


def file_watch_loop():
    while True:
        time.sleep(15)
        try:
            _ensure_jobs_loaded()
            for p in PATTERNS:
                if current_timing(p)["timing"] != "file_arrival":
                    _watch.pop(p, None)
                    continue
                names = _landing_names(p)
                st = _watch.setdefault(p, {"seen": names, "since": None})
                if names - st["seen"] and st["since"] is None:
                    st["since"] = time.time()
                if st["since"] is None:
                    continue
                runs = w.api_client.do("GET", "/api/2.2/jobs/runs/list",
                                       query={"job_id": _jobs[p], "limit": 5}).get("runs", []) or []
                if any(r.get("start_time", 0) >= st["since"] * 1000 - 5000 for r in runs):
                    st.update(seen=names, since=None)  # ジョブのトリガー（または誰か）が起動済み
                elif time.time() - st["since"] >= FA_GRACE_SEC:
                    run_now(p, origin="app_watcher")
                    st.update(seen=names, since=None)
        except Exception as e:  # noqa: BLE001
            print("file_watch_loop:", e)


def stop_all():
    for p in PATTERNS:
        a, b = _jobs[p], _jobs[p + ":silver"]
        _job_update(b, remove=["trigger"])
        _job_update(a, {"tags": {"app": APP_TAG, "pattern": p, "timing": "manual"}}, remove=["schedule", "trigger"])
        _cancel_active(a)
        _cancel_active(b)
    log_events([("*", "all_stopped", {})])


def auto_off_loop():
    """トリガーを有効にしたまま放置しても課金が続かないよう、一定時間で手動に戻す。"""
    while True:
        time.sleep(60)
        try:
            _ensure_jobs_loaded()
            for p in PATTERNS:
                t = current_timing(p)
                if t["timing"] in ("schedule", "file_arrival", "table_update") and t["enabled_at"]:
                    if time.time() - int(t["enabled_at"]) > AUTO_OFF_MIN * 60:
                        set_timing(p, "manual")
                        log_events([(p, "auto_off", {"after_min": AUTO_OFF_MIN})])
                if t["timing"] == "streaming" and not _active_runs(_jobs[p]):
                    _job_update(_jobs[p], {"tags": {"app": APP_TAG, "pattern": p, "timing": "manual"}})
        except Exception as e:  # noqa: BLE001
            print("auto_off_loop:", e)


# ─────────────────────── ソースシステム（疑似） ───────────────────────

CUSTOMERS = ["田中商店", "佐藤物産", "鈴木工業", "高橋食品", "伊藤電機", "渡辺建設", "山本薬局", "中村書店"]
ITEMS = {"ノートPC": 148000, "モニター": 32800, "キーボード": 9800, "マウス": 4200,
         "ヘッドセット": 12800, "ドック": 21800, "Webカメラ": 8900, "SSD 1TB": 15800}
STATUS_FLOW = ["受注", "出荷準備", "出荷済", "配達完了"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_order(oid: int, ts: str) -> dict:
    item = random.choice(list(ITEMS))
    qty = random.randint(1, 5)
    return {"order_id": oid, "customer": random.choice(CUSTOMERS), "item": item, "qty": qty,
            "amount": float(ITEMS[item] * qty), "status": "受注", "updated_at": ts}


def read_source() -> list[dict]:
    return sql(f"SELECT {', '.join(DATA_FIELDS)} FROM {NS}.source_orders ORDER BY order_id")


def _write_source(rows: list[dict]):
    if not rows:
        sql(f"TRUNCATE TABLE {NS}.source_orders")
        return
    vals = ",\n".join("(" + ", ".join([q(r["order_id"]), q(r["customer"]), q(r["item"]), q(r["qty"]),
                                       q(r["amount"]), q(r["status"])]) + f", TIMESTAMP {q(r['updated_at'])})"
                      for r in rows)
    sql(f"INSERT OVERWRITE {NS}.source_orders VALUES {vals}")


def _upload(path: str, lines: list[dict]):
    data = ("\n".join(json.dumps(r, ensure_ascii=False) for r in lines) + "\n").encode()
    w.files.upload(path, io.BytesIO(data), overwrite=True)


def change_source(inserts: int = 3, updates: int = 2, deletes: int = 1, initial: bool = False) -> dict:
    """ソース DB を変更し、各パターンのランディングへ「そのパターン用のエクスポート」を書き出す。

    追記パターン → 新規レコードのみ / フルリフレッシュ → 全件スナップショット / CDC → 変更レコード
    """
    with _lock:
        rows = [] if initial else read_source()
        if not initial:
            for r in rows:  # 型を揃える（Statement API は TIMESTAMP を文字列で返す）
                r["updated_at"] = str(r["updated_at"]).replace(" ", "T")
        by_id = {r["order_id"]: r for r in rows}
        now = _now()
        ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        seq0 = time.time_ns() // 1000
        changes: list[dict] = []
        movable = [r for r in rows if r["status"] != STATUS_FLOW[-1]]
        updated = set()
        for r in random.sample(movable, min(updates, len(movable))):
            r["status"] = STATUS_FLOW[STATUS_FLOW.index(r["status"]) + 1]
            r["updated_at"] = ts
            updated.add(r["order_id"])
            changes.append({**r, "_op": "UPDATE"})
        rest = [r for r in rows if r["order_id"] not in updated]
        for r in random.sample(rest, min(deletes, len(rest))):
            by_id.pop(r["order_id"])
            changes.append({**r, "updated_at": ts, "_op": "DELETE"})
        next_id = max([1000, *by_id.keys(), *[c["order_id"] for c in changes]]) + 1
        for i in range(inserts):
            o = _new_order(next_id + i, ts)
            by_id[o["order_id"]] = o
            changes.append({**o, "_op": "INSERT"})
        for i, c in enumerate(changes):
            c["_seq"] = seq0 + i
        new_rows = sorted(by_id.values(), key=lambda r: r["order_id"])
        _write_source(new_rows)

        fname = f"orders_{now.strftime('%Y%m%dT%H%M%S')}_{now.microsecond // 1000:03d}.json"
        exports = {}
        for p in PATTERNS:
            method = p.split("_", 1)[1]
            if method == "append":
                lines = [c for c in changes if c["_op"] == "INSERT"]
            elif method == "full":
                lines = [{**r, "_op": "SNAPSHOT", "_seq": seq0} for r in new_rows]
            else:
                lines = changes
            if lines:
                exports[p] = lines
        list(_pool.map(lambda kv: _upload(f"{VOL}/landing/{kv[0]}/{fname}", kv[1]), exports.items()))
        ops = {op: sum(1 for c in changes if c["_op"] == op) for op in ("INSERT", "UPDATE", "DELETE")}
        log_events([("*", "source_change", {"file": fname, **ops, "source_rows": len(new_rows)})] +
                   [(p, "file_landed", {"file": fname, "rows": len(v)}) for p, v in exports.items()])
        return {"file": fname, "ops": ops, "source_rows": len(new_rows)}


# ───────────────────────────── ログ ─────────────────────────────

def log_events(events: list[tuple[str, str, dict]]):
    ts = _now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    vals = ", ".join(f"(TIMESTAMP '{ts}', {q(p)}, {q(e)}, {q(json.dumps(d, ensure_ascii=False))})"
                     for p, e, d in events)
    try:
        sql(f"INSERT INTO {NS}.event_log VALUES {vals}")
    except Exception as e:  # noqa: BLE001
        print("log_events:", e)


# ─────────────────────────── 初期化 ───────────────────────────

def _ws_import(path: str, content: str, notebook: bool = False):
    body = {"path": path, "overwrite": True, "content": base64.b64encode(content.encode()).decode()}
    body.update({"format": "SOURCE", "language": "PYTHON"} if notebook else {"format": "AUTO"})
    w.api_client.do("POST", "/api/2.0/workspace/import", body=body)


def _create_tables(p: str):
    sql(f"CREATE TABLE IF NOT EXISTS {NS}.{p}_bronze ({BRONZE_COLS}) COMMENT '{p}: landing から取り込んだ生データ'")
    sql(f"CREATE TABLE IF NOT EXISTS {NS}.{p} ({SILVER_COLS}) COMMENT '{p}: 更新方法を適用した結果'")


def _rm_tree(path: str):
    try:
        for e in w.files.list_directory_contents(path):
            if e.is_directory:
                _rm_tree(e.path)
            else:
                w.files.delete(e.path)
        w.files.delete_directory(path)
    except Exception as e:  # noqa: BLE001
        if "NOT_FOUND" not in str(e) and "does not exist" not in str(e).lower():
            print("rm_tree:", path, e)


def bootstrap():
    global _ws_root
    me = w.current_user.me().user_name
    _ws_root = f"/Workspace/Users/{me}/data_eng_pattern"
    sql(f"CREATE VOLUME IF NOT EXISTS {NS}.landing COMMENT 'ソースシステムがエクスポートしたファイルの着地点'")
    sql(f"CREATE VOLUME IF NOT EXISTS {NS}.checkpoints COMMENT 'Auto Loader のチェックポイント'")
    sql(f"CREATE TABLE IF NOT EXISTS {NS}.source_orders ({SRC_COLS}) COMMENT '疑似ソースシステム（業務DB）の現在の状態'")
    sql(f"CREATE TABLE IF NOT EXISTS {NS}.event_log (ts TIMESTAMP, pattern STRING, event STRING, detail STRING) "
        "COMMENT 'アプリが記録したイベント（ファイル着地・タイミング変更など）'")
    w.api_client.do("POST", "/api/2.0/workspace/mkdirs", body={"path": f"{_ws_root}/sql"})
    _ws_import(f"{_ws_root}/autoloader_ingest", notebook_source(), notebook=True)
    for p in PATTERNS:
        _create_tables(p)
        w.files.create_directory(f"{VOL}/landing/{p}")
        if p.startswith("ci_"):
            _ws_import(f"{_ws_root}/sql/{p}_bronze.sql", bronze_sql(p))
        _ws_import(f"{_ws_root}/sql/{p}_silver.sql", silver_sql(p))
    if OWNER_USER:
        oid = w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": _ws_root})["object_id"]
        w.api_client.do("PATCH", f"/api/2.0/permissions/directories/{oid}", body={
            "access_control_list": [{"user_name": OWNER_USER, "permission_level": "CAN_MANAGE"}]})
    _ensure_jobs()
    if not sql(f"SELECT 1 FROM {NS}.source_orders LIMIT 1"):
        change_source(inserts=12, updates=0, deletes=0, initial=True)


def reset_all():
    stop_all()
    for p in PATTERNS:
        sql(f"DROP TABLE IF EXISTS {NS}.{p}_bronze")
        sql(f"DROP TABLE IF EXISTS {NS}.{p}")
    list(_pool.map(_rm_tree, [f"{VOL}/{d}/{p}" for d in ("landing", "checkpoints") for p in PATTERNS]))
    for p in PATTERNS:
        _create_tables(p)
        w.files.create_directory(f"{VOL}/landing/{p}")
    sql(f"TRUNCATE TABLE {NS}.event_log")
    _ensure_jobs()  # table_update トリガーは DROP で無効になるため作り直す
    change_source(inserts=12, updates=0, deletes=0, initial=True)
    log_events([("*", "reset", {})])


# ─────────────────────────── 状態の収集 ───────────────────────────

def _runs(jid: int, kind: str, since_ms: int) -> list[dict]:
    out = []
    res = w.api_client.do("GET", "/api/2.2/jobs/runs/list",
                          query={"job_id": jid, "limit": 25, "expand_tasks": "true"}).get("runs", []) or []
    for r in res:
        if r.get("start_time", 0) < since_ms and r.get("end_time", 1) and r.get("end_time", 1) < since_ms:
            continue
        st = r.get("state", {})
        params = {x["name"]: x.get("value", x.get("default")) for x in r.get("job_parameters", []) or []}
        out.append({
            "run_id": r["run_id"], "job": kind, "trigger": r.get("trigger"),
            "start": r.get("start_time"), "end": r.get("end_time") or None,
            "life": st.get("life_cycle_state"), "result": st.get("result_state"),
            "message": st.get("state_message"), "url": r.get("run_page_url"), "params": params,
            "tasks": [{"key": t["task_key"], "start": t.get("start_time"), "end": t.get("end_time") or None,
                       "life": t.get("state", {}).get("life_cycle_state"),
                       "result": t.get("state", {}).get("result_state")} for t in r.get("tasks", [])],
        })
    return out


def _history(table: str) -> list[dict]:
    rows = sql(f"SELECT version, CAST(timestamp AS STRING) AS ts, operation, "
               f"to_json(operationMetrics) AS metrics, to_json(operationParameters) AS params "
               f"FROM (DESCRIBE HISTORY {table} LIMIT 40)")
    for r in rows:
        r["metrics"] = json.loads(r["metrics"] or "{}")
        r["params"] = json.loads(r["params"] or "{}")
    return rows


def _landing(p: str) -> list[dict]:
    try:
        return sorted([{"name": e.name, "size": e.file_size, "modified": e.last_modified}
                       for e in w.files.list_directory_contents(f"{VOL}/landing/{p}") if not e.is_directory],
                      key=lambda x: x["name"])
    except Exception:  # noqa: BLE001
        return []


def _compare(p: str) -> list[dict]:
    """ソースと silver を突合して、行ごとの一致状態を返す。"""
    return sql(f"""
SELECT coalesce(s.order_id, t.order_id) AS order_id,
       t.customer, t.item, t.qty, t.amount, t.status, CAST(t.updated_at AS STRING) AS updated_at,
       t._file, CAST(t._processed_at AS STRING) AS processed_at, t.dup,
       s.status AS src_status, s.qty AS src_qty,
       CASE WHEN t.order_id IS NULL THEN 'missing'
            WHEN s.order_id IS NULL THEN 'deleted_at_source'
            WHEN s.status <=> t.status AND s.qty <=> t.qty AND s.amount <=> t.amount THEN 'same'
            ELSE 'stale' END AS sync
FROM {NS}.source_orders s
FULL OUTER JOIN (
  SELECT *, count(*) OVER (PARTITION BY order_id) AS dup FROM {NS}.{p}
  QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _processed_at DESC) = 1
) t ON s.order_id = t.order_id
ORDER BY order_id DESC
""")


def overview() -> dict:
    """マトリクス表示用に、6 パターン全体の一致率・稼働状況をまとめて返す。"""
    _ensure_jobs_loaded()
    parts = " UNION ALL ".join(
        f"SELECT '{p}' AS p, count(*) AS total, count_if(s.order_id IS NOT NULL AND t.order_id IS NOT NULL "
        f"AND s.status <=> t.status AND s.qty <=> t.qty) AS same, "
        f"(SELECT count(*) FROM {NS}.{p}_bronze) AS bronze_rows, (SELECT count(*) FROM {NS}.{p}) AS silver_rows "
        f"FROM {NS}.source_orders s FULL OUTER JOIN "
        f"(SELECT DISTINCT order_id, status, qty FROM {NS}.{p}) t ON s.order_id = t.order_id"
        for p in PATTERNS)
    fut_sql = _pool.submit(sql, parts)
    fut_t = {p: _pool.submit(current_timing, p) for p in PATTERNS}
    fut_a = {p: _pool.submit(lambda p=p: bool(_active_runs(_jobs[p]) or _active_runs(_jobs[p + ':silver'])))
             for p in PATTERNS}
    stats = {r["p"]: r for r in fut_sql.result()}
    return {"patterns": {p: {**stats.get(p, {}), "timing": fut_t[p].result()["timing"],
                             "active": fut_a[p].result()} for p in PATTERNS}}


def state(p: str, minutes: int = 20) -> dict:
    _ensure_jobs_loaded()
    since = time.time() - minutes * 60
    since_ms = int(since * 1000)
    since_ts = datetime.fromtimestamp(since, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    f = {
        "timing": _pool.submit(current_timing, p),
        "runs_a": _pool.submit(_runs, _jobs[p], "ingest", since_ms),
        "runs_b": _pool.submit(_runs, _jobs[p + ":silver"], "silver", since_ms),
        "hist_b": _pool.submit(_history, f"{NS}.{p}_bronze"),
        "hist_s": _pool.submit(_history, f"{NS}.{p}"),
        "events": _pool.submit(sql, f"SELECT CAST(ts AS STRING) AS ts, pattern, event, detail FROM {NS}.event_log "
                                    f"WHERE (pattern = '{p}' OR pattern = '*') AND ts >= TIMESTAMP '{since_ts}' ORDER BY ts"),
        "compare": _pool.submit(_compare, p),
        "bronze_files": _pool.submit(sql, f"SELECT _file, count(*) AS rows, CAST(min(_ingested_at) AS STRING) AS ingested_at, "
                                          f"concat_ws(',', collect_set(_op)) AS ops FROM {NS}.{p}_bronze GROUP BY _file ORDER BY _file DESC LIMIT 40"),
        "bronze_rows": _pool.submit(sql, f"SELECT order_id, customer, item, qty, amount, status, _op, _seq, _file, "
                                         f"CAST(_ingested_at AS STRING) AS ingested_at FROM {NS}.{p}_bronze "
                                         f"ORDER BY _ingested_at DESC, _seq DESC LIMIT 40"),
        "landing": _pool.submit(_landing, p),
    }
    out = {}
    for k, v in f.items():
        try:
            out[k] = v.result()
        except Exception as e:  # noqa: BLE001
            out[k] = None
            out.setdefault("errors", {})[k] = str(e)[:300]
    out["runs"] = (out.pop("runs_a") or []) + (out.pop("runs_b") or [])
    out["now"] = int(time.time() * 1000)
    out["host"] = w.config.host
    return out
