import os
import threading
import traceback
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import core

app = FastAPI(title="Data Ingestion Pattern Explorer")
STATIC = Path(__file__).parent / "static"
_boot = {"status": "starting", "error": None}


def _bootstrap():
    try:
        core.bootstrap()
        _boot["status"] = "ready"
        threading.Thread(target=core.auto_off_loop, daemon=True).start()
        threading.Thread(target=core.file_watch_loop, daemon=True).start()
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        _boot.update(status="error", error=str(e))


@app.on_event("startup")
def _startup():
    threading.Thread(target=_bootstrap, daemon=True).start()


def _guard(p: str | None = None):
    if _boot["status"] != "ready":
        raise HTTPException(503, f"初期化中です（{_boot['status']}）{_boot['error'] or ''}")
    if p is not None and p not in core.PATTERNS:
        raise HTTPException(404, p)


@app.exception_handler(Exception)
async def _err(_, exc: Exception):
    traceback.print_exc()
    return JSONResponse({"detail": str(exc)[:500]}, status_code=500)


@app.get("/api/boot")
def boot():
    return {**_boot, "catalog": core.CATALOG, "schema": core.SCHEMA, "auto_off_min": core.AUTO_OFF_MIN,
            "stream_min": core.STREAM_MIN}


@app.get("/api/overview")
def overview():
    _guard()
    return core.overview()


@app.get("/api/state/{p}")
def state(p: str, minutes: int = 20):
    _guard(p)
    return core.state(p, minutes)


@app.get("/api/code/{p}")
def code(p: str):
    _guard(p)
    return core.code_for(p)


class Change(BaseModel):
    inserts: int = 3
    updates: int = 2
    deletes: int = 1


@app.post("/api/source/change")
def change(c: Change):
    _guard()
    return core.change_source(max(0, min(c.inserts, 20)), max(0, min(c.updates, 20)), max(0, min(c.deletes, 20)))


class Timing(BaseModel):
    timing: str


@app.post("/api/timing/{p}")
def timing(p: str, t: Timing):
    _guard(p)
    try:
        return core.set_timing(p, t.timing)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


class Run(BaseModel):
    stage: str = "all"


@app.post("/api/run/{p}")
def run(p: str, r: Run):
    _guard(p)
    return {"run_id": core.run_now(p, stage="bronze" if r.stage == "bronze" else "all")}


@app.post("/api/stop")
def stop():
    _guard()
    core.stop_all()
    return {"ok": True}


@app.post("/api/reset")
def reset():
    _guard()
    core.reset_all()
    return {"ok": True}


if STATIC.exists():
    app.mount("/assets", StaticFiles(directory=STATIC / "assets"), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        f = STATIC / path
        return FileResponse(f if path and f.is_file() else STATIC / "index.html")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8000")))
