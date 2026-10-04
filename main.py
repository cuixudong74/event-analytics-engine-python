"""HTTP 接口：事件接入、视图查询、全量重建、进度与跳过记录。"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException

from engine.engine import BackpressureError, Engine, EngineConfig
from engine.projections import CountByKind, LatestByKey, SumByKey

config = EngineConfig(
    batch_size=int(os.getenv("ENGINE_BATCH_SIZE", "100")),
    max_queue=int(os.getenv("ENGINE_MAX_QUEUE", "10000")),
    apply_timeout_s=float(os.getenv("ENGINE_APPLY_TIMEOUT_S", "30")),
    data_dir=Path(os.getenv("ENGINE_DATA_DIR", "./data")),
)
engine = Engine(config)
for proj in (CountByKind(), SumByKey(), LatestByKey()):
    engine.register_view(proj)

app = FastAPI(title="event-analytics-engine")


@app.on_event("shutdown")
def _shutdown() -> None:
    engine.close()


@app.post("/events")
def ingest(event: dict[str, Any]) -> dict[str, int]:
    try:
        offset = engine.ingest(event)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except BackpressureError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    return {"offset": offset}


@app.get("/views/{view}")
def query(view: str) -> dict[str, Any]:
    try:
        snap = engine.query(view)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"view": snap.view, "offset": snap.offset, "state": snap.state}


@app.get("/views/{view}/skips")
def skips(view: str) -> list[dict[str, Any]]:
    try:
        records = engine.query_skips(view)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return [
        {"event_id": s.event.event_id, "view": s.view, "reason": s.reason.value, "detail": s.detail}
        for s in records
    ]


@app.post("/views/{view}/rebuild")
def rebuild(view: str) -> dict[str, str]:
    try:
        engine.rebuild(view)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"status": "rebuilt", "view": view}


@app.get("/progress")
def progress() -> dict[str, dict[str, int]]:
    return engine.progress()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
