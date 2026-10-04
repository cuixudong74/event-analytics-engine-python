"""HTTP API for the event analytics engine."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from event_engine.engine import BackpressureError, Engine
from event_engine.models import EngineConfig
from event_engine.views import CountByKeyView, SumByKeyView


def build_engine() -> Engine:
    config = EngineConfig(
        data_dir=os.environ.get("EVENT_ENGINE_DATA_DIR", ".event_engine"),
        batch_size=int(os.environ.get("EVENT_ENGINE_BATCH_SIZE", "100")),
        queue_capacity=int(os.environ.get("EVENT_ENGINE_QUEUE_CAPACITY", "10000")),
    )
    engine = Engine([CountByKeyView(), SumByKeyView()], config)
    engine.start()
    return engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = build_engine()
    app.state.engine = engine
    try:
        yield
    finally:
        engine.close()


app = FastAPI(lifespan=lifespan)


class EventIn(BaseModel):
    id: str
    ts: float
    type: str
    key: str
    value: float


@app.post("/events", status_code=202)
def submit_event(body: EventIn) -> dict[str, Any]:
    try:
        ev = app.state.engine.submit(body.id, body.ts, body.type, body.key, body.value)
    except BackpressureError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    return {"seq": ev.seq}


@app.get("/views/{name}")
def get_view(name: str) -> dict[str, Any]:
    snap = app.state.engine.snapshot(name)
    if snap is None:
        raise HTTPException(status_code=404, detail=f"unknown view {name!r}")
    return {"view": snap.view, "offset": snap.offset, "state": snap.state,
            "committed_at": snap.committed_at}


@app.get("/views/{name}/skipped")
def get_skipped(name: str) -> list[dict[str, Any]]:
    return [
        {"event_id": s.event_id, "seq": s.seq, "reason": s.reason.value,
         "detail": s.detail}
        for s in app.state.engine.skipped(name)
    ]


@app.post("/views/{name}/rebuild")
def rebuild_view(name: str) -> dict[str, Any]:
    try:
        snap = app.state.engine.rebuild(name)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown view {name!r}")
    return {"view": snap.view, "offset": snap.offset, "state": snap.state}


@app.get("/status")
def get_status() -> list[dict[str, Any]]:
    return [vars(s) for s in app.state.engine.status()]


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "event-analytics-engine"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
