"""HTTP API smoke tests."""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("EVENT_ENGINE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("EVENT_ENGINE_BATCH_SIZE", "2")
    import importlib

    import main
    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def test_ingest_query_rebuild_skipped(client):
    for i in range(5):
        r = client.post("/events", json={
            "id": f"e{i}", "ts": float(i), "type": "t",
            "key": f"k{i % 2}", "value": float(i)})
        assert r.status_code == 202
    # duplicate + invalid
    client.post("/events", json={"id": "e0", "ts": 0.0, "type": "t",
                                 "key": "k0", "value": 0.0})
    client.post("/events", json={"id": "bad", "ts": 0.0, "type": "t",
                                 "key": "k0", "value": 1e13})

    import time
    deadline = time.monotonic() + 5.0
    while True:
        status = {s["name"]: s for s in client.get("/status").json()}
        if all(s["pending_events"] == 0 for s in status.values()):
            break
        assert time.monotonic() < deadline
        time.sleep(0.02)

    r = client.get("/views/count_by_key")
    assert r.status_code == 200
    assert r.json()["state"] == {"k0": 4, "k1": 2}

    skipped = client.get("/views/sum_by_key/skipped").json()
    reasons = {s["event_id"]: s["reason"] for s in skipped}
    assert reasons == {"e0": "duplicate", "bad": "out_of_range"}

    r = client.post("/views/sum_by_key/rebuild")
    assert r.status_code == 200
    assert r.json()["state"] == {"k0": 6.0, "k1": 4.0}

    assert client.get("/views/nope").status_code == 404
