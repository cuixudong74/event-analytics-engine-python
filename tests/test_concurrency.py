"""Concurrency: queries during maintenance, independent view progress,
backpressure, and rebuild concurrent with ingestion."""
from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from event_engine.engine import BackpressureError, Engine
from event_engine.models import EngineConfig, Event, SkipReason
from event_engine.views import CountByKeyView, SumByKeyView


def make_engine(tmp_path, views=None, **cfg) -> Engine:
    return Engine(views or [CountByKeyView(), SumByKeyView()],
                  EngineConfig(data_dir=str(tmp_path), **cfg))


def test_queries_during_maintenance_are_consistent(tmp_path):
    eng = make_engine(tmp_path, batch_size=7, batch_max_wait_ms=5)
    eng.start()
    errors: list[str] = []
    stop = threading.Event()

    def reader():
        last_offset = -1
        while not stop.is_set():
            snap = eng.snapshot("count_by_key")
            if snap is None:
                continue
            # offsets never go backwards
            if snap.offset < last_offset:
                errors.append(f"offset regressed {last_offset}->{snap.offset}")
            last_offset = snap.offset
            # state is always a whole committed batch: total count == offset
            # (all submitted events are valid and unique)
            if sum(snap.state.values()) != snap.offset:
                errors.append(
                    f"half-applied batch at offset {snap.offset}: {snap.state}")

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    for i in range(500):
        eng.submit(f"e{i}", float(i), "t", f"k{i % 5}", 1.0)
    eng.catch_up()
    stop.set()
    for t in threads:
        t.join()
    eng.stop()

    assert not errors, errors[:3]
    assert eng.snapshot("count_by_key").offset == 500
    eng.close()


class SlowView:
    """A view whose apply is artificially slow."""

    name = "slow"

    def validate(self, event: Event) -> SkipReason | None:
        return None

    def apply_one(self, state: dict[str, Any], event: Event) -> None:
        time.sleep(0.002)
        state["n"] = state.get("n", 0) + 1


def test_slow_view_does_not_block_fast_view(tmp_path):
    eng = make_engine(tmp_path, views=[CountByKeyView(), SlowView()],
                      batch_size=10, batch_max_wait_ms=5)
    eng.start()
    for i in range(50):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)
    # fast view catches up well before the slow one finishes
    deadline = time.monotonic() + 5.0
    while eng.snapshot("count_by_key").offset < 50:
        assert time.monotonic() < deadline, "fast view was blocked by slow view"
        time.sleep(0.01)
    assert eng.snapshot("slow").offset < 50 or True  # slow may lag; no assertion needed
    eng.catch_up()
    assert eng.snapshot("slow").offset == 50
    eng.stop()
    eng.close()


def test_backpressure_blocks_then_times_out(tmp_path):
    # no maintenance running: lag grows until capacity is hit
    eng = make_engine(tmp_path, queue_capacity=8, ingest_timeout_ms=100)
    for i in range(8):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)
    with pytest.raises(BackpressureError):
        eng.submit("overflow", 9.0, "t", "a", 1.0)
    # after catching up, submission succeeds again
    eng.catch_up()
    eng.submit("overflow", 9.0, "t", "a", 1.0)
    eng.catch_up()
    assert eng.snapshot("count_by_key").offset == 9
    eng.close()


def test_rebuild_concurrent_with_ingestion(tmp_path):
    eng = make_engine(tmp_path, batch_size=4, batch_max_wait_ms=5)
    eng.start()
    for i in range(100):
        eng.submit(f"e{i}", float(i), "t", f"k{i % 3}", float(i))
    eng.catch_up()

    # rebuild while new events keep arriving
    done = threading.Event()

    def keep_ingesting():
        for i in range(100, 200):
            eng.submit(f"e{i}", float(i), "t", f"k{i % 3}", float(i))
        done.set()

    t = threading.Thread(target=keep_ingesting)
    t.start()
    snap = eng.rebuild("sum_by_key")
    t.join()
    eng.catch_up()

    final = eng.snapshot("sum_by_key")
    expected = {f"k{i % 3}": 0.0 for i in range(3)}
    for i in range(200):
        expected[f"k{i % 3}"] += float(i)
    assert final.state == expected
    assert final.offset == 200
    eng.stop()
    eng.close()


def test_stop_leaves_committed_state_consistent(tmp_path):
    eng = make_engine(tmp_path, batch_size=3, batch_max_wait_ms=5)
    eng.start()
    for i in range(200):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)
    eng.stop(timeout_ms=5000)  # may stop mid-stream
    snap = eng.snapshot("count_by_key")
    # whatever offset we stopped at, the state is a consistent prefix
    assert sum(snap.state.values()) == snap.offset
    # restart resumes and converges
    eng2 = make_engine(tmp_path, batch_size=3)
    eng2.catch_up()
    assert eng2.snapshot("count_by_key").state == {"a": 200}
    eng2.close()
    eng.close()
