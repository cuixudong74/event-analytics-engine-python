"""Crash/recovery: resume from the last committed position without
double-apply, gaps, or half-written views."""
from __future__ import annotations

import json
import os

import pytest

from event_engine.checkpoint import CheckpointStore
from event_engine.engine import Engine
from event_engine.models import EngineConfig
from event_engine.views import CountByKeyView, SumByKeyView

VIEWS = [CountByKeyView(), SumByKeyView()]


def make_engine(tmp_path, **cfg) -> Engine:
    return Engine(list(VIEWS), EngineConfig(data_dir=str(tmp_path), **cfg))


def test_resume_from_committed_offset(tmp_path):
    eng = make_engine(tmp_path, batch_size=2)
    for i in range(5):
        eng.submit(f"e{i}", float(i), "t", "a", float(i))
    eng.catch_up()
    assert eng.snapshot("count_by_key").offset == 5
    eng.close()

    eng2 = make_engine(tmp_path, batch_size=2)
    # state survives restart exactly; nothing re-applied
    assert eng2.snapshot("count_by_key").state == {"a": 5}
    eng2.submit("e5", 5.0, "t", "a", 5.0)
    eng2.catch_up()
    assert eng2.snapshot("count_by_key").state == {"a": 6}
    assert eng2.snapshot("sum_by_key").state == {"a": 15.0}
    eng2.close()


def test_pending_batch_discarded_and_replayed(tmp_path):
    eng = make_engine(tmp_path, batch_size=2)
    for i in range(3):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)
    eng.catch_up()
    eng.close()

    # simulate a crash while a batch over offsets [3, 5) was in progress
    eng = make_engine(tmp_path)
    for i in (3, 4):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)
    ck = CheckpointStore(os.path.join(str(tmp_path), "checkpoints.json"))
    ck.begin_batch("count_by_key", 3, 5)   # never committed -> "crash"
    del eng  # no clean close: in-flight batch is lost

    eng2 = make_engine(tmp_path)
    # pending marker was discarded; committed state untouched
    assert eng2.snapshot("count_by_key").state == {"a": 3}
    eng2.catch_up()  # events 3,4 replayed exactly once
    assert eng2.snapshot("count_by_key").state == {"a": 5}
    assert eng2.snapshot("count_by_key").offset == 5
    eng2.close()


def test_crash_between_state_write_and_checkpoint_commit(tmp_path, monkeypatch):
    eng = make_engine(tmp_path, batch_size=4)
    for i in range(4):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)

    # crash right after the state file is written, before checkpoint commit
    def boom(*a, **k):
        raise RuntimeError("simulated crash")
    monkeypatch.setattr(eng.checkpoints, "commit_batch", boom)
    with pytest.raises(RuntimeError):
        eng.catch_up()
    del eng

    eng2 = make_engine(tmp_path)
    # the fully-written batch is adopted, not re-applied or lost
    snap = eng2.snapshot("count_by_key")
    assert snap.state == {"a": 4} and snap.offset == 4
    assert eng2.checkpoints.committed_offset("count_by_key") == 4
    eng2.submit("e4", 4.0, "t", "a", 1.0)
    eng2.catch_up()
    assert eng2.snapshot("count_by_key").state == {"a": 5}
    eng2.close()


def test_torn_state_file_tmp_ignored(tmp_path):
    eng = make_engine(tmp_path)
    eng.submit("e0", 0.0, "t", "a", 1.0)
    eng.catch_up()
    eng.close()
    # a half-written temp file from a crashed commit must be ignored
    with open(os.path.join(str(tmp_path), "count_by_key.state.json.tmp"), "w") as fh:
        fh.write('{"offset": 999, "sta')  # torn write
    eng2 = make_engine(tmp_path)
    assert eng2.snapshot("count_by_key").state == {"a": 1}
    eng2.close()


def test_checkpoint_file_is_valid_json_after_every_commit(tmp_path):
    eng = make_engine(tmp_path, batch_size=1)
    for i in range(5):
        eng.submit(f"e{i}", float(i), "t", "a", 1.0)
        eng.catch_up()
        with open(os.path.join(str(tmp_path), "checkpoints.json")) as fh:
            json.load(fh)  # never torn
    eng.close()
