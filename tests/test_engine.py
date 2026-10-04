"""覆盖：乱序、重复、迟到、全量重建、中断恢复、并发查询、回压、超时、非法事件。"""
from __future__ import annotations

import random
import threading
import time
from pathlib import Path

import pytest

from engine.engine import BackpressureError, Engine, EngineConfig
from engine.events import Event, SkipReason
from engine.projections import CountByKind, LatestByKey, SumByKey, rebuild_state


def make_engine(tmp_path: Path, **kw) -> Engine:
    kw.setdefault("poll_interval_s", 0.005)
    kw.setdefault("data_dir", tmp_path)
    return Engine(EngineConfig(**kw))


def wait_caught_up(engine: Engine, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        prog = engine.progress()
        if prog and all(p["lag"] == 0 for p in prog.values()):
            return
        time.sleep(0.01)
    raise TimeoutError(f"views did not catch up: {engine.progress()}")


def shuffled_events(n: int, seed: int = 1) -> list[Event]:
    evs = [
        Event(f"e{i}", ts=float(n - i), kind="click" if i % 2 else "view",
              payload={"group": f"g{i % 5}", "value": i, "key": f"u{i % 7}"})
        for i in range(n)
    ]
    random.Random(seed).shuffle(evs)
    return evs


# ---------- 1. 乱序 / 重复 / 迟到：增量 == 全量重建 ----------

@pytest.mark.parametrize("proj_cls", [CountByKind, SumByKey, LatestByKey])
def test_out_of_order_duplicate_late_matches_full_rebuild(tmp_path, proj_cls):
    engine = make_engine(tmp_path, batch_size=7)
    engine.register_view(proj_cls())
    events = shuffled_events(300)
    for e in events:
        engine.ingest(e)
    for e in events[:50]:  # 重复到达
        engine.ingest(e)
    for e in events[10:15]:  # 迟到再次到达
        engine.ingest(e)
    wait_caught_up(engine)
    snap = engine.query(proj_cls().name)
    expected, _ = rebuild_state(proj_cls(), sorted(events, key=lambda e: e.event_id))
    assert snap.state == expected
    engine.close()


def test_duplicate_recorded_with_reason(tmp_path):
    engine = make_engine(tmp_path)
    engine.register_view(CountByKind())
    e = Event("dup-1", 1.0, "click")
    engine.ingest(e)
    engine.ingest(e)
    wait_caught_up(engine)
    assert engine.query("count_by_kind").state == {"click": 1}
    skips = engine.query_skips("count_by_kind")
    assert any(s.reason is SkipReason.DUPLICATE and s.event.event_id == "dup-1" for s in skips)
    engine.close()


# ---------- 2. 并发查询：只能读到一致进度点 ----------

def test_concurrent_queries_see_consistent_snapshots(tmp_path):
    engine = make_engine(tmp_path, batch_size=3)
    engine.register_view(CountByKind())
    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            snap = engine.query("count_by_kind")
            total = sum(snap.state.values())
            # 无跳过事件时：已提交 offset+1 必须等于状态里的总计数
            if total != snap.offset + 1:
                violations.append(f"offset={snap.offset} total={total} state={snap.state}")

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()
    for i in range(600):
        engine.ingest(Event(f"e{i}", float(i), "a" if i % 2 else "b"))
    wait_caught_up(engine)
    stop.set()
    for t in readers:
        t.join()
    engine.close()
    assert not violations, violations[:5]


# ---------- 3. 中断恢复：不重、不漏、不留半批 ----------

def test_restart_resumes_from_committed_offset(tmp_path):
    engine = make_engine(tmp_path, batch_size=5)
    engine.register_view(CountByKind())
    for e in shuffled_events(120):
        engine.ingest(e)
    wait_caught_up(engine)
    before = engine.query("count_by_kind").state
    engine.close()

    engine2 = make_engine(tmp_path, batch_size=5)
    engine2.register_view(CountByKind())
    assert engine2.query("count_by_kind").state == before  # 检查点恢复
    engine2.ingest(Event("new-1", 999.0, "click"))
    wait_caught_up(engine2)
    after = engine2.query("count_by_kind").state
    assert after["click"] == before["click"] + 1
    engine2.close()


def test_crash_mid_consumption_replays_without_loss_or_dup(tmp_path):
    # 第一批：消费一部分后“崩溃”（直接放弃对象，不做优雅关闭的语义验证，
    # 通过 close 前未追平来制造未提交尾部）
    engine = make_engine(tmp_path, batch_size=4)
    engine.register_view(CountByKind())
    events = shuffled_events(50)
    for e in events:
        engine.ingest(e)
    engine.close()  # 停止 worker；检查点可能落后于日志
    committed = engine.query("count_by_kind").offset

    engine2 = make_engine(tmp_path, batch_size=4)
    engine2.register_view(CountByKind())
    wait_caught_up(engine2)
    snap = engine2.query("count_by_kind")
    expected, _ = rebuild_state(CountByKind(), events)
    assert snap.state == expected          # 不漏
    assert sum(snap.state.values()) == 50  # 不重
    assert snap.offset >= committed
    engine2.close()


def test_tiny_timeout_still_commits_only_complete_subbatches(tmp_path):
    engine = make_engine(tmp_path, batch_size=50, apply_timeout_s=0.0)
    engine.register_view(CountByKind())
    events = shuffled_events(200)
    for e in events:
        engine.ingest(e)
    wait_caught_up(engine)
    snap = engine.query("count_by_kind")
    expected, _ = rebuild_state(CountByKind(), events)
    assert snap.state == expected
    assert sum(snap.state.values()) == snap.offset + 1  # 状态与进度点一致
    engine.close()


# ---------- 4. 全量重建与增量并存 ----------

def test_rebuild_while_ingesting_matches_incremental(tmp_path):
    engine = make_engine(tmp_path, batch_size=4)
    engine.register_view(SumByKey())
    for e in shuffled_events(200):
        engine.ingest(e)
    wait_caught_up(engine)

    stop = threading.Event()
    def keep_ingesting():
        i = 200
        while not stop.is_set():
            engine.ingest(Event(f"e{i}", float(i), "view",
                                {"group": f"g{i % 5}", "value": i}))
            i += 1
    t = threading.Thread(target=keep_ingesting)
    t.start()
    for _ in range(5):
        engine.rebuild("sum_by_key")
    stop.set()
    t.join()
    wait_caught_up(engine)
    snap = engine.query("sum_by_key")
    n = snap.offset + 1
    expected = {}
    for i in range(n):
        expected[f"g{i % 5}"] = expected.get(f"g{i % 5}", 0.0) + float(i)
    assert snap.state == expected
    engine.close()


def test_rebuild_equals_incremental_from_same_events(tmp_path):
    engine = make_engine(tmp_path)
    engine.register_view(LatestByKey())
    events = shuffled_events(100)
    for e in events:
        engine.ingest(e)
    wait_caught_up(engine)
    before = engine.query("latest_by_key").state
    engine.rebuild("latest_by_key")
    after = engine.query("latest_by_key").state
    assert before == after
    engine.close()


# ---------- 5. 多视图独立进度 ----------

def test_views_progress_independently(tmp_path):
    engine = make_engine(tmp_path, batch_size=2)
    engine.register_view(CountByKind())
    for e in shuffled_events(30):
        engine.ingest(e)
    wait_caught_up(engine)
    # 后注册的视图从各自进度追赶，不影响已有视图
    engine.register_view(SumByKey())
    wait_caught_up(engine)
    assert engine.query("count_by_kind").state == {"click": 15, "view": 15}
    assert engine.query("sum_by_key").state == {
        f"g{i % 5}": float(sum(v for v in range(30) if v % 5 == i % 5)) for i in range(5)
    }
    engine.close()


def test_slow_view_does_not_block_fast_view(tmp_path):
    engine = make_engine(tmp_path, batch_size=2, max_queue=10_000)
    engine.register_view(CountByKind())
    engine.register_view(SumByKey())
    engine._runtime("sum_by_key").paused.set()  # 模拟慢视图
    for e in shuffled_events(40):
        engine.ingest(e)
    deadline = time.monotonic() + 5
    while engine.progress()["count_by_kind"]["lag"] != 0:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert engine.query("count_by_kind").state == {"click": 20, "view": 20}
    engine._runtime("sum_by_key").paused.clear()
    wait_caught_up(engine)
    assert sum(engine.query("sum_by_key").state.values()) == float(sum(range(40)))
    engine.close()


# ---------- 6. 回压与配置 ----------

def test_backpressure_when_queue_full(tmp_path):
    engine = make_engine(tmp_path, max_queue=5)
    engine.register_view(CountByKind())
    engine._runtime("count_by_kind").paused.set()  # 阻止消费以累积滞后
    for i in range(5):
        engine.ingest(Event(f"e{i}", float(i), "a"))
    with pytest.raises(BackpressureError):
        engine.ingest(Event("overflow", 6.0, "a"))
    engine._runtime("count_by_kind").paused.clear()
    engine.close()


# ---------- 7. 非法 / 越界事件 ----------

def test_invalid_events_rejected_at_ingest(tmp_path):
    engine = make_engine(tmp_path)
    engine.register_view(CountByKind())
    with pytest.raises(ValueError, match="missing_field"):
        engine.ingest({"ts": 1.0, "kind": "a"})
    with pytest.raises(ValueError, match="out_of_range"):
        engine.ingest({"event_id": "x", "ts": -1, "kind": "a"})
    with pytest.raises(ValueError, match="bad_type"):
        engine.ingest({"event_id": "x", "ts": float("nan"), "kind": "a"})
    engine.close()


def test_projection_level_skips_are_locatable(tmp_path):
    engine = make_engine(tmp_path)
    engine.register_view(SumByKey())
    engine.ingest(Event("bad-1", 1.0, "view", {"group": "g", "value": "not-a-number"}))
    engine.ingest(Event("bad-2", 2.0, "view", {"value": 1}))  # 缺 group
    engine.ingest(Event("ok-1", 3.0, "view", {"group": "g", "value": 2}))
    wait_caught_up(engine)
    assert engine.query("sum_by_key").state == {"g": 2.0}  # 非法值未悄悄计入
    skips = {s.event.event_id: s for s in engine.query_skips("sum_by_key")}
    assert skips["bad-1"].reason is SkipReason.BAD_TYPE
    assert skips["bad-2"].reason is SkipReason.MISSING_FIELD
    assert all(s.view == "sum_by_key" for s in skips.values())
    engine.close()
