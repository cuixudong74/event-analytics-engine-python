"""Incremental maintenance must converge to the full-rebuild result,
regardless of out-of-order, duplicate or late arrivals."""
from __future__ import annotations

import random

import pytest

from event_engine.engine import Engine
from event_engine.models import EngineConfig, SkipReason
from event_engine.views import CountByKeyView, SumByKeyView


def make_engine(tmp_path, **cfg) -> Engine:
    return Engine([CountByKeyView(), SumByKeyView()],
                  EngineConfig(data_dir=str(tmp_path), **cfg))


def reference(events):
    """Full-rebuild semantics computed independently."""
    count, total, seen = {}, {}, set()
    for e in events:
        eid, _, _, key, value = e
        if eid in seen:
            continue
        seen.add(eid)
        if not key or not isinstance(value, (int, float)) \
                or value != value or value in (float("inf"), float("-inf")):
            continue
        count[key] = count.get(key, 0) + 1
        if abs(value) <= SumByKeyView.MAX_ABS_VALUE:
            total[key] = total.get(key, 0.0) + value
    return count, total


def test_out_of_order_duplicate_and_late(tmp_path):
    eng = make_engine(tmp_path, batch_size=3)
    eng.submit("e3", ts=3.0, type="t", key="a", value=3.0)
    eng.submit("e1", ts=1.0, type="t", key="a", value=1.0)   # out of order
    eng.submit("e2", ts=2.0, type="t", key="b", value=2.0)
    eng.submit("e1", ts=1.0, type="t", key="a", value=1.0)   # duplicate
    eng.submit("e0", ts=0.5, type="t", key="b", value=0.5)   # late arrival
    eng.catch_up()

    assert eng.snapshot("count_by_key").state == {"a": 2, "b": 2}
    assert eng.snapshot("sum_by_key").state == {"a": 4.0, "b": 2.5}
    # duplicate is reported, not silently applied
    skipped = eng.skipped("count_by_key")
    assert len(skipped) == 1
    assert skipped[0].reason is SkipReason.DUPLICATE
    assert skipped[0].event_id == "e1"
    eng.close()


def test_incremental_matches_randomized_rebuild(tmp_path):
    rng = random.Random(42)
    events = []
    for i in range(500):
        events.append((f"e{rng.randrange(400)}",  # ~20% duplicate ids
                       float(rng.randrange(1000)),  # shuffled timestamps
                       "t",
                       f"k{rng.randrange(7)}",
                       round(rng.uniform(-100, 100), 3)))
    rng.shuffle(events)  # out-of-order arrival

    eng = make_engine(tmp_path, batch_size=13)
    for e in events:
        eng.submit(*e)
    eng.catch_up()

    exp_count, exp_sum = reference(events)
    assert eng.snapshot("count_by_key").state == exp_count
    assert eng.snapshot("sum_by_key").state == pytest.approx(exp_sum)

    # and exactly matches a full rebuild over the same log
    rebuilt = eng.rebuild("sum_by_key")
    assert rebuilt.state == eng.snapshot("sum_by_key").state
    eng.close()


def test_invalid_events_skipped_with_reasons(tmp_path):
    eng = make_engine(tmp_path)
    eng.submit("ok", 1.0, "t", "a", 5.0)
    eng.submit("nan", 2.0, "t", "a", float("nan"))
    eng.submit("inf", 3.0, "t", "a", float("inf"))
    eng.submit("huge", 4.0, "t", "a", 1e13)          # out of range for sum view
    eng.submit("nokey", 5.0, "t", "", 1.0)           # invalid schema
    eng.catch_up()

    assert eng.snapshot("sum_by_key").state == {"a": 5.0}
    skipped = {s.event_id: s.reason for s in eng.skipped("sum_by_key")}
    assert skipped["nan"] is SkipReason.OUT_OF_RANGE
    assert skipped["inf"] is SkipReason.OUT_OF_RANGE
    assert skipped["huge"] is SkipReason.OUT_OF_RANGE
    assert skipped["nokey"] is SkipReason.INVALID_SCHEMA
    # every skip is attributable to a concrete event and view
    for s in eng.skipped("sum_by_key"):
        assert s.view == "sum_by_key" and s.event_id and s.seq >= 0
    eng.close()
