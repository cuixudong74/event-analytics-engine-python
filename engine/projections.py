"""投影（派生视图）定义：增量 apply 与全量 rebuild 共用同一套确定性语义。

一致性契约：
- add(state, event) 返回新状态，不修改入参（状态按不可变约定使用，
  查询侧拿到的快照引用因此永远稳定）。
- 对每个事件恰好应用一次（由引擎按 event_id 去重保证），且 add 对事件
  集合可交换、可结合 —— 因此乱序、重复、迟到到达的增量维护结果与
  用同一批事件全量重建的结果完全一致。
- 状态必须是 JSON 可序列化的，以便检查点持久化与崩溃恢复。
"""
from __future__ import annotations

import math
from typing import Any, Protocol

from .events import Event, SkipReason


class Projection(Protocol):
    """投影协议。"""

    name: str

    def initial_state(self) -> Any: ...

    def check(self, event: Event) -> tuple[SkipReason, str] | None:
        """投影级校验：返回 None 表示可计入，否则返回 (原因, 细节)。"""
        ...

    def add(self, state: Any, event: Event) -> Any: ...

    def merge(self, a: Any, b: Any) -> Any: ...


def rebuild_state(proj: Projection, events: list[Event]) -> tuple[Any, list[tuple[Event, SkipReason, str]]]:
    """全量重建：从空状态按序 fold。返回 (状态, 跳过列表)。"""
    state = proj.initial_state()
    skipped: list[tuple[Event, SkipReason, str]] = []
    for ev in events:
        problem = proj.check(ev)
        if problem is not None:
            skipped.append((ev, problem[0], problem[1]))
            continue
        state = proj.add(state, ev)
    return state, skipped


class CountByKind:
    """按 kind 计数。"""

    def __init__(self, name: str = "count_by_kind") -> None:
        self.name = name

    def initial_state(self) -> dict[str, int]:
        return {}

    def check(self, event: Event):
        return None

    def add(self, state: dict[str, int], event: Event) -> dict[str, int]:
        new = dict(state)
        new[event.kind] = new.get(event.kind, 0) + 1
        return new

    def merge(self, a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
        out = dict(a)
        for k, v in b.items():
            out[k] = out.get(k, 0) + v
        return out


class SumByKey:
    """按 payload['group'] 分组，对 payload['value'] 求和。

    value 必须是有限数值且绝对值不超过 max_abs，否则按原因跳过。
    """

    def __init__(self, name: str = "sum_by_key", max_abs: float = 1e15) -> None:
        self.name = name
        self._max_abs = max_abs

    def initial_state(self) -> dict[str, float]:
        return {}

    def check(self, event: Event):
        if "group" not in event.payload or "value" not in event.payload:
            return SkipReason.MISSING_FIELD, "payload needs 'group' and 'value'"
        v = event.payload["value"]
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v):
            return SkipReason.BAD_TYPE, f"value {v!r} is not a finite number"
        if abs(v) > self._max_abs:
            return SkipReason.OUT_OF_RANGE, f"|value| {abs(v)} > {self._max_abs}"
        return None

    def add(self, state: dict[str, float], event: Event) -> dict[str, float]:
        new = dict(state)
        g = str(event.payload["group"])
        new[g] = new.get(g, 0.0) + float(event.payload["value"])
        return new

    def merge(self, a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
        out = dict(a)
        for k, v in b.items():
            out[k] = out.get(k, 0.0) + v
        return out


class LatestByKey:
    """每个 key 保留事件时间最新的值：state[key] = [ts, event_id, value]。

    比较键为 (ts, event_id)，与到达顺序无关，因此乱序/迟到安全。
    """

    def __init__(self, name: str = "latest_by_key") -> None:
        self.name = name

    def initial_state(self) -> dict[str, list]:
        return {}

    def check(self, event: Event):
        if "key" not in event.payload or "value" not in event.payload:
            return SkipReason.MISSING_FIELD, "payload needs 'key' and 'value'"
        return None

    def add(self, state: dict[str, list], event: Event) -> dict[str, list]:
        key = str(event.payload["key"])
        cand = [event.ts, event.event_id, event.payload["value"]]
        cur = state.get(key)
        if cur is not None and (cur[0], cur[1]) >= (cand[0], cand[1]):
            return state
        new = dict(state)
        new[key] = cand
        return new

    def merge(self, a: dict[str, list], b: dict[str, list]) -> dict[str, list]:
        out = dict(a)
        for k, v in b.items():
            cur = out.get(k)
            if cur is None or (v[0], v[1]) > (cur[0], cur[1]):
                out[k] = v
        return out
