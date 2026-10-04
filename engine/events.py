"""事件模型与校验：定义事件结构、校验结果与跳过原因。"""
from __future__ import annotations

import enum
import math
from dataclasses import dataclass, field
from typing import Any


class SkipReason(enum.Enum):
    """事件被跳过（不计入视图）的原因，可按原因区分。"""

    MISSING_FIELD = "missing_field"
    BAD_TYPE = "bad_type"
    OUT_OF_RANGE = "out_of_range"
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class Event:
    """一条到达的事件。event_id 全局唯一，ts 为事件时间（秒）。"""

    event_id: str
    ts: float
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SkippedEvent:
    """被跳过的事件及其原因，便于定位到具体事件和视图。"""

    event: Event
    view: str
    reason: SkipReason
    detail: str = ""


# 事件时间的合法范围：防止明显越界的时间戳污染视图
_MIN_TS = 0.0
_MAX_TS = 4_102_444_800.0  # 2100-01-01 UTC


def validate_event(raw: Any) -> Event:
    """把外部输入校验并转换为 Event。

    非法输入抛出 ValueError（携带具体原因），绝不悄悄当成正常值。
    """
    if isinstance(raw, Event):
        ev = raw
    elif isinstance(raw, dict):
        try:
            ev = Event(
                event_id=raw["event_id"],
                ts=raw["ts"],
                kind=raw["kind"],
                payload=raw.get("payload") or {},
            )
        except KeyError as exc:
            raise ValueError(f"{SkipReason.MISSING_FIELD.value}: {exc}") from exc
    else:
        raise ValueError(f"{SkipReason.BAD_TYPE.value}: expect Event or dict, got {type(raw).__name__}")

    if not isinstance(ev.event_id, str) or not ev.event_id:
        raise ValueError(f"{SkipReason.BAD_TYPE.value}: event_id must be a non-empty str")
    if not isinstance(ev.ts, (int, float)) or isinstance(ev.ts, bool) or not math.isfinite(ev.ts):
        raise ValueError(f"{SkipReason.BAD_TYPE.value}: ts must be a finite number")
    if not (_MIN_TS <= float(ev.ts) <= _MAX_TS):
        raise ValueError(f"{SkipReason.OUT_OF_RANGE.value}: ts {ev.ts} out of [{_MIN_TS}, {_MAX_TS}]")
    if not isinstance(ev.kind, str) or not ev.kind:
        raise ValueError(f"{SkipReason.BAD_TYPE.value}: kind must be a non-empty str")
    if not isinstance(ev.payload, dict):
        raise ValueError(f"{SkipReason.BAD_TYPE.value}: payload must be a dict")
    return ev
