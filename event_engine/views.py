"""View definitions: how events fold into derived state.

A view must be a *deterministic, idempotent* fold over events so that
incremental maintenance converges to exactly what a full rebuild over the
same events would produce, regardless of arrival order, duplicates or
late arrivals. Dedup by event id happens in the engine (applied_ids), so
apply_one itself only needs to be commutative and order-independent.
"""
from __future__ import annotations

import math
from typing import Any, Protocol

from .models import Event, SkipReason


class ViewDefinition(Protocol):
    """Contract every derived view implements."""

    name: str

    def validate(self, event: Event) -> SkipReason | None:
        """Return a SkipReason if the event must not be applied, else None."""
        ...

    def apply_one(self, state: dict[str, Any], event: Event) -> None:
        """Fold one validated event into ``state`` (must be commutative)."""
        ...


def _validate_common(event: Event) -> SkipReason | None:
    if not event.id or not event.key:
        return SkipReason.INVALID_SCHEMA
    if not isinstance(event.value, (int, float)) or isinstance(event.value, bool):
        return SkipReason.INVALID_SCHEMA
    if not math.isfinite(event.value):
        return SkipReason.OUT_OF_RANGE
    return None


class CountByKeyView:
    """Counts events per key."""

    name = "count_by_key"

    def validate(self, event: Event) -> SkipReason | None:
        return _validate_common(event)

    def apply_one(self, state: dict[str, Any], event: Event) -> None:
        state[event.key] = state.get(event.key, 0) + 1


class SumByKeyView:
    """Sums event values per key, rejecting out-of-range values."""

    name = "sum_by_key"
    MAX_ABS_VALUE = 1e12

    def validate(self, event: Event) -> SkipReason | None:
        reason = _validate_common(event)
        if reason is not None:
            return reason
        if abs(event.value) > self.MAX_ABS_VALUE:
            return SkipReason.OUT_OF_RANGE
        return None

    def apply_one(self, state: dict[str, Any], event: Event) -> None:
        state[event.key] = state.get(event.key, 0.0) + event.value
