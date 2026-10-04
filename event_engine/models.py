"""Core type definitions for the event analytics engine.

Only data types and enums live here; behaviour lives in the other modules.
"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any, Mapping


class SkipReason(enum.Enum):
    """Why an event was not applied to a view."""

    DUPLICATE = "duplicate"            # event_id already applied to this view
    INVALID_SCHEMA = "invalid_schema"  # missing/ill-typed required fields
    OUT_OF_RANGE = "out_of_range"      # value outside the view's accepted domain
    UNKNOWN_TYPE = "unknown_type"      # event type the view does not handle


@dataclass(frozen=True)
class Event:
    """A single ingested event.

    seq:    monotonic offset assigned by the event log at append time.
    id:     producer-supplied unique id, used for dedup (idempotent apply).
    ts:     event-time timestamp (may arrive out of order / late).
    type:   event type discriminator, e.g. "purchase".
    key:    grouping key for aggregations.
    value:  numeric payload.
    """

    seq: int
    id: str
    ts: float
    type: str
    key: str
    value: float


@dataclass(frozen=True)
class SkippedEvent:
    """Record of an event that was not applied, with a machine-readable reason."""

    view: str
    event_id: str
    seq: int
    reason: SkipReason
    detail: str = ""


@dataclass(frozen=True)
class ViewSnapshot:
    """Immutable, consistent view of one derived view at a progress point.

    Readers only ever see a fully committed ViewSnapshot: a batch either
    appears whole or not at all (no half-applied batches, no field-level
    mixing).
    """

    view: str
    offset: int                      # log offset up to which state is consistent
    state: Mapping[str, Any]         # immutable aggregate state
    applied_ids: frozenset[str]      # event ids already applied (dedup)
    skipped: tuple[SkippedEvent, ...] = ()
    committed_at: float = 0.0


@dataclass(frozen=True)
class BatchOutcome:
    """Result of applying one batch to one view."""

    view: str
    from_offset: int
    to_offset: int
    applied: int
    skipped: tuple[SkippedEvent, ...]


@dataclass
class EngineConfig:
    """Tunable knobs for ingestion / maintenance."""

    batch_size: int = 100            # max events per maintenance batch
    batch_max_wait_ms: float = 50.0  # max wait to fill a batch
    queue_capacity: int = 10_000     # bounded ingestion queue (backpressure)
    ingest_timeout_ms: float = 5_000.0  # max time a submit() may block on a full queue
    data_dir: str = ".event_engine"  # where log/checkpoints persist
    max_skipped_kept: int = 1_000    # ring buffer of skip records per view


@dataclass
class ViewStatus:
    """Runtime status of a single view (reporting only)."""

    name: str
    committed_offset: int
    pending_events: int
    rebuilding: bool
    skipped_count: int


def now_ms() -> float:
    return time.time() * 1000.0
