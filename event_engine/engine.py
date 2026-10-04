"""Engine: ingestion, batching, per-view incremental maintenance, rebuild.

One shared event log feeds any number of views. The log itself is the
buffer: each view advances through it at its own pace (slow views never
block fast ones), and ``submit`` applies backpressure only when the
slowest view lags more than ``queue_capacity`` events behind.

Commit protocol per batch (all-or-nothing):
  1. checkpoints.begin_batch (pending marker)
  2. fold events in memory (pure; discardable on cancel/timeout)
  3. atomically write the view state file (temp + os.replace)
  4. checkpoints.commit_batch (clears pending, records offset)
  5. publish immutable snapshot for readers

A crash anywhere before step 4 leaves the previous committed state
intact; the pending marker is discarded at startup and the batch is
replayed from the committed offset - no double-apply, no gaps, no
half-written views.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

from .checkpoint import CheckpointStore
from .event_log import EventLog
from .models import (
    BatchOutcome,
    EngineConfig,
    Event,
    SkippedEvent,
    SkipReason,
    ViewSnapshot,
    ViewStatus,
    now_ms,
)
from .store import ViewStore
from .views import ViewDefinition


class BackpressureError(TimeoutError):
    """Raised when submit() cannot enqueue within the ingest timeout."""


class _ViewRuntime:
    """Mutable per-view runtime state (committed position + aggregates)."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.offset = 0
        self.state: dict[str, Any] = {}
        self.applied_ids: set[str] = set()
        self.skipped: list[SkippedEvent] = []
        self.rebuilding = False
        self.lock = threading.Lock()          # serializes commits for this view
        self.thread: threading.Thread | None = None


class Engine:
    """Coordinates ingestion and view maintenance."""

    def __init__(
        self,
        views: list[ViewDefinition],
        config: EngineConfig | None = None,
        log: EventLog | None = None,
        checkpoints: CheckpointStore | None = None,
        store: ViewStore | None = None,
    ) -> None:
        self.config = config or EngineConfig()
        d = self.config.data_dir
        self.log = log or EventLog(os.path.join(d, "events.jsonl"))
        self.checkpoints = checkpoints or CheckpointStore(os.path.join(d, "checkpoints.json"))
        self.store = store or ViewStore()
        self.views: dict[str, ViewDefinition] = {v.name: v for v in views}
        self._rt: dict[str, _ViewRuntime] = {v.name: _ViewRuntime(v.name) for v in views}
        self._cond = threading.Condition()
        self._stopping = False
        self._started = False
        self._recover()

    # ------------------------------------------------------------------
    # persistence of committed view state
    # ------------------------------------------------------------------
    def _state_path(self, view: str) -> str:
        return os.path.join(self.config.data_dir, f"{view}.state.json")

    def _write_state_file(self, rt: _ViewRuntime) -> None:
        """Atomically persist the committed view state (temp + replace)."""
        payload = {
            "offset": rt.offset,
            "state": rt.state,
            "applied_ids": sorted(rt.applied_ids),
            "skipped": [
                {"view": s.view, "event_id": s.event_id, "seq": s.seq,
                 "reason": s.reason.value, "detail": s.detail}
                for s in rt.skipped[-self.config.max_skipped_kept:]
            ],
        }
        tmp = self._state_path(rt.name) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._state_path(rt.name))

    def _read_state_file(self, view: str) -> dict | None:
        path = self._state_path(view)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    # ------------------------------------------------------------------
    # recovery
    # ------------------------------------------------------------------
    def _recover(self) -> None:
        """Load committed state; discard in-progress batch markers."""
        for name, rt in self._rt.items():
            saved = self._read_state_file(name)
            if saved is not None:
                rt.offset = int(saved["offset"])
                rt.state = dict(saved["state"])
                rt.applied_ids = set(saved["applied_ids"])
                rt.skipped = [
                    SkippedEvent(view=s["view"], event_id=s["event_id"],
                                 seq=s["seq"], reason=SkipReason(s["reason"]),
                                 detail=s.get("detail", ""))
                    for s in saved.get("skipped", [])
                ]
            committed = self.checkpoints.committed_offset(name)
            if rt.offset > committed:
                # crash between state-file write and checkpoint commit:
                # the batch was fully applied, so roll the checkpoint forward.
                self.checkpoints.commit_batch(
                    name, BatchOutcome(name, committed, rt.offset, 0, ()))
            elif committed > rt.offset:
                raise RuntimeError(
                    f"view {name!r}: checkpoint offset {committed} ahead of "
                    f"state file offset {rt.offset}; state file lost?")
            # any pending batch was never committed: discard it; the events
            # will be replayed from the committed offset.
            if name in self.checkpoints.pending_batches():
                self.checkpoints.rollback_batch(name)
            self._publish(rt)

    def _publish(self, rt: _ViewRuntime) -> None:
        self.store.publish(ViewSnapshot(
            view=rt.name,
            offset=rt.offset,
            state=dict(rt.state),
            applied_ids=frozenset(rt.applied_ids),
            skipped=tuple(rt.skipped),
            committed_at=now_ms(),
        ))

    # ------------------------------------------------------------------
    # ingestion
    # ------------------------------------------------------------------
    def submit(self, event_id: str, ts: float, type: str, key: str, value: float,
               timeout_ms: float | None = None) -> Event:
        """Append an event to the log; blocks under backpressure up to timeout.

        Backpressure is driven by the slowest view: if the number of
        events not yet committed by every view exceeds ``queue_capacity``,
        submit waits (up to ``ingest_timeout_ms``) for views to catch up,
        then raises BackpressureError instead of growing memory unboundedly.
        """
        timeout = self.config.ingest_timeout_ms if timeout_ms is None else timeout_ms
        deadline = time.monotonic() + timeout / 1000.0
        with self._cond:
            while True:
                lag = self.log.latest_offset() - self._min_committed_offset()
                if lag < self.config.queue_capacity:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BackpressureError(
                        f"ingest backpressure: lag {lag} >= capacity "
                        f"{self.config.queue_capacity}")
                self._cond.wait(min(remaining, 0.05))
            ev = self.log.append(event_id, ts, type, key, value)
            self._cond.notify_all()
            return ev

    def _min_committed_offset(self) -> int:
        if not self._rt:
            return self.log.latest_offset()
        return min(rt.offset for rt in self._rt.values())

    # ------------------------------------------------------------------
    # batch application (the only place state advances)
    # ------------------------------------------------------------------
    def _fold(self, view: ViewDefinition, rt: _ViewRuntime,
              events: list[Event], state: dict[str, Any],
              applied_ids: set[str]) -> tuple[int, list[SkippedEvent]]:
        """Fold events into ``state`` (out-of-place). Returns (applied, skipped)."""
        applied = 0
        skipped: list[SkippedEvent] = []
        for ev in events:
            if ev.id in applied_ids:
                skipped.append(SkippedEvent(view.name, ev.id, ev.seq,
                                            SkipReason.DUPLICATE))
                continue
            reason = view.validate(ev)
            if reason is not None:
                skipped.append(SkippedEvent(view.name, ev.id, ev.seq, reason))
                continue
            view.apply_one(state, ev)
            applied_ids.add(ev.id)
            applied += 1
        return applied, skipped

    def _apply_batch_locked(self, rt: _ViewRuntime, events: list[Event]) -> BatchOutcome:
        """Apply one batch to a view. Caller holds rt.lock. All-or-nothing."""
        view = self.views[rt.name]
        from_offset = events[0].seq
        to_offset = events[-1].seq + 1
        self.checkpoints.begin_batch(rt.name, from_offset, to_offset)
        # fold out-of-place: cancelling/timing out here leaves committed
        # state untouched
        new_state = dict(rt.state)
        new_ids = set(rt.applied_ids)
        applied, skipped = self._fold(view, rt, events, new_state, new_ids)
        # commit: state file first, then checkpoint
        rt.state = new_state
        rt.applied_ids = new_ids
        rt.offset = to_offset
        rt.skipped = (rt.skipped + skipped)[-self.config.max_skipped_kept:]
        self._write_state_file(rt)
        outcome = BatchOutcome(rt.name, from_offset, to_offset, applied,
                               tuple(skipped))
        self.checkpoints.commit_batch(rt.name, outcome)
        self._publish(rt)
        return outcome

    def _maintain_once(self, name: str) -> bool:
        """Apply at most one batch for a view. Returns True if progress made."""
        rt = self._rt[name]
        with rt.lock:
            if rt.rebuilding:
                return False
            events = self.log.read_from(rt.offset, self.config.batch_size)
            if not events:
                return False
            self._apply_batch_locked(rt, events)
            return True

    # ------------------------------------------------------------------
    # background maintenance
    # ------------------------------------------------------------------
    def _worker(self, name: str) -> None:
        rt = self._rt[name]
        wait_s = self.config.batch_max_wait_ms / 1000.0
        while True:
            with self._cond:
                if self._stopping:
                    return
            try:
                progressed = self._maintain_once(name)
            except Exception:
                # a failed batch is simply not committed; back off and retry
                progressed = False
                time.sleep(wait_s)
            if not progressed:
                with self._cond:
                    if self._stopping:
                        return
                    self._cond.wait(wait_s)
            with self._cond:
                self._cond.notify_all()  # let submit() re-check backpressure

    def start(self) -> None:
        """Start background per-view maintenance loops."""
        with self._cond:
            if self._started:
                return
            self._started = True
            self._stopping = False
        for name, rt in self._rt.items():
            t = threading.Thread(target=self._worker, args=(name,),
                                 name=f"view-{name}", daemon=True)
            rt.thread = t
            t.start()

    def stop(self, timeout_ms: float | None = None) -> None:
        """Stop maintenance at a batch boundary; never half-committed."""
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
        deadline = None if timeout_ms is None else time.monotonic() + timeout_ms / 1000.0
        for rt in self._rt.values():
            if rt.thread is None:
                continue
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            rt.thread.join(remaining)
        with self._cond:
            self._started = False

    def catch_up(self, view: str | None = None) -> None:
        """Synchronously drain pending events into views (test/ops hook)."""
        names = [view] if view else list(self._rt)
        for name in names:
            while self._maintain_once(name):
                pass

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def snapshot(self, view: str) -> ViewSnapshot | None:
        """Latest consistent snapshot of a view."""
        return self.store.get(view)

    def skipped(self, view: str) -> list[SkippedEvent]:
        snap = self.store.get(view)
        return list(snap.skipped) if snap else []

    def status(self) -> list[ViewStatus]:
        latest = self.log.latest_offset()
        out = []
        for name, rt in self._rt.items():
            with rt.lock:
                out.append(ViewStatus(
                    name=name,
                    committed_offset=rt.offset,
                    pending_events=max(0, latest - rt.offset),
                    rebuilding=rt.rebuilding,
                    skipped_count=len(rt.skipped),
                ))
        return out

    # ------------------------------------------------------------------
    # full rebuild (concurrent with ingestion and queries)
    # ------------------------------------------------------------------
    def rebuild(self, view: str) -> ViewSnapshot:
        """Full rebuild from the log; concurrent with ingestion and queries.

        Folds the whole log into a shadow state (with the same validation
        and id-dedup as the incremental path), repeating the tail until
        caught up with events that arrived during the rebuild, then
        commits atomically. The result is identical to what incremental
        maintenance would have produced over the same events.
        """
        if view not in self.views:
            raise KeyError(view)
        rt = self._rt[view]
        vdef = self.views[view]
        with rt.lock:  # blocks incremental commits for this view meanwhile
            rt.rebuilding = True
            try:
                state: dict[str, Any] = {}
                ids: set[str] = set()
                skipped: list[SkippedEvent] = []
                offset = 0
                while True:
                    events = self.log.read_from(offset)
                    if not events:
                        break
                    _, sk = self._fold(vdef, rt, events, state, ids)
                    skipped.extend(sk)
                    offset = events[-1].seq + 1
                rt.state = state
                rt.applied_ids = ids
                rt.offset = offset
                rt.skipped = skipped[-self.config.max_skipped_kept:]
                self._write_state_file(rt)
                self.checkpoints.commit_batch(
                    view, BatchOutcome(view, 0, offset, 0, ()))
                self._publish(rt)
            finally:
                rt.rebuilding = False
        snap = self.store.get(view)
        assert snap is not None
        return snap

    # ------------------------------------------------------------------
    def close(self) -> None:
        self.stop()
        self.log.close()
