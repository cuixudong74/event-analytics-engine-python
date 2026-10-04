"""Append-only durable event log.

Every ingested event is first persisted here and assigned a monotonic
``seq`` offset. Views (incremental maintenance and full rebuilds alike)
read events from this log, so a rebuild and the incremental path always
operate on the same source of truth.
"""
from __future__ import annotations

import json
import os
import threading

from .models import Event


class EventLog:
    """Durable append-only log of events with monotonic offsets.

    Events are appended to a JSONL file (fsync'd per append) and mirrored
    in memory for fast sequential reads. Offsets are dense: the event at
    list index ``i`` has ``seq == i``.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._events: list[Event] = []
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    self._events.append(Event(
                        seq=rec["seq"], id=rec["id"], ts=rec["ts"],
                        type=rec["type"], key=rec["key"], value=rec["value"],
                    ))
        # dense-offset invariant: seq must equal list index
        for i, ev in enumerate(self._events):
            if ev.seq != i:
                raise ValueError(f"corrupt event log at {path}: seq gap at {i}")
        self._fh = open(path, "a", encoding="utf-8")

    def append(self, event_id: str, ts: float, type: str, key: str, value: float) -> Event:
        """Persist an event and return it with its assigned ``seq``."""
        with self._lock:
            ev = Event(seq=len(self._events), id=event_id, ts=ts,
                       type=type, key=key, value=value)
            self._fh.write(json.dumps({
                "seq": ev.seq, "id": ev.id, "ts": ev.ts,
                "type": ev.type, "key": ev.key, "value": ev.value,
            }) + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._events.append(ev)
            return ev

    def read_from(self, offset: int, limit: int | None = None) -> list[Event]:
        """Read events with ``seq >= offset`` in seq order."""
        with self._lock:
            if offset < 0:
                offset = 0
            if limit is None:
                return list(self._events[offset:])
            return list(self._events[offset:offset + limit])

    def latest_offset(self) -> int:
        """Offset one past the newest event (0 when empty)."""
        with self._lock:
            return len(self._events)

    def close(self) -> None:
        with self._lock:
            self._fh.close()

    def __len__(self) -> int:
        return self.latest_offset()
