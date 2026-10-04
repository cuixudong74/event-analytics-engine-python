"""Durable progress checkpoints.

Persists, per view, the last *committed* log offset plus any in-progress
batch marker. On restart, in-progress batches are discarded and
maintenance resumes from the committed offset - no double-apply, no
missed events, no half-written views.

File layout::

    {"views": {"<view>": {"committed": <int>,
                          "pending": [from_offset, to_offset] | null}}}

All writes are atomic (temp file + os.replace + fsync), so a crash can
never leave a torn checkpoint file.
"""
from __future__ import annotations

import json
import os
import threading

from .models import BatchOutcome


class CheckpointStore:
    """JSON-file-backed store of committed offsets and pending batches."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._data: dict = {"views": {}}
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                self._data = json.load(fh)

    # -- persistence ---------------------------------------------------
    def _flush_locked(self) -> None:
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._path)

    def _entry(self, view: str) -> dict:
        return self._data["views"].setdefault(
            view, {"committed": 0, "pending": None})

    # -- API -----------------------------------------------------------
    def committed_offset(self, view: str) -> int:
        with self._lock:
            return int(self._data["views"].get(view, {}).get("committed", 0))

    def begin_batch(self, view: str, from_offset: int, to_offset: int) -> None:
        """Record an in-progress batch (cleared by commit/rollback)."""
        with self._lock:
            entry = self._entry(view)
            entry["pending"] = [from_offset, to_offset]
            self._flush_locked()

    def commit_batch(self, view: str, outcome: BatchOutcome) -> None:
        """Atomically record committed offset and clear the pending marker."""
        with self._lock:
            entry = self._entry(view)
            entry["committed"] = outcome.to_offset
            entry["pending"] = None
            self._flush_locked()

    def rollback_batch(self, view: str) -> None:
        """Clear the pending marker without advancing the offset."""
        with self._lock:
            entry = self._entry(view)
            entry["pending"] = None
            self._flush_locked()

    def pending_batches(self) -> dict[str, tuple[int, int]]:
        """In-progress batches found at startup (to be discarded/replayed)."""
        with self._lock:
            return {
                name: (entry["pending"][0], entry["pending"][1])
                for name, entry in self._data["views"].items()
                if entry.get("pending") is not None
            }
