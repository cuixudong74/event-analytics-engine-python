"""MVCC view store.

Each view's published state is an immutable ViewSnapshot. A maintenance
batch builds the next snapshot off-line and publishes it atomically, so
concurrent readers always observe a consistent progress point - never a
half-applied batch or field-level mix.
"""
from __future__ import annotations

import threading

from .models import ViewSnapshot


class ViewStore:
    """Publishes immutable per-view snapshots with atomic swap."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshots: dict[str, ViewSnapshot] = {}

    def get(self, view: str) -> ViewSnapshot | None:
        """Latest committed snapshot for ``view`` (None if never committed)."""
        with self._lock:
            return self._snapshots.get(view)

    def publish(self, snapshot: ViewSnapshot) -> None:
        """Atomically replace the visible snapshot of a view."""
        with self._lock:
            self._snapshots[snapshot.view] = snapshot
