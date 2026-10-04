"""视图存储：以原子提交方式保存 (state, offset)，读取始终落在某个一致进度点。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Snapshot:
    """某一已提交进度点上的一致性快照（state 与 offset 一定来自同一次提交）。"""

    view: str
    offset: int
    state: Any


class ViewStore:
    """单视图的原子提交存储。

    commit 把 (offset, state) 作为整体在一次锁内替换，snapshot 在同一把锁内
    取出二者，因此读者永远读到某个完整的已提交批次，不会看到半个批次或
    字段级混合的中间状态。offset 只允许单调前进。
    """

    def __init__(self, view: str, initial_state: Any = None, initial_offset: int = -1) -> None:
        self._view = view
        self._lock = threading.Lock()
        self._state = initial_state
        self._offset = initial_offset

    @property
    def offset(self) -> int:
        with self._lock:
            return self._offset

    def commit(self, offset: int, state: Any) -> bool:
        """原子提交一个完整批次。offset 倒退时拒绝并返回 False。"""
        with self._lock:
            if offset <= self._offset:
                return False
            self._state = state
            self._offset = offset
            return True

    def replace(self, offset: int, state: Any) -> None:
        """无条件替换（仅供重建换入等在互斥保护下的整体替换使用）。"""
        with self._lock:
            self._state = state
            self._offset = offset

    def snapshot(self) -> Snapshot:
        with self._lock:
            return Snapshot(view=self._view, offset=self._offset, state=self._state)
