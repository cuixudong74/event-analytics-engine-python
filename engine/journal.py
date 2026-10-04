"""事件日志与检查点：崩溃恢复时区分已提交进度与未完成批次。"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .events import Event


def _event_to_json(ev: Event) -> str:
    return json.dumps(
        {"event_id": ev.event_id, "ts": ev.ts, "kind": ev.kind, "payload": ev.payload},
        sort_keys=True,
    )


def _event_from_json(line: str) -> Event:
    d = json.loads(line)
    return Event(event_id=d["event_id"], ts=d["ts"], kind=d["kind"], payload=d["payload"])


class Journal:
    """追加式共享事件日志。offset 从 0 开始单调递增，是各视图进度的唯一基准。

    所有视图共用同一份日志、各自推进；写入只追加，因此慢视图不会阻塞
    快视图，也不会拖垮接入。
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._events: list[Event] = []
        if self._path.exists():
            with self._path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        self._events.append(_event_from_json(line))
        self._fh = self._path.open("a", encoding="utf-8")

    def append(self, event: Event) -> int:
        """追加一条事件并返回其 offset。先落盘再入内存，保证可恢复。"""
        with self._lock:
            offset = len(self._events)
            self._fh.write(_event_to_json(event) + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._events.append(event)
            return offset

    def read_from(self, offset: int, limit: int | None = None) -> list[Event]:
        with self._lock:
            if offset < 0:
                offset = 0
            chunk = self._events[offset:]
            return list(chunk if limit is None else chunk[:limit])

    def high_watermark(self) -> int:
        """下一条事件将获得的 offset。"""
        with self._lock:
            return len(self._events)

    def close(self) -> None:
        with self._lock:
            self._fh.close()


class CheckpointStore:
    """每个视图已提交 (offset, state) 的持久化。

    写入采用 临时文件 + fsync + 原子 rename：崩溃时要么看到旧检查点，
    要么看到新检查点，绝不会留下写了一半的视图状态。只有完整批次提交后
    才会写检查点，因此重启后已提交进度与未完成批次天然可区分——未完成的
    批次根本没有检查点，恢复时从上次已提交 offset 重放日志即可。
    """

    def __init__(self, directory: Path) -> None:
        self._dir = directory
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _file(self, view: str) -> Path:
        safe = view.replace("/", "_")
        return self._dir / f"{safe}.checkpoint.json"

    def save(self, view: str, offset: int, state: Any) -> None:
        payload = json.dumps({"view": view, "offset": offset, "state": state}, sort_keys=True)
        with self._lock:
            tmp = self._file(view).with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._file(view))

    def load(self, view: str) -> tuple[int, Any] | None:
        f = self._file(view)
        if not f.exists():
            return None
        with f.open("r", encoding="utf-8") as fh:
            d = json.load(fh)
        return d["offset"], d["state"]
