"""引擎：接入事件、按视图独立推进增量维护、支持全量重建与并发查询。

关键语义：
- 共享一份追加式日志（Journal），每个视图一个后台 worker 按自己的进度推进，
  慢视图不阻塞快视图，接入只追加日志、不被任何视图拖垮。
- 批次只有在完整应用完后才 (checkpoint + 内存提交) 原子生效；取消/超时/崩溃
  都不会留下半个批次。重启后从最近已提交 offset 重放，配合 event_id 去重，
  做到不重、不漏。
- 全量重建与增量维护并存：重建基于日志快照离线计算，交换时短暂暂停该视图的
  worker，把重建期间到达的尾部事件增量补齐后原子换入，结果与纯增量路径一致。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .events import Event, SkipReason, SkippedEvent, validate_event
from .journal import CheckpointStore, Journal
from .projections import Projection, rebuild_state
from .store import Snapshot, ViewStore


class BackpressureError(RuntimeError):
    """接入队列（未应用事件数）超过 max_queue 时抛出。"""


class BatchTimeout(RuntimeError):
    """单批应用超过 apply_timeout_s。"""


@dataclass
class EngineConfig:
    """可配置项：批大小、回压、超时、轮询间隔、数据目录、跳过记录容量。"""

    batch_size: int = 100
    max_queue: int = 10_000          # 未应用事件上限（回压阈值）
    apply_timeout_s: float = 30.0    # 单批应用超时
    poll_interval_s: float = 0.05    # worker 空闲轮询间隔
    data_dir: Path = Path("./data")
    skipped_capacity: int = 1000     # 每视图保留的跳过记录条数


@dataclass
class _ViewRuntime:
    projection: Projection
    store: ViewStore
    applied: set[str]                       # 已应用的 event_id（去重，保证恰好一次）
    skipped: deque[SkippedEvent]            # 跳过记录（有界）
    paused: threading.Event = field(default_factory=threading.Event)   # 重建交换期间暂停 worker
    mutex: threading.Lock = field(default_factory=threading.Lock)      # 提交与重建交换互斥
    thread: threading.Thread | None = None


class Engine:
    """事件驱动的实时分析引擎。"""

    def __init__(self, config: EngineConfig | None = None) -> None:
        self._config = config or EngineConfig()
        self._config.data_dir.mkdir(parents=True, exist_ok=True)
        self._journal = Journal(self._config.data_dir / "journal.log")
        self._checkpoints = CheckpointStore(self._config.data_dir / "checkpoints")
        self._views: dict[str, _ViewRuntime] = {}
        self._views_lock = threading.Lock()
        self._stop = threading.Event()

    # ---------------- 视图注册与恢复 ----------------

    def register_view(self, projection: Projection) -> None:
        """注册视图。若存在检查点则从最近已提交位置恢复，否则从头开始。"""
        with self._views_lock:
            if projection.name in self._views:
                raise ValueError(f"view {projection.name!r} already registered")
            saved = self._checkpoints.load(projection.name)
            if saved is None:
                offset, state, applied = -1, projection.initial_state(), set()
            else:
                offset, payload = saved
                state, applied = payload["projection"], set(payload["applied"])
            rt = _ViewRuntime(
                projection=projection,
                store=ViewStore(projection.name, initial_state=state, initial_offset=offset),
                applied=applied,
                skipped=deque(maxlen=self._config.skipped_capacity),
            )
            rt.thread = threading.Thread(target=self._run, args=(rt,), daemon=True,
                                         name=f"view-{projection.name}")
            self._views[projection.name] = rt
            rt.thread.start()

    # ---------------- 接入 ----------------

    def ingest(self, raw: Any) -> int:
        """校验并接入一条事件，返回其 offset。

        非法事件直接抛 ValueError（含可区分原因），绝不悄悄计入；
        未应用事件超过 max_queue 时抛 BackpressureError（回压）。
        """
        event = validate_event(raw)
        with self._views_lock:
            views = list(self._views.values())
        if views:
            lag = self._journal.high_watermark() - min(v.store.offset for v in views) - 1
            if lag >= self._config.max_queue:
                raise BackpressureError(f"ingest queue full: lag {lag} >= {self._config.max_queue}")
        return self._journal.append(event)

    # ---------------- 查询 ----------------

    def query(self, view: str) -> Snapshot:
        """读取某视图的一致性快照：state 与 offset 一定来自同一次已提交批次。"""
        return self._runtime(view).store.snapshot()

    def query_skips(self, view: str) -> list[SkippedEvent]:
        """该视图最近的跳过记录（含事件、原因、细节）。"""
        return list(self._runtime(view).skipped)

    def progress(self) -> dict[str, dict[str, int]]:
        """各视图已提交进度与滞后量。"""
        hw = self._journal.high_watermark()
        with self._views_lock:
            items = list(self._views.items())
        return {
            name: {"offset": rt.store.offset, "lag": hw - rt.store.offset - 1}
            for name, rt in items
        }

    # ---------------- 全量重建 ----------------

    def rebuild(self, view: str) -> None:
        """全量重建指定视图，与增量维护并存。

        重建期间新到的事件不会丢：先对日志快照 [0, hi) 离线重算，交换时再把
        (hi, 当前已提交] 的尾部事件增量补齐，最后原子换入。
        """
        rt = self._runtime(view)
        rt.paused.set()
        try:
            hi = self._journal.high_watermark()
            head = self._journal.read_from(0, hi)
            state, skipped = rebuild_state(rt.projection, head)
            applied = {e.event_id for e in head} - {s[0].event_id for s in skipped}
            skip_records: list[SkippedEvent] = [
                SkippedEvent(event=e, view=view, reason=r, detail=d) for e, r, d in skipped
            ]
            with rt.mutex:
                # 补齐重建计算期间已提交到日志的尾部事件
                cur = rt.store.offset
                for ev in self._journal.read_from(hi, cur - hi + 1):
                    if ev.event_id in applied:
                        continue
                    problem = rt.projection.check(ev)
                    if problem is not None:
                        skip_records.append(SkippedEvent(ev, view, problem[0], problem[1]))
                        continue
                    state = rt.projection.add(state, ev)
                    applied.add(ev.event_id)
                self._commit(rt, max(cur, hi - 1), state, applied, force=True)
                rt.skipped.clear()
                rt.skipped.extend(skip_records[-self._config.skipped_capacity:])
        finally:
            rt.paused.clear()

    # ---------------- 生命周期 ----------------

    def close(self) -> None:
        """停止所有 worker。进行中的批次会把已处理部分作为完整子批提交后退出，
        不会留下半个批次。"""
        self._stop.set()
        with self._views_lock:
            threads = [rt.thread for rt in self._views.values() if rt.thread]
        for t in threads:
            t.join(timeout=self._config.apply_timeout_s + 5)
        self._journal.close()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------- 内部：worker 与批次应用 ----------------

    def _runtime(self, view: str) -> _ViewRuntime:
        with self._views_lock:
            rt = self._views.get(view)
        if rt is None:
            raise KeyError(f"unknown view {view!r}")
        return rt

    def _run(self, rt: _ViewRuntime) -> None:
        cfg = self._config
        while not self._stop.is_set():
            if rt.paused.is_set():
                time.sleep(cfg.poll_interval_s)
                continue
            batch = self._journal.read_from(rt.store.offset + 1, cfg.batch_size)
            if not batch:
                time.sleep(cfg.poll_interval_s)
                continue
            self._apply_batch(rt, batch, time.monotonic() + cfg.apply_timeout_s)

    def _apply_batch(self, rt: _ViewRuntime, batch: list[Event], deadline: float) -> None:
        """应用一个批次；超时/停止时把已处理部分作为完整子批提交。

        只有整批（或完整子批）处理完才提交，视图永远不会停在批次中间。
        所有副作用（applied/skipped）都推迟到提交时一次性生效；若提交时
        发现重建正在进行（paused），整批丢弃——重建会从日志重算这些事件。
        """
        proj = rt.projection
        state = rt.store.snapshot().state
        seen: set[str] = set()  # 本批内已应用，防止同批重复
        new_ids: list[str] = []
        new_skips: list[SkippedEvent] = []
        processed = 0
        for ev in batch:
            if ev.event_id in rt.applied or ev.event_id in seen:
                new_skips.append(SkippedEvent(ev, proj.name, SkipReason.DUPLICATE, "event_id already applied"))
            else:
                problem = proj.check(ev)
                if problem is not None:
                    new_skips.append(SkippedEvent(ev, proj.name, problem[0], problem[1]))
                else:
                    state = proj.add(state, ev)
                    seen.add(ev.event_id)
                    new_ids.append(ev.event_id)
            processed += 1
            if time.monotonic() > deadline or self._stop.is_set():
                break
        with rt.mutex:
            if rt.paused.is_set():
                return  # 重建交换进行中，丢弃本批，重建会覆盖这些事件
            rt.applied.update(new_ids)
            rt.skipped.extend(new_skips)
            self._commit(rt, rt.store.offset + processed, state, rt.applied)

    def _commit(self, rt: _ViewRuntime, offset: int, state: Any, applied: set[str],
                force: bool = False) -> None:
        """原子提交：先落检查点（原子 rename），再换内存状态。

        offset 倒退（或持平）的提交直接忽略，除非 force（重建换入允许同位替换）。
        """
        if not force and offset <= rt.store.offset:
            return
        rt.applied = set(applied)
        payload = {"projection": state, "applied": sorted(applied)}
        self._checkpoints.save(rt.projection.name, offset, payload)
        rt.store.replace(offset, state)
