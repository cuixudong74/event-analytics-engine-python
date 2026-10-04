# Event Analytics Engine

事件驱动的实时分析引擎：事件持续到达，系统一边接收一边增量维护可查询的
派生视图（derived views），同时保留全量重建入口。增量维护与全量重建走同
一条事件日志、同一套折叠逻辑，保证两条路径结果一致。

## 架构

```
                ┌─────────────────────────────┐
 submit() ────► │ EventLog (append-only JSONL) │  唯一事实来源，单调 seq 偏移
                └──────────────┬──────────────┘
                               │ 每个视图独立按自己的 offset 消费
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
  view worker A          view worker B           rebuild(view)
  (增量批处理)            (增量批处理)            (影子状态全量折叠)
        │                      │                      │
        └──────────┬───────────┴──────────┬───────────┘
                   ▼                      ▼
        CheckpointStore            ViewStore (MVCC)
        committed offset +         不可变快照原子发布，
        pending batch marker       查询只读已提交快照
```

- `event_engine/event_log.py` — 追加式持久日志，事件落盘（fsync）后分配
  单调 `seq`。日志本身就是缓冲：不需要额外队列。
- `event_engine/views.py` — 视图定义（`validate` + `apply_one`），折叠必须
  可交换、与顺序无关。
- `event_engine/engine.py` — 接入、批处理、逐视图增量维护、回压、重建。
- `event_engine/checkpoint.py` — 已提交偏移 + 进行中批次标记，原子写盘。
- `event_engine/store.py` — MVCC 快照存储，原子换出。

## 一致性语义

1. **增量 ≡ 全量**：视图折叠对事件可交换且按 `event_id` 幂等去重，因此
   乱序、重复、迟到事件的增量维护结果，与对同一批事件全量重建的结果
   完全一致（`tests/test_incremental.py` 用随机乱序/重复事件验证）。
2. **读取一致性**：每个批次在内存中先完整折叠，再一次性原子发布不可变
   快照。任何时刻读到的都是某个已提交进度点（`offset`）对应的完整状态，
   不会读到半个批次或字段级混合。快照的 `offset` 单调不回退。
3. **崩溃恢复**：批次提交协议为
   `begin_batch → 内存折叠 → 原子写视图状态文件(temp+rename) → commit_checkpoint`。
   崩溃后重启：pending 批次标记被丢弃并从已提交偏移重放（不重、不漏）；
   若崩溃发生在状态文件写完与检查点提交之间，已完整落盘的批次被采纳，
   检查点向前补齐。撕裂的 `.tmp` 文件一律忽略。
4. **重建与增量并存**：重建在影子状态上折叠日志，追上重建期间新到的
   事件后，在视图提交锁内原子发布。重建期间新事件不丢，重建结果与增量
   路径对得上。
5. **多视图独立推进**：每个视图有独立的 worker、偏移和检查点。慢视图
   只影响自身进度；只有当最慢视图的滞后超过 `queue_capacity` 时，
   `submit` 才被回压（阻塞至超时抛 `BackpressureError` / HTTP 429），
   不会无界占用内存。
6. **取消与超时**：`stop()` 只在批次边界生效；批次折叠是纯内存操作，
   中途取消/超时不会留下半提交状态。重启后已提交进度（state 文件 +
   committed offset）与未完成批次（pending marker）严格区分。
7. **非法事件**：`validate` 不通过的事件不进入聚合，按 `SkipReason`
   （`duplicate` / `invalid_schema` / `out_of_range` / `unknown_type`）
   记录，可通过 `GET /views/{name}/skipped` 定位到具体事件与视图。

## 配置项（`EngineConfig` / 环境变量）

| 字段 | 环境变量 | 默认 | 含义 |
|---|---|---|---|
| `batch_size` | `EVENT_ENGINE_BATCH_SIZE` | 100 | 每批最多处理的事件数 |
| `batch_max_wait_ms` | — | 50 | worker 空闲时的等待间隔 |
| `queue_capacity` | `EVENT_ENGINE_QUEUE_CAPACITY` | 10000 | 回压阈值：最慢视图允许的最大滞后事件数 |
| `ingest_timeout_ms` | — | 5000 | `submit` 在回压下最长阻塞时间，超时抛 `BackpressureError` |
| `data_dir` | `EVENT_ENGINE_DATA_DIR` | `.event_engine` | 日志 / 检查点 / 视图状态文件目录 |
| `max_skipped_kept` | — | 1000 | 每视图保留的跳过记录上限（环形） |

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/events` | 提交事件 `{id, ts, type, key, value}`，回压超时返回 429 |
| `GET` | `/views/{name}` | 查询视图最新一致快照（含 `offset`） |
| `GET` | `/views/{name}/skipped` | 该视图的跳过记录（含原因） |
| `POST` | `/views/{name}/rebuild` | 全量重建（与接入、查询并发安全） |
| `GET` | `/status` | 各视图已提交偏移、待处理事件数、重建标志 |

## 运行

```bash
uv sync
uv run pytest tests/          # 乱序/重复/迟到/重建/恢复/并发查询全覆盖
uv run python main.py         # 启动 HTTP 服务
```
