# event-analytics-engine-python

事件驱动的实时分析引擎：事件持续到达，引擎一边接收一边维护可查询的派生视图，
同时保留全量重建入口。增量维护与全量重建对同一批事件给出完全一致的结果。

## 架构

```
            ┌─────────────┐   append   ┌──────────────┐
 ingest ──▶ │ validate    │ ─────────▶ │ Journal(追加  │◀── 共享事件流
            │ + 回压       │            │  式日志, 落盘) │
            └─────────────┘            └──────┬───────┘
                                              │ 各自按 offset 拉取
                        ┌─────────────┬───────┴───────┬─────────────┐
                        ▼             ▼               ▼             ▼
                    view A worker  view B worker   view C worker   (重建任务:
                        │             │               │            读日志快照
                        ▼             ▼               ▼            +尾部补齐)
              checkpoint+原子提交 …            每视图独立进度, 互不阻塞
```

- `engine/events.py` — 事件模型与校验，跳过原因枚举。
- `engine/journal.py` — 追加式事件日志（所有视图共享）与原子检查点。
- `engine/projections.py` — 投影协议与内置投影（计数 / 分组求和 / 最新值）。
- `engine/store.py` — 单视图原子提交存储，查询拿一致性快照。
- `engine/engine.py` — 接入、批处理、去重、回压、超时、重建、恢复。
- `main.py` — FastAPI 接口（接入 / 查询 / 重建 / 进度 / 跳过记录）。

## 一致性语义

1. **增量 == 全量**：每个事件按 `event_id` 去重后恰好应用一次；投影的
   `add` 对事件集合可交换、可结合。因此乱序、重复、迟到到达的增量结果，
   与用同一批事件全量重建的结果完全一致。
2. **一致进度点**：`(state, offset)` 作为整体原子提交（单锁内替换），
   查询在同一锁内取出二者。任何时刻读到的都是某个完整已提交批次，
   不会读到半个批次或字段级混合的中间状态。
3. **恰好一次与恢复**：批次完整应用后才先落检查点（临时文件 + fsync +
   原子 rename）再换内存状态。崩溃只可能丢失未提交批次；重启后从最近
   已提交 offset 重放日志，配合持久化的已应用 `event_id` 集合，不重、
   不漏、不留写了一半的视图。已提交进度与未完成批次天然可区分——
   未完成的批次没有检查点。
4. **重建与增量并存**：重建基于日志快照 `[0, hi)` 离线重算，换入时短暂
   暂停该视图 worker，把重建期间到达的尾部事件 `(hi, 当前已提交]` 增量
   补齐后原子换入。重建期间新到的事件不丢，重建结果与增量路径对得上。
5. **多视图独立进度**：所有视图共用同一份日志，各自按自己的 offset 推进；
   接入只追加日志，慢视图不阻塞快视图，也不拖垮接入。
6. **取消与超时**：批次应用超时或引擎关闭时，已处理部分作为**完整子批**
   提交（或整体放弃由下次重放），视图永远停在某个批次边界上。
7. **非法事件可定位**：结构非法（缺字段 / 类型错 / 时间越界）在接入时
   直接拒绝（HTTP 422）；投影级非法（如求和视图收到非数值）按
   `SkipReason` 跳过并记录（事件 ID、视图、原因、细节），可经
   `GET /views/{view}/skips` 查询，绝不悄悄计入。

## 配置项（`EngineConfig`，HTTP 服务可用环境变量覆盖）

| 配置 | 环境变量 | 默认 | 含义 |
|---|---|---|---|
| `batch_size` | `ENGINE_BATCH_SIZE` | 100 | 单批应用的最大事件数 |
| `max_queue` | `ENGINE_MAX_QUEUE` | 10000 | 回压阈值：最慢视图未应用事件数超过即拒绝接入（HTTP 429） |
| `apply_timeout_s` | `ENGINE_APPLY_TIMEOUT_S` | 30 | 单批应用超时，超时提交已完成的完整子批 |
| `poll_interval_s` | — | 0.05 | worker 空闲轮询间隔 |
| `data_dir` | `ENGINE_DATA_DIR` | `./data` | 日志与检查点目录 |
| `skipped_capacity` | — | 1000 | 每视图保留的跳过记录条数 |

## 运行

```bash
uv run python main.py          # 启动 HTTP 服务
uv run pytest tests/ -q        # 运行测试（乱序/重复/迟到/重建/恢复/并发查询等）

# 示例
curl -X POST localhost:8000/events -H 'content-type: application/json' \
     -d '{"event_id":"e1","ts":1.0,"kind":"click","payload":{"group":"g","value":2}}'
curl localhost:8000/views/count_by_kind
curl localhost:8000/progress
curl -X POST localhost:8000/views/count_by_kind/rebuild
```

## 自定义投影

实现 `engine.projections.Projection` 协议即可：`initial_state` / `check` /
`add` / `merge`。约束：`add` 不修改入参、对事件集合可交换可结合、状态
JSON 可序列化；去重（恰好一次）由引擎保证。
