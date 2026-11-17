# 垃圾邮件审核 · 模型版本固定 + 截止时间感知批处理 + 安全退役

FastAPI + NumPy 的垃圾邮件审核运维控制台。网页可切换现行模型、对非现行版本发起**退役**、
并发提交至多 16 封邮件（每封独立 100–1000ms 截止时间），单工作器按 **模型版本** 组批推理，
逐条展示分数、分类、模型版本与批次。

## 核心契约

| 需求 | 实现位置 |
| --- | --- |
| 排队邮件按「接单时」的版本分类 | 网关 `_accept_entry` 接单时快照 `active_version` 写入请求；工作器只读该字段 |
| 网页切换现行模型 | `POST /api/model` + 前端模型卡片 |
| 非现行版本退役，页面显示「退役中 / 已卸载」 | `POST /api/models/{version}/retire` + 退役面板/卡片状态徽标 |
| 退役开始即禁止切回、禁止新邮件绑定 | 网关 `ops_lock` + `worker.block_version`；切换/接单返回 409 |
| 已接单邮件（含尚未入队/派发的）按原模型与各自截止时间结算 | 版本在接单时固定；退役只拦截新接单，旧 WorkItem 跑完为止 |
| 只有排队、执行、迟到线程全部结束才卸载运行时模型 | `Worker.wait_drained`（queued+executing+late_threads 归零）后 `registry.unload` |
| 切换与退役并发：先生效的决定为准，另一方明确拒绝 | 同一把 `asyncio.Lock` 串行化两类决定，败者返回 409 |
| 并发提交 ≤16 封、每封 100–1000ms 截止 | `SubmitIn.emails`（`max_length=16` / `ge=100, le=1000`）|
| 逐条查看分数/分类/版本/批次 | 结果表 + `GET /api/jobs/{id}` |
| 单工作器按版本组批，每批 ≤4、等待 ≤200ms | `app/batching.py` 的 `_plan` / `Worker` 默认参数 |
| 新请求立即用新版，未发批的旧请求用旧版 | 版本在接单时固定；批次按版本分区 |
| 批内不混版本 | 队列按版本分桶，`_plan` 每组批只取同一版本 |
| 最早一封快到截止时间时提前发批 | `dispatch_guard_ms`（默认 25ms）提前派发 |
| 过期请求不占后续批次 | 每个请求有独立定时器，到期即解析并从队列移除 |
| 迟到结果不得改判成功 | 批次内 `wait_for` 按各自剩余时间截断 + 完成后显式比对截止时间 |
| 同批其他邮件按各自截止时间返回 | 批内并发打分，逐条独立判定/独立完成 |

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

镜像内已固定 `models/v1.json` 与 `models/v2.json` 两份不同的英文词袋线性模型，同时以只读卷
挂载 `./models` 便于查看。Compose 只起 **一个** 副本、uvicorn 也只有 1 个 worker——批次调度
与「现行模型」指针都是进程内状态。

### 本地运行

```bash
python3 -m pip install -r requirements.txt
python3 scripts/build_models.py          # 重新生成模型（可选，仓库已含产物）
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

### 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q                     # 34 项：调度语义 + 网关行为 + 退役生命周期
python3 scripts/selfcheck.py             # 对运行中的服务做端到端自测（默认 :8000，含退役小节）
```

## 模型

两份模型都是 `bow-linear.v1` 格式（冻结词表 + IDF + 线性权重 + 偏置 + 阈值），推理只用
NumPy，网关与测试结果逐位一致。

| 版本 | 说明 | 词表 | 阈值 |
| --- | --- | --- | --- |
| `v1` | 2024 baseline，只有通用促销词 | 27 | 0.50 |
| `v2` | 2026 hardened，加入 crypto/parcel/urgency 词，权重重拟合 | 63 | 0.60 |

同一封「加密货币投资」邮件在 v1 下是 ham（0.20 左右），在 v2 下是 spam（0.87 左右），
所以版本固定能在页面上直接看出来。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 审核控制台 |
| GET | `/healthz` | 健康检查 + 现行模型（只列已加载版本） |
| GET | `/api/models` | 模型列表，含生命周期状态（`loaded/retiring/unloaded`）与权重摘要 |
| POST | `/api/model` | 切换现行模型 `{"version": "v2"}`；退役中/已卸载版本返回 409 |
| POST | `/api/models/{version}/retire` | 退役非现行版本（202）；排空后自动卸载，重复/现役退役返回 409 |
| GET | `/api/state` | 工作器指标、退役记录与排空计数、按版本统计、最近批次、最近任务 |
| POST | `/api/jobs` | 异步提交（202），返回任务与接单进度；现行版本退役时返回 409 |
| GET | `/api/jobs/{id}` | 任务结果 |
| POST | `/api/classify` | 同步提交并等待结果（脚本/自测用） |
| GET | `/api/batches` | 最近批次记录 |
| POST | `/api/demo/interleaved` | 一键交错演示：v1 接单 → 切 v2 → 新请求 + 慢推理 |

提交体示例：

```json
{
  "label": "人工抽检",
  "emails": [
    {"text": "Congratulations! You are a winner.", "deadline_ms": 900},
    {"text": "Hi team, attached is the quarterly report.", "deadline_ms": 300,
     "submit_after_ms": 50, "simulate_latency_ms": 0}
  ]
}
```

`submit_after_ms`（0–5000）与 `simulate_latency_ms`（0–5000）是演示用：前者错峰接单，
后者模拟慢推理，用来观察「批次执行中超时 + 迟到结果丢弃」。二者只影响时点，不改变批处理
契约。

## 退役与安全卸载

模型版本有三种生命周期状态，`GET /api/models` 与 `GET /api/state` 都会返回：

1. **loaded（在用/可切换）**：运行时注册表里有完整模型，可被切为现行版本。
2. **retiring（退役中）**：调用 `POST /api/models/{version}/retire` 后立即进入。
   - 现行版本不能退役（先切走再退役，返回 409）；
   - 该版本立即被 `Worker.block_version` 拉黑：新邮件不得绑定、`POST /api/model`
     切回返回 409，工作器直接 `submit` 抛 `ModelRetiredError`；
   - 已接单的邮件不受影响——即使还没派发，也继续按接单时固定的版本与各自截止时间结算；
   - 网关在每次接单前复查拉黑集合，因此带 `submit_after_ms` 的延迟接单也不会落到退役版本上，
     被拦截的条目以 `status=error / reason=model_retired` 结算，不占用该模型推理。
3. **unloaded（已卸载）**：后台退役任务调用 `Worker.wait_drained(version)`，等该版本
   **排队（queued）+ 执行中（executing）+ 迟到线程（late_threads）** 三类活动全部归零后，
   才执行 `ModelRegistry.unload` 释放 NumPy 权重并打上时间戳。此后 `/healthz` 与运行时注册表
   都不再包含该版本，仅在 `/api/models` 保留一条「墓碑」记录供页面展示。

### 为什么迟到线程会拖住卸载

`asyncio.wait_for` 超时只会取消 *await*，底层线程池里的打分线程（Python 线程不可抢占）仍在
运行。工作器用 `run_in_executor` 拿到线程 future，并以 `asyncio.shield` 防止它在超时时被
错误标记为已完成；future 由 `_scorer_threads` 持有强引用、真正返回时回调移除并通知排空事件。
因此「请求已超时但线程还在跑」的窗口会被计为 `late_threads`，模型在此期间保持 `retiring`，
绝不会在活线程下被卸载。迟到结果回到工作器后只做丢弃标记，不能把 `timeout` 改回 `ok`。

### 切换与退役并发

切换（`POST /api/model`）与退役共用网关内的一把 `asyncio.Lock`：先拿到锁的决定先生效，
后到的操作拿到明确的 409 与中文原因，不会出现「先退役又被切回」或「新邮件绑到已卸载版本」。

## 调度语义细节

- 队列按版本分桶；每轮只派发 **最早到期** 的那一批，避免一个慢批次吞掉另一个版本的截止时间。
- 组批规则：同版本队列满 4 封立即发批；不足 4 封时等 `min(最老入队 + 200ms, 最早截止 − 25ms)`。
  已经等满 200ms 的请求优先成批（防饿死），剩余名额用同版本 EDF 工作填充。
- 派发时同步从队列摘除并取消该请求的定时器，执行阶段由 `asyncio.wait_for(剩余时间)` 与
  完成后的显式截止检查共同判定；超时结果先到就先解析，迟到结果只做
  `late_results_discarded` 标记，**不会** 把 `timeout` 改回 `ok`。
- 批内并发打分，因此单封慢邮件不会拖住同批其他邮件；每封各自按自己的截止时间返回。

## 已知限制

- 状态全部在进程内（单工作器要求），重启会丢失进行中的任务、切换与退役历史；没有持久化，
  已卸载的模型重启后会随 `models/` 目录重新加载（进程内 `reset_runtime` 仅供测试复位）。
- 模型产物是仓库内预置的两份静态 JSON，没有训练流水线；卸载只移除运行时对象，不删除磁盘文件。
- 演示用的 `simulate_latency_ms` 通过执行器线程中的 `sleep` 模拟，结束的线程不会被强制中断
  （Python 线程无法抢占）；退役会等它自然结束，其结果则按迟到丢弃。
