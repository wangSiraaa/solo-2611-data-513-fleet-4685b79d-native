# 死信重驱：投递身份、入仓效果复用与 CAS

生产起点（`deadletter_service.py` 的一发一收、`event_loader.py` 的单次加值）已扩展为可持久化、可重驱、可恢复的实现。全部运行于 Python 3.11 + 内置 SQLite，不依赖真实消息代理。

## 运行

```bash
cd materials/513
python3.11 demo.py          # 材料原有正常路径（+5，写临时库）
python3.11 test_redrive.py  # 真实 SQLite 行上的完整核对（41 项）
```

写入都在临时目录，源文件保持只读。

## 数据模型（同一 SQLite 库）

| 表 | 作用 |
| --- | --- |
| `facts` | 业务事实行：`key`, `value`, `version` |
| `messages` | 不可变收件箱，主键 `(origin_id, revision)`，正文一旦写入不允许 UPDATE/DELETE（触发器强制） |
| `effects` | 每个消息身份一条成功回执（原消息回执），与 facts 更新同事务写入；不可变 |
| `deliveries` | 每次投递尝试一行，独立 `delivery_id`，保留全部历史 |
| `message_state` | 每个消息身份的 pending / inflight / acked / dead、当前 `delivery_id`、错误分类 |

`messages` 与 `effects` 上的触发器使“修改原消息记录”或“覆盖旧成功回执”直接失败。

## 关键语义

### 身份与正文
* 消息身份是 `(origin_id, revision)`；同一身份重复出现时正文必须逐字段一致，否则返回 `permanent/identity_body_conflict`，不改库。
* 超时重发、重复投递、从死信重驱都创建新的 `delivery_id`，保留消息身份与正文。

### EventLoader（`event_loader.py`）
* 校验顺序：先类型/范围，再 CAS。`key` 非空字符串；`delta` 必须是 `int` 且非 `bool`、在 int64 内；`revision >= 1`。
* 初次不存在的 `key` 视为 `(value=0, version=0)`。
* 仅当 `expected_row_version == 当前 version` 时执行 `value += delta`、`version += 1`，否则返回 `stale` 且不修改事实。
* `value + delta` 溢出 int64 视为永久错误。
* facts 更新与 `effects` 回执在同一个 `BEGIN IMMEDIATE` 事务提交。
* 已有回执的身份直接返回首次结果（`replayed`），**不再**按当前行重新做 CAS 或再次加值；即使之后人工改过行也如此。
* 非法类型/溢出 → `permanent`；受控 busy（`RetryableBusyError` 或 SQLite locked/busy）→ `retryable` 且无效果；其它未知异常也按 `retryable` 处理，绝不当作明确业务拒绝。

### 结算映射（`deadletter_service.py`）
* `applied` / `replayed` → 投递 acked，消息 acked。
* `permanent` / `stale` → 投递 dead，消息 dead，并记录 `failure_class`。
* `retryable` → 在本轮尝试上限内回 pending；超过 `max_attempts`（默认 3）或硬上限 5 次投递后进入 dead。
* 结算以 `delivery_id` 为键；已结算或已被新尝试取代的旧投递再结算只返回 `ignored`，不会改动新的 inflight 投递或消息状态。

### 未知结果的依据
本实现选择**单库**：facts、效果回执和投递账本在同一个 SQLite 数据库中，效果与事实同事务提交。因此“提交后、ACK 前崩溃”时唯一权威是 `effects` 中的回执：

* `recover()` 先读回执：存在 → 直接确认，不重复执行；不存在 → 说明事实事务未提交，才允许新尝试。
* `inflight` 或缺失结算只是**未知**，永远不推断成业务拒绝。
* 若把 `store` 换成独立库的效果接口，则不再声称跨库恰好一次传输；恢复仍以效果接口为准，可能出现重复执行风险，由效果接口自身去重。

### 重驱（`redrive`）
* `choice="original"`：原消息身份、revision、正文原样重投，不偷换 revision。
* `choice="corrected"`：仅当该 origin **尚无成功效果**时允许；创建 `revision+1`，保存完整正文，并保留 `predecessor_revision` 关联；不修改已存在的原消息行，也不能把修正正文套到旧成功回执上。
* 修正消息必须由管理员显式给 `expected_row_version`；服务**不会**读取当前最新版本代填。
* 修正投递仍执行 CAS：失败以来人工改过 facts 时，旧 expected 版本返回 `stale`，不覆盖人工修改；管理员须给出新预期版本后再重驱下一修订。

### 隔离
每个 `(origin_id, revision)` 独立结算；单个消息返回 pending 或 dead 不影响其他 key 的投递。

## 范围限制
* 每原消息最多 3 个 revision（含修正），最多 5 次投递；`MAX_REVISIONS` / `MAX_DELIVERIES` 常量控制。
* 未实现完整队列、租约续期、历史补偿/撤回。

## 验证覆盖（`test_redrive.py`）
1. +5 首次投递、重复投递回放首次回执、值类型错误永久入 dead、人工改值。
2. 原始 SQLite 行核对加值次数、版本、效果回执与全部历史投递。
3. commit 后 ACK 前崩溃 → 恢复读回执确认，不重复加值。
4. 超时重发产生新 delivery；旧 delivery 迟到结算被忽略，新 inflight 不被修改。
5. 原文重驱：身份/正文不变，仍按当前 row version 检查。
6. 修正重驱：旧 expected 版本 stale 不覆盖人工值，显式新版本后成功；拒绝在已有成功效果上修正；原消息行保持原样。
7. 受控 busy → retryable/pending，重试后只生效一次。
8. 一个 key 阻塞时另一 key 正常投递。
