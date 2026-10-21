# 长任务保障与 Trace 实现核查

核查日期：2026-10-19。依据当前工作区代码，包含尚未提交的修改；未检查线上进程、数据库内容或运行时环境变量。本次只新增说明文档，不修改业务实现。

## 1. 结论

项目已经实现两层任务状态管理：业务数据库中的 durable job/outbox 负责投递与调度，Agent 的领域 checkpoint、工具流水与 LangGraph checkpoint 负责阶段恢复。其保证是“至少一次投递、按身份去重、检查点续跑、安全重试、未知副作用阻断”，不能表述为所有长任务都能自动完成，或所有外部副作用都恰好执行一次。

Trace 同样分层：OpenTelemetry 负责运行链路与耗时，FullTrace 负责业务事件、推荐决策和评测投影，AMEM RuntimeTrace 负责记忆检索解释。恢复任务依赖持久化状态，不依赖 trace。

当前 Compose 主入口使用 MySQL + Kafka；Redis Streams、RabbitMQ/Celery 是代码中保留的其他传输实现。多个模块的 SQLite/Celery 注释已经滞后，应以实际数据库路由和部署配置为准。

## 2. 后台任务如何保障

```text
HTTP 提交
  -> 同一业务事务：写业务状态 + durable_jobs（部分入口同时写 SSE 事件）
  -> relay 领取发布租约并向 Kafka 投递任务身份
  -> worker 从数据库读取权威输入，领取执行租约
  -> 执行具体业务 / Agent 阶段
  -> 数据库提交完成、失败、重试或待对账状态
  -> 提交 Kafka offset
```

### 2.1 任务持久化与重投

- `durable_jobs.enqueue()` 接收调用方的数据库连接，因此业务更新与任务创建可以一起提交、一起回滚。不是先提交业务再依赖一次内存发送。
- `job_id` 唯一，payload 规范化后计算 SHA-256。同一个任务 ID 不能绑定不同 kind、用户或输入。
- relay 使用独立的 `publish_token/publish_lease_until`；MySQL 领取发布批次使用 `FOR UPDATE SKIP LOCKED`。发布租约与执行租约相互独立。
- broker 确认失败可能意味着实际已经接收，因此保留有限发布租约，过期后允许重投。在 worker 领取前，queued 任务也会周期性重投，以恢复消息丢失。
- Kafka Producer 配置 `enable.idempotence=true`、`acks=all`，但这不等于业务副作用 exactly-once。

代码：[durable_jobs.py](../recommend-radio/backend/durable_jobs.py)、[kafka_transport.py](../recommend-radio/backend/kafka_transport.py)、[kafka_relay.py](../recommend-radio/backend/kafka_relay.py)。

### 2.2 领取、心跳、顺序与确认

- worker 校验消息的任务 ID、输入哈希、kind 和 topic，再从数据库取 payload，避免把 broker 消息当成业务权威输入。
- claim 生成唯一 `lease_token`，增加 attempts。Kafka worker 默认租约 120 秒，心跳最多间隔 10 秒续租；失去所有权或续租异常会设置取消标志。
- `finish()` 同时检查 token、running 状态和租约未过期，旧 worker 的迟到结果不能覆盖新 owner 的任务结果。
- 同一业务 lane 按数据库顺序执行；running 或 needs_reconciliation 的前序任务会阻塞后续任务。对话按用户和 session 设置 lane。
- Kafka 每个 partition 暂停读取后处理一条在途消息，poll 线程继续负责消费心跳和 offset。分区撤销时取消旧执行，assignment epoch 防止旧分区任务提交 offset。
- 只有数据库已记录终态或已持久化下一次重试，才提交旧消息 offset。若 SQL 已提交但 offset 尚未提交就崩溃，重复投递通过数据库状态跳过。

代码：[kafka_worker.py](../recommend-radio/backend/kafka_worker.py)、[dialogue_task_service.py](../recommend-radio/backend/dialogue_task_service.py)。

### 2.3 重试与待对账

| 情况 | 实际处理 |
| --- | --- |
| `retry_safe=true` 的普通失败 | 重排 queued，指数退避，延迟上限 120 秒，最多 8 次执行 |
| 安全任务租约过期 | 未达次数上限则重新 queued，达到上限则 failed |
| 非安全任务失败或执行中丢失 worker | needs_reconciliation，停止自动重放 |
| `JobPermanentFailure` | failed |
| `JobReconciliationRequired` | needs_reconciliation，即使外层任务标为 retry_safe 也不重试 |

对话提交默认没有启用 retry_safe，因此对话中断不会自动无条件续跑。Discovery 外层允许重试，但内部 prepare/admit 阶段保守地标记为非幂等，未知结果仍会阻断。`job_admin.py status/resolve` 能查看、登记处理结果，resolve 记录操作人和理由；它只解决 job 状态，不能代替 Agent 工具流水对账。

### 2.4 执行隔离与用户断线

Kafka worker 对 dialogue、discovery、evolution、discovery_watch、media_import 使用独立子进程。普通任务进程期限为 270 秒，media_import 为 3660 秒；失去所有权或达到期限会终止进程树。Windows 用 Job Object，POSIX 用独立进程组。

前端 SSE 只是观察后台任务。事件先落业务数据库，重连携带 `Last-Event-ID` 或 `lastEventId` 后按用户、session 和游标补读；默认保留 7 天。没有游标的新连接从当前最新事件开始，因此不等于自动重放全部历史。连接断开不会撤销数据库中的任务。

代码：[isolated_job.py](../recommend-radio/backend/isolated_job.py)、[web_asgi.py](../recommend-radio/backend/web_asgi.py)、[sse_event_client.py](../recommend-radio/backend/sse_event_client.py)、[sse_event_store.py](../recommend-radio/backend/sse_event_store.py)。

## 3. Agent 内部如何续跑并控制长上下文

### 3.1 两套 checkpoint 的职责

- `music_agent.py` 显式使用 `agent-runs.sqlite3`，保存 Agent run、model turn、checkpoint、tool call 和 approval；通用 `BusinessAgentRuntime` 若不传 state_store，则默认使用内存实现，持久化不是默认自动获得的。
- LangGraph 执行 prepare → model → tool → finish；有持久化 store.path 时使用旁路文件 `<path>.langgraph` 保存图执行状态，否则用 InMemorySaver。
- 图节点每次从领域 store 重新取 checkpoint。若模型结果已经写入领域 checkpoint，而图节点状态尚未保存就崩溃，恢复时跳过已提交的模型步骤。
- 模型结果提交后，checkpoint 保存 messages、待执行工具及 final_output；工具完整结果先写工具流水，再把有界结果追加到模型上下文。
- 同一 tenant/request_id 返回既有 run；输入变化则拒绝。Radio 后台操作用 job_id 构造稳定 request_id，并用用户、请求与阶段生成稳定 call_id。

### 3.2 工具执行流水

`ReliableToolRuntime` 在调用前把工具记为 executing，调用后记为 succeeded 并保存完整输出。恢复遇到 succeeded 直接复用结果；幂等工具允许有限次数重试，非幂等工具每次流水最多执行一次。执行中断且结果未知时，非幂等副作用进入 reconciliation_required。

同步 Python 工具的线程不能靠 asyncio 超时强杀，因此超时且仍在运行时，即使工具宣称幂等，也会标记待对账，避免未观察到的旧调用与重试重叠。通用 runtime 提供审批、工具对账和补偿接口，但补偿依赖工具实现 compensator，不是通用自动回滚事务。

Discovery 已拆成 prepare、search、admit、embed 四个工具阶段；search 为只读幂等，embed 为幂等，prepare/admit 非幂等。恢复粒度是已提交阶段，不能从一次外部搜索或 LLM 请求的任意字节位置继续。

### 3.3 租约、预算与上下文

- Agent 自身还有一层默认 30 秒的 run 租约，每约 10 秒续租；run 更新校验版本和租约 token。
- 默认策略限制步骤、模型/工具调用次数、输入输出 token、运行时间和费用预算。Radio operation 覆盖为单次执行时间 220 秒、单工具 210 秒、工具最多尝试 3 次，步数按阶段数设置。时间限制作用于一次执行入口；恢复后会重新建立该计时，不能把它理解为跨恢复累计的墙钟截止时间。
- 模型调用前计算输入与预留输出预算。到上下文阈值后，压缩旧消息，保留系统消息、原始任务、当前意图、有效约束、历史摘要以及最近完整消息组，避免破坏 tool call/result 协议。
- 约束带版本和 superseded 状态，例如“不要 Rap”随后被“现在可以 Rap”替代，连续压缩也保留新方向。
- 工具输出给模型的是有预算的节选和引用，完整 JSON 留在工具流水中。压缩后仍超预算会拒绝模型调用，而不是无限延长任务。

代码：[runtime.py](../src/agent_memory_runtime/agent/runtime.py)、[graph_driver.py](../src/agent_memory_runtime/agent/graph_driver.py)、[sqlite.py](../src/agent_memory_runtime/agent/stores/sqlite.py)、[tool_runtime.py](../src/agent_memory_runtime/agent/tool_runtime.py)、[context_window.py](../src/agent_memory_runtime/agent/context_window.py)、[music_agent.py](../recommend-radio/backend/music_agent.py)、[discovery_workflow.py](../recommend-radio/backend/discovery_workflow.py)。

## 4. Trace 如何记录和串联

### 4.1 OpenTelemetry 运行链路

`telemetry.py` 配置 TracerProvider；配置 `OTEL_EXPORTER_OTLP_ENDPOINT` 后通过 OTLP/HTTP 批量导出到 `/v1/traces`。未配置 endpoint 时没有外部 exporter。

Flask、requests 和 gRPC 有自动 instrumentation；显式 span 覆盖 Agent prepare/model/tool/finish、music operation/stage、任务执行和业务步骤。HTTP 响应的 `X-Trace-ID` 返回当前有效 OTel trace ID。

`enqueue()` 把当前 W3C `traceparent/tracestate` 存入 `durable_jobs.trace_context`。Kafka worker 从该字段恢复父上下文；隔离子进程也提取该字段。所以请求结束、消息重投或进程切换后仍可挂回原始 trace。当前 Kafka Producer 本身没有显式 publish span，不能声称所有 Kafka 发布阶段均已被观测；RabbitMQ 发布路径有独立 messaging.publish span。

OTel span 不主动记录 prompt、Cookie、凭证或异常消息，仅记录错误类型和状态。HTTP hook 去掉 query、userinfo；这不代表对所有业务文本实现完整敏感信息识别。

代码：[telemetry.py](../src/agent_memory_runtime/telemetry.py)、[telemetry_setup.py](../recommend-radio/backend/telemetry_setup.py)、[kafka_worker.py](../recommend-radio/backend/kafka_worker.py)、[isolated_job.py](../recommend-radio/backend/isolated_job.py)。

## 分篇导航

- [第 1 篇](long-task-and-trace-review-2026-10-19.md)
- [第 2 篇](long-task-and-trace-review-2026-10-19-part-2.md)
