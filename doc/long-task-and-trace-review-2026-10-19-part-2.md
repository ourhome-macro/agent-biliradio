# 长任务保障与 Trace 实现核查（续篇 2）

## 续篇章节

### 4.2 FullTrace 业务与评测投影

| 表 | 内容 |
| --- | --- |
| evaluation_traces | trace 类型、root/parent、用户/session/request、起止、状态、属性 |
| evaluation_trace_spans | 步骤、耗时、输入输出摘要、metrics、错误类型 |
| evaluation_trace_events | 按 sequence 编排的事件及 payload |

对话、推荐、发现建立业务 trace，使用 parent_trace_id/root_trace_id 串树。异步 discovery 以 job_id 作为业务 trace ID，排队时创建，worker 通过 `FullTrace.resume()` 继续，记录 queue_wait。

`PersistedAgentTraceObserver` 把 Agent 事件投影为 `agent-run:<run_id>`，记录 sourceEventId/sourceSequence、模型和工具起止、attempts、token、cost、执行类型等。`durable_jobs.agent_run_id` 提供 job 到 run 的关联。Radio 的 music-operation-router 是确定性控制器，标为 execution_kind=control，不能把其轮次当成真实 LLM 调用。

`record_sampling()` 直接观察 provider sampling，先保存真实 token 用量，再进行业务解析，因此 JSON 解析失败仍保留已发生的消耗。只有配置了价格且 usage 已知才计算成本，未知使用量与未知成本单独计数；汇总优先真实 sampling，避免和 runtime 汇总重复计算。

`measure_step()` 可以用一次计时同时产生 OTel business.step span 和本地业务 span。FullTrace 对敏感键脱敏并限制深度、集合数量和文本长度；超过 64 KiB 的序列化内容保存截断预览和哈希。

业务 trace 是 best effort：批量缓存达到 128 条或退出作用域时 flush；写入异常可能丢弃该批次。observer 异常只增加 observers.failed，不阻塞业务执行。进程被强杀也可能丢失尚未 flush 的业务事件或 OTel span。它们不是恢复流水、审计完整性凭证或精确计费账本。

查询接口需要管理员身份：

- `GET /api/admin/evaluations/traces/<trace_id>`：读取所在业务 trace 树。
- `GET /api/admin/evaluations/metrics`：汇总耗时、token、成本已知性和推荐相关指标。

OTel 的 32 位十六进制 trace ID 与 `trace:...`、`discovery:...`、`agent-run:...` 业务 ID 不是同一个标识。业务 span 带 `business.trace_id` 可辅助关联；本地表没有统一保存每一条记录对应的 otel_trace_id/span_id，不能直接把 X-Trace-ID 当作业务查询接口的 ID。

### 4.3 AMEM 记忆解释

`audit/trace.py` 的 RuntimeTrace 记录选中 memory ID、阻断数量、评分分解、规则版本、config_hash/state_hash、词法/语义候选数、embedding/vector/fusion 耗时及超时原因。它回答“为什么这批记忆进入上下文”，与调用链 span、Agent checkpoint 的用途不同。audit/replay 的事件重放用于核对记忆状态，也不等于重新调用模型和外部工具。

## 5. 上生产必须明确的边界

1. **当前属于有时间上限的后台工作流。** 普通任务是分钟级，媒体任务约一小时。要支撑数小时/数天的复杂任务，需要把工作拆成短阶段，在调度层持久化阶段依赖与后续唤醒，不能只增大 timeout。
2. **Agent 持久化仍是本地 SQLite。** Compose 在同一主机通过挂载共享 `/app/data` 保留它；多机 worker 若各自有独立 agent-runs 文件，就无法共享去重与恢复事实。多机接管需要共享数据库中的 AgentStateStore 与匹配的持久化图 checkpoint，并迁移存量状态。
3. **run fencing 不覆盖每一笔子状态写入。** 当前 save_checkpoint/update_tool_call 依赖版本检查，没有同时验证 run lease。Kafka 子进程在失租后会被取消，但失租检查到进程退出存在窗口；外部业务写入也不能仅靠 finish 的 token 校验保障。要获得严格隔离，子状态与业务提交应校验执行 epoch/token，外部副作用必须使用稳定幂等键或专门对账协议。
4. **待对账不是自动恢复成功。** 非幂等未知结果会阻塞 lane，必须观察、核实并分别解决 job 与 Agent 工具状态。现有 CLI 不会自动把两层状态原子修复。
5. **监控尚无 Trace 查询后端。** 当前 Collector 配置只把 trace 写到轮转 JSON 文件，单文件 32 MiB、max_days=7、max_backups=3；Compose 没有配置 Tempo/Jaeger trace 存储与查询，不能把现有 Grafana 指标看板描述为完整链路检索平台。要做生产排障，应接入持久化 trace 后端并完善 ID 关联。
6. **示例 Kafka 单副本不提供集群高可用。** Compose 的 replication factor 为 1；任务恢复还依赖数据库、Agent 文件、媒体存储与备份可用。进程恢复能力不能替代存储灾难恢复。
7. **不同传输后端保障不完全相同。** 以上心跳失效立即取消、分区 epoch 与隔离取消描述针对当前 Kafka 路径。Redis worker 的续租失败只记警告或返回，默认调用隔离进程时没有传入失租取消事件；RabbitMQ 路径使用另一套硬超时与领取租约，不能直接套用 Kafka 的保证。

## 6. 本次验证

执行以下现有定向回归，结果 **36 passed in 5.15s**：

```powershell
py -3.12 -m pytest -q recommend-radio/backend/tests/test_durable_jobs.py recommend-radio/backend/tests/test_loop_reliability.py recommend-radio/backend/tests/test_full_trace_and_agent_evals.py recommend-radio/backend/tests/test_sampling_runtime.py tests/test_context_constraint_selection.py
```

覆盖业务与 job 原子回滚、发布中断重投、并发领取、旧 token 阻断、lane 顺序、重试退避、同步工具超时未知结果、连续压缩约束、trace 层级/脱敏、provider usage 与解析失败记账。首次沙箱运行因 pytest 临时目录访问被拒而出现 fixture 错误；授权后在沙箱外执行上述同一命令通过。

本次没有启动实际 Kafka/MySQL/Collector 做故障注入，没有运行真实 broker 集成测试，也没有证明线上服务已启用 OTel。上述生产边界来自静态实现核查，不能用本地 36 项回归替代部署验收。

## 分篇导航

- [第 1 篇](long-task-and-trace-review-2026-10-19.md)
- [第 2 篇](long-task-and-trace-review-2026-10-19-part-2.md)
