# Kafka / Elasticsearch 自动化验收小工具方案

日期：2026-10-20。本轮对照现有测试提出方案，仅新增文档，未实现 CLI 或执行故障注入。

## 要解决的问题

项目已有 Kafka/MySQL 与搜索权限测试，新增工具应补齐真实跨系统验收、可控故障和最终状态对账，避免重新实现一套业务逻辑或只检查服务端口。

建议做一个 Python CLI：`recommend-radio/scripts/check_event_pipeline.py`，内部复用业务服务、Relay、消费者和 ES 客户端；pytest 继续维护契约测试。以下命令均为拟议接口，当前不可执行。

## 1. verify：真实链路验收

```text
python recommend-radio/scripts/check_event_pipeline.py verify --env-file <test-env> --output <report-dir>
```

每轮创建独立 SQL schema、Kafka topic prefix / group 和 ES index，启动指向这些资源的 Relay、搜索和缓存消费者。使用真实组件；用户账号固定为作者 Alice 和访问者 Bob。业务输入通过正式服务方法或真实 HTTP API 进入，不通过直接拼接 SQL 模拟发布。

| 场景 | 必须观察的结果 |
| --- | --- |
| 发布公开 UGC | SQL 和 Outbox 同事务存在；自然消费后 Bob 可通过 search / suggest 检索 |
| 编辑标题 | 新查询最终匹配新标题，响应中的正文来自 SQL 当前版本 |
| 公开转私有 | 暂停索引消费者，保留旧 PIT 和候选缓存；新发起的 Bob 查询仍不能拿到目标内容，Alice 可访问自己的已发布私有内容 |
| 删除 / 下架 | 索引最终形成无正文 tombstone；旧版本 ES 写入被拒绝 |
| SQL 授权读取失败 | 不从缓存正文返回成功结果；以接口错误契约判定 |
| 普通 B 站内容准入 | 经 `FeedRepository.admit()` 入库，未经手动 rebuild 或直接 projector 调用即最终可搜；当前实现预期会暴露事件缺口 |
| 多消费者组 | search-v1 和 cache-v1 分别完成同一事件的处理；不能只检查一个组 |

链路延迟测试不调用 `_refresh`，否则会掩盖自然搜索可见延迟。仅测试 ES 版本写入等底层契约时允许显式刷新。

## 2. fault：按明确边界注入故障

```text
python recommend-radio/scripts/check_event_pipeline.py fault --case relay-after-ack --env-file <test-env> --output <report-dir>
```

| 故障点 | 操作 | 核心断言 |
| --- | --- | --- |
| broker 已 ACK，Outbox 尚未标记 published | 测试 Relay 在该边界发出就绪信号，终止该进程并重启 | 事件不丢，允许重复；消费效果最终收敛 |
| ES 写入成功，Inbox SQL 尚未提交 | 阻断该测试消费者的 Inbox 提交，恢复后重试 | offset 未提前确认；ES 可重复 prepare；Inbox 只留一条身份记录 |
| Inbox 已提交，offset 尚未确认 | 在测试消费者内的该边界暂停后终止进程 | 重投后识别已有 Inbox，不重新产生业务效果 |
| 执行中的 worker 失去分区 | 启动第二实例并触发真实 rebalance，释放旧执行的等待屏障 | 旧 assignment 不确认 offset；旧 token 不能完成新 owner 的任务 |
| ES 不可达 | 对测试 worker 的 ES 请求故障适配器注入连接错误，或阻断其独立测试连接 | 不确认未完成投影；恢复后积压排空且结果正确 |
| 同内容快速更新 | 使用多个 Relay / worker，发布多个递增版本并重放旧消息 | 最终版本不回退，聚合消费 head 单调前进 |

异常注入用于验证逻辑分支；真实进程终止、真实 rebalance 用于验证生命周期行为，报告中必须区分。故障不依赖随机 sleep；采用“已到边界”屏障、父子进程握手和有截止时间的条件轮询。

故障适配器只存在于测试 harness，不给生产服务增加公开的崩溃 HTTP 接口。终止操作只针对本轮启动并记录的进程，不按通用进程名或共享服务名批量停止。

## 3. reconcile：只读状态对账

```text
python recommend-radio/scripts/check_event_pipeline.py reconcile --manifest <run-manifest> --output <report-dir>
```

只对账本轮创建的资源，检查 SQL 内容当前 version、permission_version、状态与 ES 当前文档是否一致；检查事件是否已发布、各消费者 Inbox / head 是否达到预期，以及是否仍有待投递和处理中记录。

对账必须先确定水位：停止本轮写入，等待目标事件发布、预期消费者完成并达到 offset 屏障；超时则明确失败。避免把正常异步延迟直接判成永久不一致，也不能用“Kafka lag 为零”替代 SQL / ES 内容一致性检查。

删除内容对账检查的是带正确版本的 tombstone，不是文档不存在。私有内容允许保留可供作者检索的索引正文，权限测试必须按访问者身份判断。保留的 Outbox / Inbox 历史行不算积压。

该命令只报告漂移，不自动修复，以免掩盖漏发事件。修复或重放可在后续另设显式命令。

## 4. 统一结果和可复现性

每轮输出 JSON、Markdown；已有 pytest 运行结果可附 JUnit XML。至少记录：

- runId、代码版本和相关配置摘要；若工作区有未提交变更，记录 dirty 状态及相关源码指纹。
- 场景、检查点、注入边界是否真正到达、失败原因、超时阈值。
- 内容版本、eventId / sequence、消费者组、分区 / offset、SQL 状态及 ES 版本证据。
- 提交到 Outbox 发布、消费完成、自然搜索可见的耗时；采样充足时再报告 p50 / p95 / p99。
- 缺失事件数、重复业务效果数、版本回退数、越权结果数、清理结果。

区分 passed、failed、blocked / skipped：组件不可用不能报通过；预期场景未执行应导致完整验收模式非零退出。判定依据是已声明的业务不变量；外部 prepare 的重复次数可以大于一，不应直接当作重复业务效果。

使用 seed 生成状态序列时，把 seed 和操作序列写入报告，便于重现“公开→私有→编辑→删除→重放”的组合问题。

## 5. 隔离和清理

工具显式装载测试环境，启动子进程时覆盖业务数据库、Redis、Kafka、ES 和应用数据目录配置，避免继承本机默认权威库。测试 manifest 精确记录本轮创建的资源，清理仅操作这些资源。

创建 SQL schema、Kafka topic、ES index 需要的管理权限与业务运行权限分开。报告和异常中对 DSN / 密码脱敏；清理失败保留 manifest 并明确报错。当前 Kafka/MySQL fixture 清理了 SQL schema，但没有删除它创建的 Kafka topics，新工具应补上自身 topic 生命周期管理。

## 6. 复用现有代码与实施顺序

可复用的现有测试：

- `backend/tests/test_kafka_mysql_integration.py`：真实 Kafka/MySQL，覆盖 SKIP LOCKED、旧 token、重复投递、SQL 后 offset、rebalance、prepare 失败恢复和 Relay 顺序。
- `backend/tests/test_domain_outbox.py`：Outbox 回滚、去重与租约等快速契约测试。
- `backend/tests/test_content_search_policy.py`：候选缓存、PIT、权限、当前正文和 tombstone 契约。
- `scripts/check_content_search_live.py`：真实 ES 的角色、PIT 与版本验收；它通过直接 projector 调用驱动索引，不能替代完整 Kafka 索引链路验收。

推荐先交付 verify，优先锁住普通内容准入事件缺口和 UGC 权限撤销；再加入三个提交边界的 fault 场景；最后加入 reconcile 和带 seed 的长时间状态序列测试。先验证正确性，再依据实际运行数据设置吞吐和延迟门槛。
