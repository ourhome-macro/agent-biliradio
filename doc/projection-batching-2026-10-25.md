# 点赞投影批处理与过渡状态合并

日期：2026-10-25。实现已完成，未部署。批处理改善了积压消化；HTTP 高负载仍有超时，容量验收未通过。

计数的 Redis 落点已进一步核对为 String/SDS，见[存储核对与真实编码实测](counter-sds-audit-2026-10-25.md)。

## 处理语义

- 每批最多 64 个 `feed_projection` 任务，领取同一内容的到期任务，按索引中的 `available_at,id` 排序，使用 MySQL `FOR UPDATE SKIP LOCKED`；不领取其他类型、顺序 lane 或其他 worker 的活跃租约。带 lane 的 anchor 单独处理。
- 领取前等待 100ms 聚合窗口，这是异步投影等待；HTTP 源关系事务的提交不等待窗口。按 `(user_id,content_id)` 取最高版本，计算已投影状态到最新状态的净增减。每个内容每批更新一次计数，计数 version 是快照版本，不再对应事件条数。
- 例如原投影是 neutral，连续 like(v1)→dislike(v2)→neutral(v3)→like(v4)，只更新为 like(v4)，计数净增加一个赞；四个有效事件全部写 Inbox。跨批次的旧版本也不能覆盖新版本。
- 原始任务不删除、payload 不改写；只合并计数投影，行为日志与 AMEM 事件不合并。无效或冲突事件单独保留失败状态，不当作成功消费。
- 投影与在线 rebuild 共用内容/track 锁。关系和 Inbox 批量写入同一个 SQL 事务；Redis 在提交后 pipeline 发布绝对计数和版本化 Bitmap，部分发布失败可重放，不重复计数。
- 批次开始、SQL 提交前及任务完成前检查租约；续期使用相同的行锁顺序。Kafka assignment 丢失后旧 worker 不能完成任务，已提交 SQL 可以重放。后续 Kafka 通知仍校验身份和 input hash，再确认已完成任务。

入口：[生产批处理](../recommend-radio/backend/feed_runtime/projection_batch.py)、[租约](../recommend-radio/backend/feed_runtime/projection_jobs.py)、[投影 SQL](../recommend-radio/backend/feed_runtime/projection_sql.py)。Kafka、Redis/Rabbit 任务分发均接入生产处理器。

## 固定已提交事件量对照

独立真实 MySQL/Redis，单消费者；每组计时前先提交 512 条事件，即 64 用户各 8 次有效状态变化，最终均为 like。A 逐条投影，B 批量合并；三轮按 A/B、B/A、A/B 交替。原始 JSON 的 seed 字段表示轮次，不代表随机输入种子。

这是连续操作集中在积压中的样本，合并比例不能外推到所有真实用户；消费速度不能当作 HTTP RPS。

| 轮次 | A 耗时（秒） | B 耗时（秒） | A SQL 执行次数 | B SQL 执行次数 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 21.282 | 1.549 | 11,649 | 193 |
| 2 | 20.540 | 1.643 | 11,649 | 193 |
| 3 | 21.379 | 1.764 | 11,649 | 193 |

耗时中位数 **21.28→1.64 秒**，下降约 **92.3%**；SQL 执行次数下降约 **98.3%**。B 每轮 8 批、64 次关系更新，合并 448 个过渡状态（87.5%）。SQL 次数含领取、租约校验和完成任务；批量 INSERT 计为一次执行，未声称减少 Inbox 行数。

每组均核验：512/512 任务完成、512/512 Inbox、SQL 与 Redis 最终 64 赞/0 踩。Redis 指标为 A 的 1,024 次单独调用与 B 的 8 个 pipeline 批次；pipeline 内仍有多条命令，不能理解为总命令数只有 8 条。

最终领取排序与索引对齐后另行复核：A **21.01 秒**、B **1.62 秒**，SQL 次数及全部正确性断言一致；这次单组复核未混入三轮中位数。

## HTTP 阶梯复跑与边界

目标 200/500/1000/2000 RPS，各 4 秒，共 14,800 个计划请求。固定 16 个 WSGI 线程、2 个投影执行单元、64MiB MySQL buffer pool；fixture 身份，不含 OIDC 和 Kafka 传输。

| 配置 | HTTP 200 | HTTP 超时 | 客户端丢弃 | 发压返回后收敛（秒） |
| --- | ---: | ---: | ---: | ---: |
| 上轮逐条，投影与 HTTP 共进程 | 2,241 | 0 | 12,554 | 31.31 |
| 本轮批量，10ms 窗口，共进程 | 1,381 | 343 | 13,036 | 0.27 |
| 本轮批量，100ms 窗口，共进程 | 1,455 | 327 | 12,982 | 0.53 |
| 独立消费进程对照 A：逐条 | 1,821 | 81 | 12,867 | 3.86 |
| 独立消费进程对照 B：批量 | 1,865 | 102 | 12,811 | 0.27 |

还存在 503 和少量连接错误，完整状态数见[报告快照](projection-batching-2026-10-25.json)。收敛定义为发压函数返回后，连续三次观察到 pending=0、HTTP/Flask 活跃数≤1；不是单条事件延迟，也不是最后一个请求发出后的精确延迟。

所有轮次最终计数与源关系一致、pending=0。共进程轮次虽追平快，前台明显退化，不能作为上线通过证据。独立进程 A/B 各用两个消费进程，避免消费者 Python 执行直接占用 HTTP 进程；SQL/Redis 监控统计只覆盖 HTTP 进程，不能与旧共进程 SQL 总量混比。

独立进程对照仅一组、固定 A 后 B，成功响应接近且仍有超时，不证明 HTTP 容量提升。不同接收量的收敛时间不能作为严格 A/B 因果结论；主要提速证据来自固定 512 条实验。生产 Kafka 本就使用独立 worker，部署仍须明确 HTTP 与 worker 的资源配额。

## 迁移、验证与复现

`radio_011` 增加 durable_jobs.projection_key 和领取索引，回填旧投影任务并保留 payload。独立 MySQL 演练覆盖 radio_010→011 和重复升级。上线前由管理员执行显式迁移，本次未操作业务库。

最终验证：独立 MySQL **20 项通过**，真实 Kafka/MySQL **9 项通过**；覆盖合并、乱序与重复、Redis 部分发布失败重放、毒消息、租约/assignment 丢失、顺序 lane、并发投影及 rebuild 互斥。`quality.py pr` 退出码 0：Python **360 passed / 60 deselected**、Golden **40/40**、前端 **10 项**及类型检查/构建通过。完整 main 涉及 ES/RabbitMQ 等验收，本轮未执行，不计为通过。

```powershell
py -3.12 scripts/validate_capacity_remediation.py --kafka
py -3.12 scripts/benchmark_projection_batches.py
py -3.12 scripts/benchmark_projection_http.py
py -3.12 scripts/quality.py pr
```

原始记录位于 `artifacts/perf/projection-batches`、`projection-http`、`projection-http-v2`、`projection-http-processes`；快照记录来源与 SHA-256。测量与功能测试分开执行；未提交测试数据库、凭据或业务配置。
