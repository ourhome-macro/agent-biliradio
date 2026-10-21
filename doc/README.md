# 文档索引

先看稳定主题文档；带日期的实施记录描述当时的结果，不代表当前服务运行状态。

## 开发与验证

| 文档 | 内容 |
| --- | --- |
| [质量门禁](quality.md) | pre-commit、PR、main、Golden Set 和文档篇幅规则 |
| [CI 实施记录](ci-implementation-2026-10-21.md) | 本轮变更、验证结果和未完成边界 |
| [Agent 评测](../recommend-radio/doc/AGENT_EVALUATION.md) | 固定样本、评测阈值和 trace 指标 |
| [Golden 扩充](golden-expansion-2026-10-21.md) | 评测自证修复、13 个新案例与行为回归 |

## 系统设计

| 文档 | 内容 |
| --- | --- |
| [架构](architecture.md) | AMEM 总体结构与模块边界 |
| [推荐](recommendation.md) | 播放器、请求约束、候选池和长期画像 |
| [Agent](agent-design.md) | 执行、工具调用与状态管理 |
| [上下文](context.md) | 上下文投影与 prompt 预算 |
| [检索](retrieval.md) | FTS、语义检索、过滤和温度 |
| [存储](storage.md) | SQL、向量与审计投影 |
| [API 契约](api-contract.md) | 写入、查询与推荐接口 |
| [治理](governance.md) | 生命周期、审核和保留策略 |
| [安全](security.md) | 身份、可见性和敏感数据处理 |
| [运维](operations.md) | 启动、监控、评测与故障处理 |

## 专题与历史证据

| 文档 | 范围 |
| --- | --- |
| [Kafka / ES 使用说明](elasticsearch-kafka-usage-2026-10-20.md) | 内容事件、后台任务和搜索权限边界 |
| [事件链路验收工具方案](kafka-es-test-tool-proposal-2026-10-20.md) | 后续 verify / fault / reconcile 工具设计，尚未实现 |
| [长任务与 Trace 核查](long-task-and-trace-review-2026-10-19.md) | 执行租约、恢复、OpenTelemetry 与业务 trace；含续篇 |
| [任务恢复与播放修复](agent-feed-recovery-fixes-2026-10-19.md) | 历史隔离回归证据与运行限制 |
| [框架统一](framework-unification-2026-09-16.md) | 迁移、trace 与运行方式的历史实施记录 |

其他日期记录保留在本目录，按文件名查找。历史方案中的部署方式可能已被新实现取代；判断当前行为应同时核对源码与部署配置。文档不保存业务凭据、签名媒体地址或私人记忆正文。
