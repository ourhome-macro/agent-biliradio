# Agent 与 Feed 修复提交记录

日期：2026-10-19。目标分支：`develop/memory-runtime-simplification`。

本次提交包含 Agent 租约写入隔离、可重入工具对账和 job 收尾恢复，以及 Feed 冷源转存、上传失败续播、签名刷新和分片代际检查。

修复依赖此前工作区尚未提交的 Kafka outbox/worker、业务迁移、统一权限、搜索与内容接口等基础代码，本次一并纳入可构建源码、测试和部署文件。应用 README 同步当前 Kafka/MySQL 部署入口，并将已验证但被忽略规则排除的上下文约束回归测试纳入版本控制。

保留原有 `doc/README.md`、`doc/context.md` 的未提交改动。历史截图、历史验收报告及其他本地分析文档保持原状；本次仅提交当前分析、修复说明与对应的 JUnit 和浏览器结果。私人环境文件、服务数据、二进制程序与构建目录不纳入提交。

验证依据：

- 隔离回归：324 passed、1 skipped，详见 [JUnit XML](agent-feed-recovery-regression-2026-10-19.xml)。
- 实际 HLS 解码的 Chromium 检查：7 项通过，详见 [浏览器结果](cold-playback-recovery-browser-2026-10-19.json)。
- 前端生产构建与类型检查通过。
- 真实 Kafka/MySQL 等实例故障验收因本机服务不可用尚未完成，详细边界及对账命令见 [修复说明](agent-feed-recovery-fixes-2026-10-19.md)。

提交信息采用中文，保留上述验证范围及部署限制。
