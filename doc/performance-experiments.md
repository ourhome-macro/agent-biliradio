# 隔离容量与 A/B 实验

日期：2026-10-25。全部模块和五组 A/B 已执行，见[容量结果](performance-results-2026-10-25.md)与[A/B 详情](performance-ab-2026-10-25.md)。代码使用独立原生 MySQL、Redis、MinIO 和新建 Kafka 容器，不连接已有业务基础设施，不使用私人账号或模型密钥；发现的超载与失败保留，不标记为容量通过。

## 范围

| 优先级 | 实验 | 实际口径 |
| --- | --- | --- |
| P0 | 点赞 200/500/1000/2000 RPS | HTTP、真实 MySQL 状态、SQL 任务投影与 Redis Bitmap；Kafka 吞吐独立测 |
| P0 | Feed 集中/均匀访问 | 分开测元数据缓存组件和完整 10 卡页面，计数实际 MySQL 语句 |
| P0 | 16/32/64/128/256 路流代理 | 受控上游、Range 初播/拖动/切换/断开，记录连接上限导致的拒绝 |
| P1 | Kafka 超载与恢复 | 独立 broker、生产代码消费循环与持久任务，记录 lag 与恢复 |
| P1 | 并发 UGC 直传 | 不同大小的有效音频、真实 MinIO，区分直传与后台确认延迟 |
| P0/P1 | HotKey、Singleflight、分层缓存、PlayURL | 同一输入与资源预算，控制变量，保留每轮原始数据 |
| P1 | AMEM 排序 | 固定可审查标签，生产排序器与画像投影；不冒充线上 CTR/完播率 |

## 指标约定

- HTTP 延迟从计划到达时刻算到完整响应，包含调度和排队；不只报告成功请求 P99。
- 超时、传输失败、服务端 503 与压测机因在途上限丢弃的请求分别记录。生成器吞吐未达到目标时不能称为服务已承受该目标 RPS。
- 请求成功必须核对返回业务状态；点赞计数通过当前关系表独立重算，等待投影清空后再判断正确性，另外报告收敛时间。
- 缓存命中率以实际 cache.get 调用为分母；SQL 语句、SELECT、loader 次数、合并次数分别计量。
- P95/P99 用原始请求集合计算，不把不同轮次分位数直接合并成一个分位数。随机种子和 A/B 顺序保留，差异不稳定时不判定胜出。
- 资源值包括采样范围和进程角色。模拟播放器的网络压力不等于真实 B 站 CDN 的带宽、限流或可用性。
- 暂无真实用户曝光、点击和观看实验数据，CTR/完播率应为“未测”，不能用合成点击补齐。

## 入口

```powershell
py -3.12 scripts/run_perf_suite.py --phases reactions,cache --quick --output artifacts/perf/p0-smoke
py -3.12 scripts/run_perf_suite.py --phases cache --output artifacts/perf/cache
py -3.12 scripts/run_perf_suite.py --phases reactions,stream,kafka,uploads,amem --output artifacts/perf/modules
py -3.12 scripts/summarize_perf.py --cache artifacts/perf/cache --main artifacts/perf/modules
```

快速模式用于验证工具正确性，不代表长期稳态容量。执行中结果位于指定目录的 `summary.json`，原始请求另存；基础设施在独立临时目录中，结束后关闭并清理，不提交凭据、数据库副本或业务配置。

本次最终缓存来自 `artifacts/perf/full`，其余最终项目来自 `artifacts/perf/remaining`；初轮 smoke 仅用于工具验证与边界探索。缓存为 3 种子对照；完整页面/点赞为单次阶梯探索，不提供容量置信区间。

交付沿用 `ci/unified-quality-gate` 分支与 PR #7。仅提交隔离实验工具、度量回归和报告快照，原始请求保留在 artifacts 并记录 SHA-256；不提交测试数据库、凭据或工作区原有修改。
