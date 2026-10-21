# 音乐推荐 Agent 全链路 Trace 与 Golden Set

本文定义 Recommend Radio 的可执行评测契约。评测目标不是给模型一个总分，而是同时回答结果、过程、可信和工程四类问题，并保留可以复查的证据链。

## Trace 拓扑

```text
dialogue trace
  -> dialogue.session.prepare
  -> dialogue.route
  -> dialogue.warm_memory.write
  -> recommendation trace
       -> scene_memory.retrieve
       -> profile.project
       -> candidate_pool.read
       -> discovery.plan
       -> candidate.score
       -> candidate.rank_select
       -> candidate.persist
       -> recommendation_trace.persist
       -> feedback trace (shown)
            -> feedback.normalize
            -> feedback.persist
            -> feedback.learn
            -> profile.lifecycle.update
  -> discovery trace (sync/async child)
       -> discovery.queue_wait
       -> keyword_governance.select
       -> bilibili.search / uploader_supply / related_supply / favorite_supply
       -> candidate.admit
       -> candidate.embedding.persist
```

通用 `BusinessAgentRuntime` 可注册 `PersistedAgentTraceObserver`，将 `model.started/completed`、`tool.started/completed`、approval、run terminal event 投影到同一套 trace 表。

## 存储契约

数据库 schema v24 新增：

- `evaluation_traces`：一次 dialogue、recommendation、discovery、feedback 或 agent run；
- `evaluation_trace_spans`：步骤、工具、检索、Memory、排序和存储 span；
- `evaluation_trace_events`：按 sequence 保存状态、动作、工具结果和 terminal event。

每条 trace 保存 `root_trace_id` 和 `parent_trace_id`。异步 Discovery 在入队时创建 queued trace，worker 恢复同一个 trace，因此 `duration_ms` 包含排队等待时间。

Trace 是 best-effort observability，不得让埋点失败破坏推荐请求。落库前会递归脱敏 authorization、cookie、password、secret、token 和 API key，并限制深度、列表长度和字符串长度。原始用户消息默认只记录 SHA-256 与长度；结构化 RequestSpec 可以进入 span。

## 查询接口

以下接口仅允许管理员访问：

```text
GET /api/admin/evaluations/traces/{trace_id}
GET /api/admin/evaluations/metrics?since={ISO-8601}
```

推荐、Discovery job 和反馈响应都会返回 `fullTraceId`。传入任意子 trace ID 时，trace 查询接口返回其完整 root tree。

## 可计算指标

`trace_metrics.summarize_traces()` 当前输出：

### 工程层

- dialogue/recommendation/discovery/feedback/agent-run 的 P50、P95、P99；
- 各 span 的 P50、P95、P99；
- trace 成功率；
- 平均 spans/trace。

### 过程层

- B 站工具调用成功率；
- CandidatePool 命中率；
- 无效召回率：`(result_count - admitted_count) / result_count`；
- MMR lexical fallback 比例；
- 最终曝光的召回源贡献分布；
- keyword governance action 分布。

### 结果层

- completion/like 正向率；
- 基于作品、歌手、版本、tags、genre 的 intra-list diversity；
- 探索曝光比例；
- 探索与利用正向率及未加权 gain；
- propensity 覆盖率。

### 可信层

- Track 到 Recording 的实体接地率；
- 低置信度实体比例；
- 外键可证明的本地资产接地；
- trace 脱敏回归。

死链率仍需要定时回查 B 站，当前不会把“数据库中存在”误报成“线上资源仍有效”。探索 gain 在 propensity 覆盖率不足时仅是描述性指标，不能宣称无偏因果收益。

## Golden Set

Golden Set 位于 `recommend-radio/evals/golden/`：

| 文件 | 定位 | 当前覆盖 |
| --- | --- | --- |
| `capability.v1.jsonl` | 工具、路由、RequestSpec、Memory 检索、实体和 DAG 规划 | 15 cases |
| `business.v1.jsonl` | 固定候选集排序、硬约束、作品去重和多样性 | 3 cases |
| `adversarial.v1.jsonl` | 无效 BVID、跨 tenant 检索、敏感信息、注入、否定和 DAG 环 | 9 cases |
| `regression.v1.jsonl` | 语言否定、舞台范围、场景续接、跨用户 / 会话、实际跳过反馈、讨论路由与嵌套脱敏 | 13 cases |

所有 case 必须包含稳定唯一 `id`、`layer`、`task`、`input` 和 `expected`。首版只使用程序化 judge；在 LLM Judge 与人工标签完成一致性校准前，不将主观模型分数纳入发布门禁。

## 当前门禁

`recommend-radio/evals/gates.v1.json` 定义：

- overall/capability/business/adversarial pass rate = 100%；
- route、RequestSpec、Memory retrieval、entity、scene restoration accuracy = 100%；
- grounded selected candidate rate = 100%；
- ranking NDCG@8 >= 0.90；
- ranking Hit@8 = 1.0；
- intra-list diversity >= 0.45；
- forbidden violation rate = 0。

基线快照位于 `recommend-radio/evals/baseline.v1.json`，保留最初 27 个案例的历史结果。2026-10-21 扩充至 40/40，NDCG@8=0.9684，Hit@8=1.0，intra-list diversity=0.7959。样本组成和评测夹具已变化，不能将这组数与历史基线直接比较为质量提升。

排序用例直接调用真实 RecommendationPolicy，仅 embedding 使用固定向量。候选来源、反馈、数量放在 input；expected 只判定结果，不能被排序器用来排除候选。排序样本使用小规模固定 slate，未覆盖完整线上供给和大列表随机探索。禁止项反馈丢失、预期标签反向过滤与 NaN 指标均有反例测试。详见 [扩充记录](../../doc/golden-expansion-2026-10-21.md)。

## 运行方式

Golden regression：

```powershell
python .\recommend-radio\scripts\run_agent_evals.py --fail-on-gate
```

带真实 trace 聚合：

```powershell
python .\recommend-radio\scripts\run_agent_evals.py `
  --db-path .\recommend-radio\backend\data\bili_radio.sqlite3 `
  --since 2026-09-12T00:00:00+08:00 `
  --fail-on-gate
```

更新显式基线快照：

```powershell
python .\recommend-radio\scripts\run_agent_evals.py `
  --fail-on-gate `
  --output .\recommend-radio\evals\baseline.v1.json
```

测试套件中 `test_agent_golden_set.py` 会执行相同门禁，防止只更新 baseline 文件掩盖回归。

## Golden Set 维护规则

1. 生产事故或错误推荐必须先固化为 failing case，再修代码。
2. 修改 expected 必须说明产品契约变化，不能仅为了让门禁变绿。
3. business case 使用冻结候选全集，必须同时声明 relevant、forbidden 和最小多样性。
4. adversarial case 优先覆盖误报，因为幻觉候选、越权工具和负向约束失效的代价高于漏报。
5. 新版本另建 `*.v2.jsonl`；不要原地改变已用于历史对比的语义。
6. 基线只在 gate 全部通过后更新。

## 局限与下一步

- 当前 Golden Set 是最小回归集，不代表真实用户分布；后续从匿名生产 failure 中持续扩充。
- logged slate 只能评估已曝光内容；做 IPS/SNIPS 前必须记录真实 selection propensity。
- 复听、次日/七日留存需要时间窗口成熟后计算，不能用短期测试数据伪造。
- 实体准确率需要人工标注集和音频指纹复核；当前程序化集重点验证“不确定时不合并”。
- B 站死链率需要低频、限流、缓存的在线巡检任务。
