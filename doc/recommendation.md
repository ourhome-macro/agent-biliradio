# 推荐系统架构

Recommend Radio 将 AMEM 作为记忆与画像运行时，将 B 站作为异步候选供给源。在线推荐不直接调用 B 站搜索，只读取本地已准入库存。

```text
行为/对话
  -> L0 事件
  -> L1 偏好原子 / L2 场景 / L3 稳定画像
  -> RequestSpec + MusicProfile
  -> CandidatePool
  -> 业务打分 + persisted-vector MMR + 探索 + 多样性
  -> impression / feedback / keyword attribution

异步补给：
MusicProfile + RequestSpec
  -> DiscoveryPlanner
  -> 关键词搜索 + 偏好 UP + 相关推荐 + 收藏夹
  -> SongWork / Recording / VideoAsset 归一化
  -> 候选准入 + 文本向量持久化
  -> CandidatePool
```

## 在线职责

| 组件 | 责任 |
| --- | --- |
| `RequestInterpreter` | 将当轮自然语言转为地区、语言、流派、声线、情绪和排除条件 |
| `SceneMemoryService` | 保存带 TTL 的 L2 RequestSpec，只在明确 continuation 时恢复 |
| `ProfileProjector` | 从 AMEM 记忆投影长期偏好、回避项、情绪和探索策略 |
| `CandidatePool` | 按 default/request scope 提供已准入候选，不执行外部搜索 |
| `RecommendationService` | 画像打分、硬过滤、trace、impression 和反馈编排 |
| `RecommendationEngine` | 读取持久化候选向量，执行 MMR、探索和多样性选择 |

## 音乐实体

`tracks.track_id=bvid+cid` 继续表示可播放视频资源。歌曲去重不再把视频 ID 当成作品 ID，也不直接用不可靠的“标题+UP 主”合并，而是采用三级实体：

```text
SongWork       作品级：规范化歌名、歌手
  -> Recording 录音级：原版、Live、Cover、Remix、伴奏及音频时长桶
    -> Track    资源级：BVID + CID
```

实体解析结果包含 resolver version、confidence 和 evidence。无法可靠提取歌手时，作品保持 asset-local，宁可暂时重复，也不做会传播负反馈的错误合并。

实体层用于：

- 同一 Recording 的跳过与近期播放抑制；
- 同一 SongWork 的结果页多样性；
- 多个视频资源共享 Recording 向量；
- 保留 Live/Cover/Remix 的独立偏好。

## 场景与候选作用域

- 明确请求候选属于 `request` scope，只供相同 RequestSpec 使用。
- 首页只读 `default` scope。
- 对话泛推荐最多借入两首有效 L2 请求候选。
- 用户新约束优先于 L2；只有“再来几首”等 continuation 指令恢复旧场景。
- `conversation_warm_memories` 用 `(user, scope_type, scope_key, memory_type, memory_key)` 标识温上下文，`source_session_id` 只承担来源追踪。

温上下文按类型分别失活，不再由一个 `session_id` 同时承担来源、类型和适用范围。

## Discovery

`DiscoveryPlanner` 将 query 分为 entity 与 semantic：

- entity query（歌名、歌手、流派、语言）优先用于 B 站搜索；
- semantic query（舒缓、安静、氛围感）最多使用一个外部 probe，完整语义主要进入本地向量重排；
- profile anchor、相邻流派 probe 和 cold-start fallback 共享固定 search budget。

Discovery 还可以从以下独立供给通道补货：

- 用户喜欢/完播过的 UP 主投稿；
- 喜欢或完播歌曲的相关推荐；
- `RECOMMEND_DISCOVERY_FAVORITE_MEDIA_IDS` 指定的收藏夹。

所有外部接口失败只减少本轮补给，不影响已有 CandidatePool 的在线 serving。

## 向量

候选准入后通过持久化 `content_embedding` 任务异步生成文本向量，文本由规范化作品信息、版本、标题、分 P 标题、UP 主、分区、tags 和 description 组成。Discovery 完成不再等待模型编码，任务由 media 通道消费。在线 MMR 读取已存候选向量，query 编码使用单独的 250ms HTTP 超时参数；向量不可用时采用 lexical MMR 并记录原因。

可选音频管线使用 `content_embedding_jobs`：只有用户喜欢/完播过的 Recording，或高 Yield 关键词产生的候选才进入音频队列。配置 CLAP-compatible `/audio-embeddings` 后，worker 获取音频流并请求 30 秒采样向量。

文本相关性和音频多样性是两个明确空间：

```text
0.45 * normalized business relevance
+ 0.35 * text query similarity
- 0.20 * candidate diversity similarity
- 0.20 * negative-context similarity
```

音频向量覆盖完整时，candidate diversity 使用音频空间；其余项仍使用文本空间。不同空间的 cosine 不直接当作同一尺度混算。

## 反馈与画像生命周期

`shown` 只用于曝光、疲劳和归因，不进入 AMEM 偏好证据。播放、完播、点赞、跳过、收藏和评价可以形成 L0 证据。

```text
同主题证据 >= 3
  -> L1 近期偏好

同主题证据 >= 6 且观察跨度 >= 7 天
  -> L3 稳定画像
```

长期无证据会衰减；相反证据达到阈值会通过 demotion outbox 将旧 L3 supersede，避免本地事务与 AMEM 写入失败造成静默分叉。

## 关键词学习

每个推荐结果保存 keyword、family 和 discovery job 来源。Affinity 对点击、完播、点赞和负反馈使用平滑估计；Yield 综合新颖度、单位搜索供给和准入率。

- 用户反馈默认 14 天 half-life；
- Yield observation 默认 7 天 half-life；
- 未达到最低曝光量的词获得 cold-start bonus；
- UCB uncertainty bonus 控制探索；
- 高/低 Affinity × 高/低 Yield 决定 anchor、rewrite、downweight、retire。

实验关闭时保持生产行为不变。开启后，memory 与 keyword-governance 实验均按 user_id 稳定分桶并持久化 assignment，不能按 session 随机。

## 评测边界

`recommendation_impressions` 保存不可变的线上 slate、排名、score signals、policy 和实验版本；后续反馈通过 `(trace_id, track_id)` 关联。

`scripts/evaluate_memory_runtime.py` 提供两类评测：

1. 固定随机种子的 100-query 受控歧义基准；
2. `--db-path` 驱动的真实 logged-slate 评测，只使用 serving 时保存的快照和之后七天反馈。

真实日志评测不会用未来画像重建过去决策，也不会把未曝光歌曲当负样本。由于当前没有 selection propensity，它只能报告 exposed-slate 条件指标和有限消融，不能声称无偏反事实收益；未来实施 IPS/SNIPS 前必须先记录真实选择概率。

全链路采用 `evaluation_traces / evaluation_trace_spans / evaluation_trace_events`，把 dialogue、recommendation、Discovery、feedback 与通用 Agent run 组织为父子 trace。Golden Set、门禁与指标口径见 `recommend-radio/doc/AGENT_EVALUATION.md`。

## 性能与降级

- 推荐 trace 输出 L2、画像、候选池、打分、MMR、持久化和反馈阶段耗时。
- 画像缓存命中后不重复调用 LLM。
- 对话选曲和补货入队读取已提交画像，首批卡片与事件同事务发布；首连接 SSE 提供只读会话快照。实现及三秒目标的隔离测量边界见[首批结果优化](recommendation-first-result-2026-10-27.md)。
- 文本 embedding 不在在线请求中批量计算候选向量。
- query embedding 不可用或候选向量覆盖不足时使用 lexical MMR。
- Discovery、音频 embedding 和关键词演化都是可失败的异步补给能力，不改变 SQLite 已提交事实。
