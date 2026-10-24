# Feed 缓存、MinIO 与信息流链路

更新日期：2026-10-25。本文按当前源码核对；未连接运行中的业务服务，配置数值为代码默认值，不代表部署现值。

## 三层存储分别保存什么

严格说是“进程缓存 → Redis 缓存 → 权威业务库”，第三层不是缓存。

| 层次 | 保存内容 | 生命周期与范围 |
| --- | --- | --- |
| 进程缓存 | 公开内容的 `metadata_json` 解析字典，如歌曲标题、作者、来源、BVID/CID 等 Track 字段 | 每个 API 进程独立；2 秒；最多 2048 项；只在 `shared=true` 时使用 |
| Redis 内容缓存 | 与进程缓存对应的内容元数据，附版本和软硬失效时间 | 默认软期限 60 秒、硬期限 300 秒；实际硬期限乘 0.8～1.0 抖动；公开共享，私人内容按用户隔离 |
| Redis 页面缓存 | `feed_items` 记录，包括内容 ID、页内顺序、item ID、推荐决策快照 | 按用户、会话、页号隔离；期限不超过会话剩余时间；不进进程公共缓存 |
| Redis 互动投影 | like/dislike Bitmap、互动版本，以及计数快照 | 与内容/页面缓存并列；页面渲染中的计数仍取 SQL 当前行 |
| SQL 业务库 | 内容目录、权限和版本、Feed 会话/固定页/条目、媒体资产状态和对象引用、互动关系、计数、观看事件及任务状态 | 权威持久数据；代码支持 MySQL 和 SQLite，具体取决于数据库配置 |

页面缓存与内容缓存属于同一个 Redis 层，不是两个上下级缓存。也不是每次请求都会完整经过三个层次。

公开内容 key 包含内容版本与权限版本；私人内容 key 还包含用户身份。命中旧页面后仍通过当前 SQL 权限校验，已私有化、下架、不喜欢或已取消关注的内容会被抑制。临时媒体签名 URL 和观看租约不放进公共元数据缓存。

缓存具备进程内 singleflight、Redis 刷新租约、软失效后台刷新、到期抖动、Redis 短熔断和有限回源并发。当前 `_render` 仍逐卡执行 JOIN，因此不能仅凭三级结构宣称已经消除数据库压力。

另有 L1/L2/L3 **记忆层级**：L1 原子事实，L2 场景记忆，L3 稳定画像；它们参与推荐画像，不是上述缓存层。记忆事实由 AMEM 存储管理，详见[存储边界](storage.md)。

## 热点 key 是否动态续期

2026-10-25 已新增可配置的热点探测与主动刷新，默认关闭。关闭时 `FeedCache` 沿用请求触发的软失效刷新：

1. 普通命中直接返回；进程本地命中不延长本地 2 秒期限，Redis 普通命中不延长 Redis TTL。
2. Redis 数据超过 `soft_at`、尚未超过 `hard_at` 时，先返回缓存，再尝试提交后台 loader。
3. 刷新成功后，用新数据重新写入 `soft_at`、`hard_at` 和 Redis TTL。默认内容缓存约 60 秒后进入软失效，硬寿命为 240～300 秒；持续访问可以持续触发刷新，无访问则不会后台定时刷新。
4. 进程内 singleflight 合并 loader；跨进程以 `SET NX EX 5` 获取刷新租约，用 token 校验删除。该锁不自动续期，不能宣称超过 5 秒的 loader 仍受完整跨进程互斥保护。
5. 硬过期或 miss 必须重新加载；回源受并发限制，无法及时合并时可返回 503。

这里应称为“刷新后重设 TTL”，不应称为“热点识别后直接延长旧数据寿命”。多进程未抢到租约者可能读到尚未硬过期的旧值直接返回，不代表每次软失效请求都实际刷新了数据。页面缓存的软硬参数来自会话剩余时间，读取页面不会延长 Feed 会话；不能沿用内容缓存默认 60 秒的解释。

开启 `FEED_HOTKEY_ENABLED=true` 后，公开内容的本地命中也计入跨进程热度，热点进入持久调度集合，由后台重新校验当前公开权限、版本后刷新；低热度持续多个窗口后退出。它不会单纯延长旧值寿命，也不会续期 Feed 会话。见[生命周期设计](feed-hotkey-design.md)及[实现与验证](feed-hotkey-validation.md)。

## MinIO 保存什么

媒体 worker 将完整媒体包装为 HLS/fMP4 后上传到私有 bucket，默认名称为 `radio-media`。对象路径形如 `assets/{asset_id}/{digest}/{文件名}`。

- `index.m3u8`：HLS 播放列表。
- `init.mp4`：初始化片段。
- `seg_000000.m4s` 等：实际音视频分片。
- `audio.m4a`：独立音频。
- `cover.jpg`：封面。
- `metadata.json`：媒体包装元信息。

上传以大小和 SHA-256 校验，播放列表最后发布；完整发布后才把业务库中的资产标记为可重播。业务库保存文件 key、大小、摘要、对象版本及播放清单等索引，MinIO 保存文件本体。临时下载/封装文件在本地工作目录，不能与 MinIO 持久对象混为一谈。

播放请求先由 API 验证权限与播放租约，再为对象生成短期签名 URL。已入库存储的播放列表会把分片地址改写为签名地址；冷启动阶段可从本地已完成分片开始播放。

## Feed 如何生成

1. 创建会话：校验幂等键和模式，解析用户条件，冻结画像、请求条件及策略版本；默认会话有效期 30 分钟。
2. 取得候选：推荐/上新流读取已经准入的内容目录，过滤权限、用户条件、已发放内容、近期已看内容和不喜欢内容；不足时异步触发补货。
3. 排序：`personal` 用冻结画像进行确定性推荐，再结合近期显式反馈和有效观看做有限重排，并保留至多 20% 探索位；`new` 优先可播放资产，再按准入时间倒序。
4. 固定页面：每页最多 10 条，把顺序和决策快照写入 `feed_pages`、`feed_items`；以不透明 cursor 连接下一页。同一 cursor 重试复用固定页，不重新排序；当前权限变化仍可抑制条目。
5. 渲染卡片：读页面缓存，回查当前权限、资产、关系和计数，读取可用内容元数据缓存，组装返回值。
6. 前端加载：Pinia 保存会话、cursor 和卡片，追加时按 content ID 去重；补货状态采用有次数上限的退避轮询。
7. 播放与反馈：媒体已入库时读取 MinIO；未入库时后台共享导入。观看和互动事件以幂等方式落库，更新观看投影，符合条件的行为再进入记忆 outbox。

`following` 是独立的关注时间线：冻结关注快照和时间水位，按发布时间及内容 ID 倒序做 keyset 分页，读取创作者内容索引，异步同步供给，并在渲染时再次核对当前关注关系。分页不访问上游网络或调用 LLM。

这里 Feed 是分页内容产品，任务消息队列是异步执行通道，媒体 HLS 是播放传输，三者职责不同。

## Feed 排序与 Agent 推荐的区别

2026-10-25 核对：两者复用 `RecommendationEngine.score` 和 `rank_and_select`，并非两套完全独立的评分算法。

- Feed `personal` 通过 `decide_feed` 对已准入且经过权限过滤的目录候选评分，使用会话冻结画像；随后叠加 Feed 短期反馈重排、探索和固定分页。此路径不调用聊天模型，也不直接执行完整的 `list_recommendations` 操作。
- 对话推荐由 `DialogueService` 处理请求，调用 `list_recommendations(scene="conversation", request_spec=...)`。完整推荐服务还处理场景记忆、当前画像投影、候选生成、发现计划、结果持久化和推荐追踪；画像投影是否调用模型受个人密钥、缓存和实验配置影响，不是每次推荐都调用 LLM。
- `list_recommendations` 经 `music_operation("recommendation")` 接入持久 Agent 运行时。该包装层的 `OperationGateway` 是确定性控制路由，负责工具执行与恢复，不是额外的聊天模型，也不直接替换排序算法。

因此 Agent 的作用包括理解和组织推荐任务、管理工具执行与状态；推荐引擎负责候选评分、过滤、多样性和选择；Feed 负责把推荐结果做成稳定分页的内容消费体验。不能把 Agent 推荐简单描述为“大模型直接决定所有歌曲顺序”。

相关实现：[推荐服务](../recommend-radio/backend/recommendation_service.py)、[推荐引擎](../recommend-radio/backend/recommendation_engine.py)、[对话服务](../recommend-radio/backend/dialogue_service.py)、[Agent 操作包装](../recommend-radio/backend/music_agent.py)。

## 源码入口

- [缓存实现](../recommend-radio/backend/feed_runtime/cache.py)
- [Feed 会话、固定页与渲染](../recommend-radio/backend/feed_runtime/feed.py)
- [短期反馈重排](../recommend-radio/backend/feed_runtime/recommendation.py)
- [关注时间线](../recommend-radio/backend/feed_runtime/following_feed.py)
- [媒体包装和上传](../recommend-radio/backend/feed_runtime/worker.py)
- [播放授权和清单](../recommend-radio/backend/feed_runtime/media.py)
- [MinIO 存储](../recommend-radio/backend/feed_runtime/storage.py)
- [前端状态](../recommend-radio/frontend/src/stores/feedStore.ts)
