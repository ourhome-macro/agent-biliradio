# Recommend Radio

Recommend Radio 是 AMEM 的 B 站音乐推荐应用：Vue 前端、Python Web（Flask API + 异步 SSE）、embedded AMEM 和后台 Worker。

## 能力

- B 站视频/音频播放、曲库、播放历史、喜欢和歌单；
- 对话和首页共享同一个 RecommendationEngine；
- RequestSpec 支持语言、地区、流派、声线、情绪和排除条件；
- Discovery 负责 B 站补货，CandidatePool 只保留可服务候选；
- SongWork/Recording/VideoAsset 三级实体避免把原版、现场和翻唱错误合并；
- 候选文本向量在准入阶段持久化，可选 CLAP 音频任务只处理高价值增量候选；
- L0-L3 和记忆温度驱动长期画像、近期上下文、去重和疲劳控制；
- 推荐 trace 输出画像、请求约束、候选来源和性能耗时；
- 本地 bge-m3 embedding 支持 AMEM 语义检索。
- impression、稳定用户分桶和 logged-slate 评测支持可证伪的线上/离线实验。
- dialogue → recommendation → discovery → feedback 使用统一父子 trace，并提供三层 Golden Set 回归门禁。

## 启动

默认 Docker 栈使用 MySQL、Kafka、Redis 和 Elasticsearch；AMEM 默认嵌入运行。Windows 可通过管理脚本创建隔离配置并启动：

```powershell
.\scripts\manage-docker-stack.ps1 -Action prepare
.\scripts\manage-docker-stack.ps1 -Action up
```

默认 `docker-compose.yml` 引用 `docker-compose.full.yml`。私有环境文件保存在 `server-data/kafka-acceptance/private.env`，不得提交到仓库；MinIO 按脚本配置使用独立宿主进程或外部对象存储。

Linux 宿主部署可参考 `deploy/radio.env.example`、`deploy/systemd/` 和 `deploy/Caddyfile.host`：先构建 `frontend/dist`、安装 `backend/requirements.txt` 与根包，配置环境并运行迁移，再启动 Web、relay 及 Kafka workers。首次切换前备份业务数据库、Agent 状态库和媒体目录。

默认隔离 Docker 栈访问 `http://127.0.0.1:13000`；实际端口以私有配置为准。

Agent 对账、任务恢复和冷内容连续播放的操作与验证见 [修复说明](../doc/agent-feed-recovery-fixes-2026-10-19.md)。

## 组件

```text
Frontend :13000      Vue 前端和 API 代理
Python Web :15000    Flask API、异步 SSE、媒体授权
Kafka relay/workers  对话、发现、行为、媒体、搜索与缓存投影
MySQL / Redis / ES   业务事实、缓存与搜索读模型
MinIO                媒体对象存储
embedding :8001      可选的宿主 bge-m3 OpenAI-compatible 服务
```

## 推荐原则

```text
RequestSpec 硬约束
> L2 当前场景
> L3 稳定回避项
> L1 近期偏好
> L3 长期偏好
> 探索与多样性
```

明确请求会同步 bootstrap discovery 后再返回候选；首页默认池低于 32 条时后台预热。请求定向候选不会污染首页，泛推荐最多借入两首近期对话上下文候选。

## 性能

推荐 API 返回 `timing`，trace 保存画像、LLM API、L2、候选池、打分、MMR 和 Discovery 搜索/准入耗时。画像缓存命中后不会再次调用 LLM。

Agent 评测与 trace 使用方式见 [doc/AGENT_EVALUATION.md](doc/AGENT_EVALUATION.md)。
