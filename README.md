# AMEM × Recommend Radio

本项目把 Agent 长期记忆运行时和 B 站音乐播放器结合成一个本地优先的个性化推荐系统。

- **AMEM**：管理 L0-L3 记忆、hot/warm/cold 温度、权限、审计、FTS 和 bge-m3 语义检索。
- **Recommend Radio**：提供 B 站播放、对话式选歌、候选池、画像、反馈闭环和推荐解释。

## 推荐系统

```text
用户消息 / 首页
  -> RequestSpec（本轮约束） + MusicProfile（长期偏好）
  -> Discovery（B 站补货）
  -> CandidatePool（准入候选）
  -> RecommendationEngine（过滤、排序、MMR、多样性）
  -> 播放/喜欢/跳过事件
  -> AMEM L0 -> L1 -> L2 -> L3 生命周期
```

明确请求优先于长期画像。例如“推荐欧美 R&B”只能返回英文/欧美 R&B；“来一点 Rap”可覆盖长期 Rap 回避项，但只影响本轮，不修改 profile。泛推荐以默认候选池和 L1/L3 记忆为主体，最多借入两首近期对话上下文候选。

## 快速启动

```powershell
cd recommend-radio
docker compose up -d --build
```

打开 `http://localhost:3000`。后端为 `http://127.0.0.1:5000`，AMEM gRPC 为 `127.0.0.1:19090`。

首次推荐会建立画像和候选库存；后续命中画像与 L2 缓存时通常只需约百毫秒。默认使用本机 bge-m3 embedding 服务，详见 [recommend-radio/README.md](recommend-radio/README.md)。

## 开发

本地与 CI 共用 [质量门禁](doc/quality.md)：提交前执行快速钩子，PR 执行契约、前端与 Golden Set，main 增加真实服务验收。当前不自动发布。

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev,qdrant]"

cd recommend-radio\frontend
npm install
npm run build
```

## 文档

- [系统架构](doc/architecture.md)
- [推荐系统架构](doc/recommendation.md)
- [记忆与上下文](doc/context.md)
- [检索与温度策略](doc/retrieval.md)
- [存储模型](doc/storage.md)
- [API 契约](doc/api-contract.md)
- [运维与性能](doc/operations.md)
- [治理与安全](doc/governance.md)

一次性调试报告和阶段性实现记录不再保留在仓库中；稳定设计只维护在上述文档内。
