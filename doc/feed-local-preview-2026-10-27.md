# 本地 Feed 预览运行记录

日期：2026-10-27。本轮复用已有本地 Docker 前后端和基础设施，补启动 MinIO 并初始化缺失 bucket；未重建镜像、未迁移数据库、未修改业务代码或 `.env`。

## 访问与依赖

- 前端：`http://127.0.0.1:13000/#/feed`，使用 hash 路由。
- 后端：`http://127.0.0.1:15000`，`/health/live` 返回 200。
- Feed 健康检查：前端代理 `/api/feed/health` 返回 200，enabled、Redis、MinIO、FFmpeg、ffprobe 均为 true。
- 前后端与 Kafka、MySQL、Redis、Elasticsearch 容器原已运行。当前容器为此前构建的镜像，不代表最新 Git 提交已部署。

最初 Feed 健康检查为 503：MinIO 未运行。启动本地栈配置的 MinIO 后，进一步确认 `radio-media` bucket 不存在；通过现有 `feed_runtime.cli provision` 创建后恢复健康。没有删除或覆盖已有对象。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File recommend-radio/scripts/manage-docker-stack.ps1 -Action start-native-storage
docker exec radio-kafka-backend-1 python -m feed_runtime.cli provision
```

已尝试通过浏览器打开本地地址；系统浏览器已有管理员实例，首次启动出现权限重启提示，未代替用户确认。后续原生窗口观察到电台页面标题；未把窗口启动成功视为播放验证，也未执行完整浏览器播放验收。

## 内容从哪里来

Feed 先从本地业务数据库中的准入池读取内容，不是每次翻页都直接搜索上游。

1. 推荐候选在 `content_cache` 中维护，生成普通 Feed 页时 `_sync()` 将可服务候选同步至 `feed_content`；公开发现来源与个人曲库来源保留不同可见范围。
2. UGC 在发布流程中形成准入内容记录；关注流另有关注作者的异步补货路径。
3. `personal` 使用画像与推荐排序，`new` 使用 ready 优先和入库时间排序，`following` 从关注作者索引按发布时间分页。
4. 已选择的页及条目持久化至 `feed_pages`、`feed_items`，随后填充页面和 Metadata 缓存；渲染仍重新检查权限、内容状态及用户反馈。

本轮只读统计：原候选池 `content_cache` 为 60 条，初次观察 `feed_content` 为 0；页面访问后 `feed_content` 为 60。后续快照为 6 个会话、12 页、90 个下发条目，含 personal、new、following 会话。这些是本地实例观察值，不是压测数据，也不保证每种模式都有十条可展示内容。

代码：[页面生成与渲染](../recommend-radio/backend/feed_runtime/feed.py)、[候选池](../recommend-radio/backend/candidate_pool.py)、[关注补货](../recommend-radio/backend/feed_runtime/following_feed.py)。
