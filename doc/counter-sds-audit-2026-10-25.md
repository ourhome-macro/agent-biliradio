# 计数 Redis String / SDS 核对

日期：2026-10-25。结论：当前聚合计数已经写入 Redis String，独立 Redis 8.2.2 实测编码为 raw，即 SDS 字符串存储；无需更换业务存储结构。本轮增加存储契约回归，未改变计数算法或迁移业务数据。

## 实际数据与路径

Key 为 `radio:feed:counter:<content_id>`，默认 TTL 为 300 秒。Value 是普通 JSON 字符串，包含 likes、dislikes、version 等字段，不是 Redis Hash 或 RedisJSON 文档。例如：

```json
{"content_id":"audit-content","likes":7,"dislikes":2,"version":12,"updated_at":1792930000.0}
```

- 单条投影：`publish_counter()` → `_PUBLISH` Lua → `SET key json EX 300`。
- 批量投影：`publish_projection_batch()` → pipeline 中执行相同 Lua，计数仍是 String。
- 封装的回源填充：`counter()` → GET；未命中后加载 SQL 快照，再通过相同发布路径写 String。此接口当前只有测试调用，尚未接入业务读取。
- Lua 比较 version 后发布绝对值，较旧投影不能覆盖较新投影；重复消费不会像重复 INCR 那样累计错误。

代码：[计数读写](../recommend-radio/backend/feed_runtime/cache.py)、[投影事务](../recommend-radio/backend/feed_runtime/reactions.py)。SDS 是 Redis 的内部字符串表示，不是单独的数据类型；数字 String 可能使用 int 编码，本项目的 JSON 信封在本次实测中为 raw。参考 [Redis 对象编码说明](https://redis.io/docs/latest/commands/object-encoding/)。

## 独立服务实测

| 路径 | TYPE | OBJECT ENCODING | TTL（秒） | 赞 / 踩 / 版本 |
| --- | --- | --- | ---: | --- |
| 单条发布 | string | raw | 300 | 7 / 2 / 12 |
| 批量发布 | string | raw | 300 | 7 / 2 / 12 |
| 回源填充 | string | raw | 300 | 7 / 2 / 12 |

[实测快照](counter-sds-audit-2026-10-25.json)来自本轮新建的 loopback Redis，不使用业务服务或账号。版本变化时允许 Redis 采用 raw 或 embstr 两种 SDS 编码，不把特定编码阈值写死为业务约束。

[回归测试](../recommend-radio/backend/tests/test_counter_sds.py)覆盖这三条路径的 String/SDS 编码、JSON 内容、TTL、旧版本拒绝、重复发布、Redis 命中，以及删除后的回源重建；3 项全部通过。

统一 PR 门禁退出码 0：Python 363 passed / 60 deselected、Golden 40/40、前端 10 项、类型检查和构建通过。60 项未选中的集成测试不计为本轮通过。

## 与持久化和展示的关系

当前是 **SQL 提交源关系、计数和 Inbox，再向 Redis 发布计数投影**。MySQL 保留恢复基准；Redis 是可过期、可重建的投影，不是唯一持久化副本。

普通 Feed 页面与详情目前直接使用 SQL 查询取得的计数，见 [Feed](../recommend-radio/backend/feed_runtime/feed.py) 与[详情](../recommend-radio/backend/feed_runtime/product.py)。核对当前调用点，`counter()` 只有测试调用，Redis 聚合计数读路径尚未接入业务。用户点赞关系的 Bitmap 与聚合计数属于不同投影；不能把 Bitmap 当作此处的计数值，也不能把“已经存入 SDS”理解为展示请求已经改为 Redis 读取。

本轮未调整业务读写拓扑、未部署；完整 main 验收不在本轮核对范围内。
