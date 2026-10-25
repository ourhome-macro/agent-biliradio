# UGC 音频直传与 B 站流代理

日期：2026-10-25。代码、真实隔离专项和统一 PR 门禁已完成，尚未部署。现有业务数据和 MinIO 对象不删除。

## 两条媒体路径

- 自有 UGC 音频：草稿 → 限额上传会话 → MinIO 签名 POST 直传 → 完成通知 → 持久任务校验摘要、大小和媒体格式 → 生成不可变 AAC 资产 → 发布短事务与 Outbox → Kafka → ES 版本化投影 → Inbox。
- B 站视频：当前权限校验 → 按需解析上游 DASH 地址 → 读取小范围 SIDX 索引 → 生成 HLS 字节范围清单 → Range 流代理。签名失效后重新解析，不下载完整文件，不写 MinIO。

音频文件 I/O、FFprobe/FFmpeg 和 ES 网络请求都在核心内容写事务外。发布前必须确认当前上传处理成功，不能相信浏览器的“上传成功”。内容状态、版本、媒体引用与 Outbox 同事务提交。

## 实现边界

- `radio_010` 增加上传会话和音频引用；SQLite 使用显式 Alembic 迁移，MySQL 保留显式、可重入的升级路径。不自动升级现行业务库。
- 直传限制固定 key、精确字节数、声明类型、有效期、用户和草稿归属。源对象只是临时区，工作进程实际下载并验证 SHA-256 后再生成独立正式对象，防止签名有效期内覆写源对象影响已发布文件。
- 后台处理继承持久任务租约，过期 worker 不可提交资产状态；发布幂等、消费者 Inbox 和 ES 外部版本控制复用现有机制。
- 代理只接受服务端解析出的受信 CDN 地址，每一跳重定向重新校验；不接受客户端自带 URL。保留 Range、播放租约、当前资源权限及有限并发；客户端断开后释放上游连接。
- B 站是按需上游，不具有本平台持久存储承诺。地址过期、资源撤销或上游不可用应明确返回错误，不能用旧文件掩盖。
- 旧媒体副本兼容读取，不新增 B 站入库任务；运行时收到遗留 `media_import` 任务会取消并永久结束，不再执行 MinIO 上传。已经运行的旧进程需随部署重启。历史对象由独立维护动作清理，此次不删除业务资产。

音频上限 64 MB、每用户最多 4 个未过期活跃上传会话，签名有效期 15 分钟。全局容量按现有媒体字节与每会话 160 MB 的保守预留计算；处理后音频不超过 96 MB。FFprobe/FFmpeg 只允许音频容器白名单和 file/pipe 协议，不接受 HLS/concat 等可引用外部资源的上传文件。

已验证的音频输出为 AAC/M4A，默认最长 1 小时。视频 Range 代理默认每进程最多 32 个上游连接；签名元数据缓存最多 256 项、60 秒，冷解析 singleflight 最多 32 个进行中请求，等待最多 10 秒。只支持单段显式起点 Range，不把多段 Range 转成无限并行回源。

## 接口和运维

| 接口 | 作用 |
| --- | --- |
| `POST /api/ugc/posts`，`kind=audio` | 创建音频草稿 |
| `POST /api/ugc/posts/{id}/uploads` | 绑定草稿、字节数、SHA-256、类型，返回签名 POST 表单 |
| `PATCH /api/ugc/posts/{id}`，`audioUploadId` | 绑定当前上传，校验用户和草稿归属 |
| `POST /api/ugc/uploads/{uploadId}/complete` | 幂等创建持久 `ugc_audio` 任务，返回 202 |
| `GET /api/ugc/uploads/{uploadId}` | 查询 awaiting/queued/processing/ready/failed/expired |
| `POST /api/ugc/posts/{id}/publish` | 仅 ready 资产可发布，版本、内容与 Outbox 同事务 |
| `POST /api/media/playbacks` | B 站返回 proxy，音频返回原生 M4A 播放描述符 |

运行 API 与媒体 worker 前先使用独立迁移进程执行 `recommend-radio/backend/migrate_db.py`；生产设置 `REQUIRE_DB_MIGRATION=1`，旧库不得由普通请求升级。MySQL 009→010 已实现可重入升级；SQLite 同步提升到 user_version 34。此次没有对业务库执行迁移。

音频需要 `UGC_ENABLED=true`、现有 MinIO 凭据和公开 endpoint，以及消费 media lane 的任务 worker。浏览器必须能访问公开 endpoint，MinIO CORS 需要允许站点的 POST 表单上传；使用 HTTPS 站点时公开 endpoint 也应为 HTTPS。签名采用受限 POST，避免 PUT 无法在策略中精确限制上传大小的问题。

B 站播放不依赖 MinIO 凭据或 FFmpeg。Feed health 将 MinIO/FFmpeg 保留为组件状态，核心 ready 不再要求它们全部为真。遗留存储资产及 UGC 音频仍需 MinIO。

临时对象必须在签名过期后再删除，避免有效签名重建孤儿对象。维护入口为：

```powershell
# 从 backend 目录运行，显式清理过期的临时上传对象
py -3.12 -m feed_runtime.cli cleanup-audio-uploads --apply
```

任务重试耗尽时，状态查询和清理会把 queued/processing 收敛为 failed。正式对象沿用 `assets/` 管理前缀，继续受现有备份和清理规则管理。

## 验证记录

`py -3.12 scripts/validate_media_paths.py` 在独立数据目录和随机端口运行，不使用业务数据库、密钥或私人账号。专项结果：

- 17 项回归通过，覆盖发布准入、上传归属、大小/摘要、幂等完成、过期清理、失去租约、恶意播放列表、任务耗尽、B 站零入库任务、SIDX 偏移、权限与播放租约、签名过期重解析、代理容量及连接回收、重定向拒绝。
- 真实 MinIO 受限签名直传、大小不匹配拒绝、真实 FFmpeg 校验处理、源对象覆写不影响正式资产均通过。
- 已构建前端＋实际 API＋独立 Redis/MinIO 的 Chromium 流程通过：选择音频、直传、等待处理、发布、详情播放；JavaScript 错误为 0。身份使用隔离夹具，不使用私人账号。
- 受控 HTTP 上游＋真实 FFmpeg fMP4 文件的 HLS 字节范围播放与 seek 通过，观察到 9 次 Range 请求，MinIO 写入为 0。
- 独立原生 MySQL 009→010 升级保留已有数据、重复升级无变更，音频草稿、上传引用与任务入队通过。

原始汇总在 `artifacts/quality/media-paths.json`，专项 JUnit 在 `media-paths.xml`；[本次汇总快照](ugc-audio-and-bili-proxy-2026-10-25.json)随代码交付。MySQL 迁移校验补强后又单独执行 `scripts/validate_media_paths.py --mysql-only`，升级、幂等与上传入队再次通过。

最终 `py -3.12 scripts/quality.py pr` 通过：Python 335 passed、59 deselected，耗时 130.06 秒；Golden 40/40，前端 Node 10/10、vue-tsc 与 Vite build 通过。保留已有大 chunk 构建提示；59 项为非集成入口排除的用例，不计为通过。

没有运行真实 B 站账号/真实 CDN 长时间播放、全量 Kafka/ES 新链路部署或线上容量验收，不能把受控上游测试表述为这些验收通过。真实专项需要本地 MinIO、Redis、MySQL 可执行文件、FFmpeg 和 Playwright Chromium；可使用 `REDIS_SERVER_PATH`、`MYSQLD_PATH` 显式指定测试程序。临时基础设施均由脚本创建并关闭，不复用业务服务。

交付沿用 `ci/unified-quality-gate` 分支及 PR #7，不包含工作区原有 `doc/context.md` 修改和无关历史产物，不修改 `.env` 或生产数据库。

## 代码入口

- [上传会话](../recommend-radio/backend/ugc/uploads.py)与[后台校验](../recommend-radio/backend/ugc/audio_worker.py)
- [无存储视频代理](../recommend-radio/backend/feed_runtime/bili_proxy.py)
- [SQLite 迁移](../recommend-radio/backend/migrations/versions/radio_010_audio_uploads.py)与[MySQL 升级](../recommend-radio/backend/mysql_storage/upgrades.py)
- [回归测试](../recommend-radio/backend/tests/test_audio_uploads_and_proxy.py)与[真实隔离验证入口](../scripts/validate_media_paths.py)
