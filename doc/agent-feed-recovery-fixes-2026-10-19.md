# Agent 副作用恢复与 Feed 播放连续性修复

日期：2026-10-19。针对当前工作区实现修复，保留已有未提交改动。本轮没有部署服务、没有修改生产数据；本文更新此前核查中相关缺口的状态。

## 1. Agent 执行写入隔离

原问题有两个窗口：checkpoint、turn、tool、approval 的写入只检查自身版本，没有检查执行租约；旧执行异常处理重新加载 run 后，可能使用新 owner 的 token 标记失败。

本次修改：

- 每次领取 run 后构造独立的执行视图，绑定领取时的 lease token。不同 run 不共享可变的执行 token。
- checkpoint、turn、工具创建/更新、审批创建，在同一个 SQLite 写事务或内存写锁内核验 run 仍为 running、token 一致且租约未过期，再提交自身版本更新。
- ReliableToolRuntime 遇到失租直接停止，不把失租当普通工具异常重试。
- 失败、超时重排和待对账收尾校验本次 execution 身份，旧 worker 不能借用新 owner 的 token。
- 心跳更新异常时取消当前执行；取消后迟到的工具结果不能写成 succeeded。未知结果留待核实，取消的 run 不会因对账重新启动。

这些保证覆盖 runtime 的执行路径。存储层保留不带执行 token 的受信维护接口，供原子对账和管理操作使用。它不意味着可以阻止已经发到外部系统的请求产生副作用；外部接口仍需要业务幂等键或核实实际结果。

代码：[leases.py](../src/agent_memory_runtime/agent/stores/leases.py)、[sqlite.py](../src/agent_memory_runtime/agent/stores/sqlite.py)、[in_memory.py](../src/agent_memory_runtime/agent/stores/in_memory.py)、[runtime.py](../src/agent_memory_runtime/agent/runtime.py)、[tool_runtime.py](../src/agent_memory_runtime/agent/tool_runtime.py)。

## 2. 工具、run 与 job 的恢复闭环

### 工具和 run 原子对账

`reconcile_tool_record()` 在同一状态库事务内写工具结果、审计元数据和 run 状态；中间失败则回滚全部修改。内存 store 同样支持回滚。

工具流水新增兼容旧 JSON 的 reconciliation 字段，保存 resolution_id、操作人、理由、时间、结果哈希和处理决定。同一 resolution_id、同一证据重入时不重复更新；不同证据被拒绝。仍有其他未知副作用时，run 保持待对账。已取消或失败的 run 只补录结果，不复活任务。

### Agent 库与业务库之间可重入协调

两者目前仍是不同数据库，不能声称存在跨库原子事务。`reconcile_job_tool()` 使用现有 durable_job_resolutions 表实现处理意图：

```text
业务库提交 agent_tool_pending 意图
  -> Agent 状态库原子对账
  -> 业务库提交 job 后续状态及 agent_tool_applied
```

如果进程在 Agent 提交后、job 提交前中断，重复同一条命令即可继续后半段。成功且没有其他未知结果时重新排队；核实失败时 run 和 job 都终止为 failed。不会通过更改 retry_safe 或重置 attempts 绕过重试预算。

另新增 `resume-job`，处理 run 已完成但 job 尚未提交完成、或已提交检查点可安全接续的情况。它拒绝活动 owner、未知非幂等副作用、取消/失败 run 和用尽重试预算的任务。run 已完成时 worker 会重放已有结果，继续执行外层业务收尾。

旧的 `resolve` 命令拒绝单独解决已关联 Agent 的 job，避免工具状态仍未知而 job 却被标成完成。

### 操作入口

在应用后端环境执行：

```powershell
cd recommend-radio/backend
py -3.12 -m job_admin status
py -3.12 -m job_admin inspect-job "实际 job_id"
```

先确认旧执行已停止，并核实外部操作结果。inspect-job 输出 call ID、状态、尝试数和 resolution ID，不输出工具参数或结果正文。

需要补录工具结果时，将核实的完整阶段输出写入 JSON 文件，并记录一次稳定的 resolution ID：

```powershell
py -3.12 -m job_admin reconcile-tool "实际 job_id" `
  --call-id "实际 call_id" `
  --resolution-id "本次处理的稳定 ID" `
  --outcome succeeded `
  --output-file "verified-output.json" `
  --reason "说明核查方式和外部凭据"
```

确认未成功则使用 `--outcome failed`，输出文件仍需提供 JSON 对象。不能用猜测的输出恢复流程。命令中断后保持 ID、文件内容、理由和操作人一致再执行，不要生成新 ID 规避冲突。

如果工具状态已知、run 已完成或检查点可接续：

```powershell
py -3.12 -m job_admin resume-job "实际 job_id" `
  --resolution-id "本次恢复的稳定 ID" `
  --reason "旧执行已停止，已核对工具流水和检查点"
```

恢复后由现有 relay/worker 继续执行。存在未知副作用时必须先处理工具对账，不能直接 resume-job。上述元数据使用已有 JSON 和业务审计表，无需新增数据库 schema 迁移。

代码：[recovery.py](../src/agent_memory_runtime/agent/recovery.py)、[agent_job_recovery.py](../recommend-radio/backend/agent_job_recovery.py)、[job_admin.py](../recommend-radio/backend/job_admin.py)、[music_agent.py](../recommend-radio/backend/music_agent.py)。

## 3. Feed 从冷源到持续播放

### 同一份媒体在转存时保留身份

获取源文件时将 sourceGeneration 保存到 source.json；封装上传后继续保存到资产 manifest。上传重试复用完整源文件时保留该身份，不用新的上传任务 ID 替代媒体身份。

冷源与存储源的 generationKey 因而保持一致。HLS 播放器不会仅因 mode 从 cold 变成 stored 就销毁 MediaSource、清空缓冲或从头播放。真正重新获取了源媒体时才切换 generation，并恢复已有位置。

冷源 manifest 中的 init 和媒体分片链接携带 generation 参数；ASGI 入口在提供文件前校验。如果旧播放器请求已经被替换的 generation，返回 409 触发获取最新描述符，防止把新旧来源的分片混进同一缓冲区。检查与文件选择复用同一份 import 快照。

### 签名续期不清空 HLS 缓冲

HLS adapter 刷新授权 manifest，按平台固定的 init/segment 文件名更新后续请求 URL；xhrSetup 在发送时使用新签名，并按实际目标 origin 决定是否携带凭证。

它不会因签名续期销毁已有 HLS 实例。网络错误恢复优先更新 URL 并恢复加载；只有源确实改变或需要重建播放时才重新 attach。过期的异步刷新不能覆盖已经切换的新源。

用户播放意图与 media.paused、playing 事件分开保存。重建时在 metadata 就绪后恢复位置，并重新检查用户是否仍希望播放，避免暂停后被后台刷新自动播放。续期失败发生在缓冲仍可播放时，也不会伪造暂停状态和丢失观看时长。

原生 HLS 浏览器仍需在必要续期时重新加载来源，代码保留位置和播放意图；本轮没有实际 Safari/iOS 验收，不能把 Chromium HLS.js 的无重挂载结论外推到所有浏览器。

### 上传失败与可播放状态分开

已经完成获取和校验的本地 HLS，即使后续上传失败，仍可在有效播放租约和权限范围内继续播放。descriptor 新增 persistenceStatus，持久化失败不会直接把完整可播放副本标成不可播放。

清理任务识别有效播放引用，在资源锁内复查，避免删除正在观看的本地目录。未完成源、已取消导入、下架/不支持的资产以及过期/释放的 playback 仍按原规则阻断。上传成功后转到对象存储时，已有播放身份不变。

播放创建遇到超时、429 或服务端临时错误，会以同一 Idempotency-Key 有限重试，避免“服务器已创建、客户端没收到响应”时重复创建。

代码：[media.py](../recommend-radio/backend/feed_runtime/media.py)、[worker.py](../recommend-radio/backend/feed_runtime/worker.py)、[maintenance.py](../recommend-radio/backend/feed_runtime/maintenance.py)、[web_asgi.py](../recommend-radio/backend/web_asgi.py)、[VideoPlayerAdapter.ts](../recommend-radio/frontend/src/video/VideoPlayerAdapter.ts)、[FeedView.vue](../recommend-radio/frontend/src/views/FeedView.vue)。

## 4. 验证记录

### 定向验证

- Agent 新增测试覆盖：失租/过期后的全部执行写入、旧异常收尾、原子回滚、处理证据重入/冲突、多未知结果、核实失败、取消过程中副作用、跨库提交中断、已完成 run 的 job 恢复、CLI 禁止单独解决关联 job。
- Feed 测试覆盖：冷转存 generation 一致、完整源上传失败继续播放、有效播放阻止清理、重试上传复用源、签名按请求刷新、旧 generation 分片被拒绝。
- 相关大组回归曾通过 85 项；补充媒体代际检查后的定向组通过 48 项。
- `npm run build` 通过，包含 vue-tsc 类型检查。构建仍报告既有 HLS chunk 超过 500 kB 的提示。
- 新增代码静态检查通过；原 runtime 中少数既有超长行未在本轮整体重排。

### 真实浏览器验证

执行：

```powershell
py -3.12 recommend-radio/scripts/check_cold_playback_recovery.py
```

本机 FFmpeg 生成实际 H.264/AAC fMP4 HLS，Chromium 真正解码播放，API 由受控路由模拟状态和故障。七项检查通过：创建重试键稳定、冷转存不换 MediaSource、瞬时心跳故障不结束会话、续签保留暂停/位置/缓冲、签名过期后 seek 使用新 URL、真正换源后保留暂停位置、观看身份单一且无 JavaScript 异常。

结果：[browser JSON](cold-playback-recovery-browser-2026-10-19.json)。这项验证没有实际请求 B 站或 MinIO。

### 扩展回归与环境隔离

直接继承本机环境运行全部测试，结果为 312 passed、8 failed、14 errors、1 skipped：本机配置指向未启动的 MySQL/Kafka，部分真实实例测试无法运行。Docker Desktop Linux engine 当前也不可连接。

随后使用临时 APP_DATA_DIR 和空数据库配置隔离回归。首次把 JOB_TRANSPORT 固定为 disabled 导致两项本来要开启异步任务的测试失败；已改为空值，并清空 Kafka 地址、默认关闭 RabbitMQ，使各测试仍可自行选择传输。

最终隔离回归结果为 **324 passed、1 skipped，耗时 251.99 秒**，记录在 [JUnit XML](agent-feed-recovery-regression-2026-10-19.xml)。这轮显式排除 test_kafka_mysql_integration.py 和 test_reaction_bitmap_native.py，不能据此宣称真实 Kafka/MySQL/Redis 全链路故障验收通过。

在仓库根目录使用 PowerShell 重跑同一隔离命令：

```powershell
$regression = @'
import os, subprocess, sys, tempfile
with tempfile.TemporaryDirectory(prefix='radio-agent-feed-regression-') as folder:
    env = dict(os.environ, APP_DATA_DIR=folder, DATABASE_URL='', BUSINESS_DATABASE_URL='',
               JOB_TRANSPORT='', KAFKA_BOOTSTRAP_SERVERS='', RABBITMQ_ENABLED='false',
               RADIO_OUTBOX_EMBEDDED='true', AUTH_MODE='disabled',
               ALLOW_INSECURE_LOCAL_AUTH='1', REQUIRE_DB_MIGRATION='0')
    result = subprocess.run([
        sys.executable, '-m', 'pytest', '-q', '--tb=short',
        'tests', 'recommend-radio/backend/tests',
        '--ignore=recommend-radio/backend/tests/test_kafka_mysql_integration.py',
        '--ignore=recommend-radio/backend/tests/test_reaction_bitmap_native.py',
        '--junitxml=doc/agent-feed-recovery-regression-2026-10-19.xml',
    ], env=env)
    sys.exit(result.returncode)
'@
py -3.12 -c $regression
```

## 5. 交付边界

- 外部副作用未知时仍需要实际凭据与人工核实；本轮补齐的是安全接续、可重入对账和状态协调，不能保证任意外部接口 exactly-once。
- Agent 状态仍保存在本地 SQLite，冷媒体仍依赖该主机工作目录。本轮未迁移到支持多机共享的 Agent 状态库，也未实现跨主机冷媒体分发。
- 对象存储仍走带期限签名 URL，已签发 URL 的即时撤销能力没有改变。
- 本轮没有测量真实 B 站/MinIO 网络下的首帧分位数或进行生产压测；不声称首帧性能提升百分比。
- 前后端应同时更新，保留已有 Agent 状态库与媒体工作目录。旧 JSON 缺少 sourceGeneration/reconciliation 时有兼容读取逻辑，无需重置历史数据。
