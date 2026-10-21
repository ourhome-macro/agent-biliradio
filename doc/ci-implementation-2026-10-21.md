# CI 分层与文档整理实施记录

日期：2026-10-21。先完成 CI，再审阅业务实现并扩充 Golden Set。本轮没有启动业务应用或操作浏览器。

## 已实施

- 新增 GitHub Actions：PR / merge queue 跑 lint、契约、前端与 Golden；main push / 手动运行增加真实服务全量验收。统一必需检查名 `quality-gate`，不新增发布流程。
- 新增 `scripts/quality.py`，本地与 CI 共用命令；pre-commit 调用快速 lint 与文档契约。
- 核心完整 Ruff、旧 Radio 的确定性错误检查、配置语法、前端 Node 测试 / 类型 / 构建纳入门禁。
- 完整确定性 Golden Set 同时进入 PR 与 main，随后由 27 扩充至 40 个；JSON / JUnit 报告作为短期 CI artifacts。
- 真实服务测试使用显式配置文件；MySQL 夹具支持临时 runner 的隔离数据库实例，Redis 程序支持 Linux PATH。
- 修正前端测试被 ignore 的范围；新增检查零测试、未预期 skip、服务测试遗漏和文档篇幅的门禁回归。
- 文档启用 markdownlint、UTF-8、每篇 8,000 字符和相对文件链接检查。四份超长材料拆为有导航的分篇，保留原入口和正文；索引按质量、稳定设计、历史证据整理。

## 验证记录

初轮受限沙箱无法访问 Python 私有临时目录，Node 子进程也被 EPERM 阻止；这些是执行环境失败，不计入通过结果。经明确批准在沙箱外运行隔离命令后：

- 含两个业务修复的统一 PR 命令完整通过：Python 297 passed、59 deselected，93.96 秒，无失败或跳过。
- Golden：40/40，gates.passed=true；原始门槛未降低，历史 27 案例快照保留。
- 前端 Node：10/10；vue-tsc 与 Vite build 通过。现有大 chunk 提示未在本轮处理。
- 临时独立 MySQL 实例适配：42 passed、7 deselected，168.09 秒；验证了新建 schema 与清理路径，不接触原业务实例。
- 最终门禁 / Golden 专项 14 passed，包含随后补充的必需案例清单守卫和输入 / 预期字段校验。这组与 PR 测试有重叠，不能相加作为独立用例总数。
- 文档：99 篇通过 markdownlint、篇幅和相对文件链接检查。
- actionlint 固定版本 1.7.7 通过；已修正 `runner.temp` 在 job env 中不可用的问题。

## 明确边界

本机 Docker Desktop Linux engine 未运行，尚未执行这份 Actions workflow 的真实容器服务组合，不能宣称 main 集成层已经验收通过。远端 workflow 执行与 main 分支保护需在相关文件进入 GitHub 后确认；本轮没有 push 或修改远端规则。

本轮清理了核心 lint 的现有格式问题；旧 Radio 的完整风格规则仍有存量债务。所有规则、命令和环境变量以 [质量门禁](quality.md) 为准。

评测自证问题、13 个新案例与修复的两个行为问题见 [Golden 扩充记录](golden-expansion-2026-10-21.md)。

## GitHub 投递

用户已授权推送。投递分支为 `ci/unified-quality-gate`，从最新 `origin/main` 建立，通过 PR 审查后再合并；不直接推 main。提交包含 CI、质量工具、对应测试、Golden 数据和必要文档，保留原有 `doc/context.md` 未提交修改及无关历史验收产物。远端结果以该分支的 Actions / PR 检查为准，不预先标记通过。
