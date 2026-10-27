# 质量门禁

质量规则由仓库脚本执行，GitHub Actions 负责调度。人和 Codex 使用相同命令、相同阈值、相同失败状态。

## 三层检查

| 层次 | 执行内容 | 通过条件 |
| --- | --- | --- |
| pre-commit | 暂存文件 Python lint、配置语法、Markdown 格式 / 文件链接 / 篇幅 | 所有钩子成功；不启动业务或真实中间件 |
| PR / merge queue | 全仓 lint、非 integration 的 Python 测试、前端测试与类型检查 / 构建、完整确定性 Golden Set | `quality-gate` 汇总所有必需任务成功 |
| main push / 手动全量 | PR 检查，加真实 MySQL / Kafka / Redis / ES 与旧 RabbitMQ 兼容验收 | 全量测试和真实 ES 验收成功，无意外跳过 |

Golden Set 当前 40 个用例，成本很低，因此 PR 同样全跑。它覆盖确定性契约，不能替代真实模型效果、线上业务指标或浏览器验收。真实模型评测涉及成本与随机性，后续单独建受控评测入口，不放进当前必需门禁。

main 全量会再次运行全部 Python 测试，确保它们与真实服务测试在同一收集和执行环境中通过。当前不设发布、打 tag、镜像推送或部署任务。

## 安装与统一入口

要求 Python 3.12、Node.js 24；本地契约测试还需要 Redis server、FFmpeg / ffprobe。Windows 可通过 `REDIS_SERVER_PATH` 指定独立测试用 Redis 程序。

```powershell
python -m pip install -e ".[dev]" -r recommend-radio/backend/requirements.txt
npm ci
npm --prefix recommend-radio/frontend ci
pre-commit install
```

常用命令均从仓库根运行：

```powershell
python scripts/quality.py lint
python scripts/quality.py test
python scripts/quality.py golden
python scripts/quality.py frontend
python scripts/quality.py pr
```

其中 `docs` 单独检查文档；`integration` 执行全量 Python 与真实 ES 验收；`main` 在同一入口串起 lint、integration、golden、frontend。环境缺失会失败，不退回只跑单测。

`pre-commit run --all-files` 可手动重跑所有钩子。钩子默认只检查本次提交涉及的文件，CI 会重新检查全仓，不能依赖开发者是否安装钩子。

## 全量服务与环境边界

Actions 在临时 runner 中创建 MySQL 8.4、Kafka 4.0.1、Redis 7.4、ES 8.19.23、RabbitMQ 4.3。账号仅用于这轮临时实例，不使用仓库 secrets 或本机业务配置。

手动复现需显式提供：

- `RADIO_TEST_MYSQL_ADMIN_URL`：独立 loopback MySQL 测试端口，禁止默认 3306 / 3307；夹具为各实例创建随机 schema。
- `RADIO_TEST_ENV_FILE`：只包含测试端点与凭据的文件。需有 `KAFKA_TEST_ROOT_DATABASE_URL`、`KAFKA_TEST_BOOTSTRAP_SERVERS`、`KAFKA_TEST_REDIS_URL`、`SEARCH_ES_ADMIN_USER`、`SEARCH_ES_ADMIN_PASSWORD`。
- `RADIO_TEST_RABBITMQ_URL`：隔离兼容 broker；`RADIO_TEST_ES_URL` 默认 `http://127.0.0.1:19200`。

质量脚本清空业务数据库配置、剔除模型 key 和服务配置，并禁用 dotenv 自动装载。pytest 结果中零测试、未预期 skip、缺失 Kafka 验收都使门禁失败。Windows 无权创建 symlink 的已知备份测试允许单独跳过；完整 Linux CI 不依赖该例外。

Golden Set 的预期标签只参与判定，不进入候选选择；manifest 固定全部必需案例 ID 和数量下限，不能靠删掉案例通过。非有限指标（NaN / Infinity）不能通过阈值检查。新增样本与修复见 [评测扩充记录](golden-expansion-2026-10-21.md)。

## Lint 范围

- `src/`、`tests/`、`scripts/`：使用根 `pyproject.toml` 的完整 Ruff 规则。
- 旧 `recommend-radio/backend` 和 `recommend-radio/scripts`：强制 `E9,F63,F7,F82,F821`，先阻止语法和未定义变量等确定性错误。其余风格和复杂度问题是明确的存量债务，本轮没有使用自动忽略基线伪装全通过。
- 前端：现有 Node 契约测试、`vue-tsc` 与 Vite build。尚未配置 ESLint，不称为前端完整风格验收。

## 文档规范

所有版本管理内的 Markdown，以及本地尚未忽略的新 Markdown，统一检查：

1. markdownlint 默认规则；中文文档不限制单行长度，允许 HTML 与跨章节相同标题，不强制表格列填充样式。
2. 每篇最多 **8,000 个 Unicode 字符**，包括空格、代码、链接和标题；换行统一按 LF 计算，避免平台差异。
3. 相对文件链接必须存在；不请求外部网址，也不把代码块中的示例链接当成真实引用。锚点校验当前未实现。

稳定设计集中维护在主题文档；带日期的记录保存当时证据并标注适用范围。超长文档按主题拆分，原文件保留第一篇并附导航；不靠删除证据或关闭篇幅规则过关。生成的 JUnit / Golden 报告放 `artifacts/quality/`，由 Actions 保留 14 天，不持续追加到正文。

## GitHub 配置与证据

工作流见 [quality.yml](../.github/workflows/quality.yml)。采用只读 `contents` 权限，不用 `pull_request_target` 执行 PR 代码；PR 更新可取消旧检查，main 验证不互相取消。

仓库管理员应把 `quality-gate` 设为 main 的必需检查并要求通过 PR 合并。仅提交 workflow 不会自动开启远端分支保护；main 合并后失败也不能替代合并前检查。

Codex 参考的是“仓库约定给出准确验证命令，完成判断绑定观察到的证据”，不是让模型打分代替 CI。依据：[官方维护实践](https://developers.openai.com/blog/skills-agents-sdk)、[GitHub workflow 语法](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax)、[pre-commit](https://pre-commit.com/)。
