# 面试主线与 gRPC / Function Calling / MCP 选型

日期：2026-10-19。基于当前工作区代码与官方协议资料。本次只新增面试分析文档，不修改实现、不新增测试；不把已实现能力表述为已上线收益。

## 1. 推荐主讲哪一个成就

默认按 Agent / AI 应用研发岗位考虑，主讲“把 Agent 工具执行做成可恢复、可追溯、能控制副作用的业务流程”，Feed 和音乐推荐作为真实应用场景。

选择理由：

1. 问题足够具体：模型或外部工具超时、进程中断、消息重复时，任务不能丢，也不能盲目重做副作用。
2. 贡献能与框架区分：LangGraph 管节点控制流，领域状态、工具流水、幂等键、租约、对账与业务完成判据仍需要自己实现。
3. 有完整追问链：任务提交 → outbox → worker → Agent checkpoint → 工具执行 → 故障恢复 → trace 与回归。
4. 可以用代码和测试举证，并明确单机 SQLite、多机接管、未知副作用等边界。

纯后端或内容平台岗位可改为主讲 Feed 的媒体生命周期与异步一致性；不必为了 AI 标签忽略岗位匹配。

## 2. 可改写成个人经历的一分钟回答

> 我最有成就的部分，是把 Agent 的工具调用做成了可恢复的业务执行流程。具体问题是：模型调用和外部工具都可能超时，进程也可能在执行中退出，而副作用不能简单重试。
>
> 我把控制流和执行事实分开：LangGraph 编排节点，业务库持久化后台任务，工具流水保存稳定调用身份、状态和结果。恢复时复用已提交阶段的结果，安全操作有限重试，结果未知的非幂等操作进入对账。
>
> 同时记录运行 trace 和业务评测数据，让每次失败可以追到具体阶段。验证围绕消息重投、旧 worker 结果、同步工具超时和压缩后约束保留展开。
>
> 我认为这部分价值是定义清楚系统何时能继续、何时必须停下来核实；目前仍有本地 checkpoint 和多机接管边界，不会把它表述为所有任务都能自动恢复。

使用者应把“我做了”限制在本人实际承担的工作，并准备对应提交、故障案例和测试证据。本次会话此前运行的 36 项回归覆盖 durable_jobs、loop reliability、FullTrace、sampling 和 context constraint selection；并不等于真实 Kafka/MySQL 集群故障验收，也没有吞吐或收益提升数据。

## 3. 三者分别承担什么

| 机制 | 所在边界与职责 | 本项目中的证据 |
| --- | --- | --- |
| Function Calling | 模型输出工具名与参数，宿主校验、授权并执行 | OpenAICompatibleModelGateway 将 ToolDefinition 转为 function schema，解析 tool_calls；ReliableToolRuntime 负责执行 |
| gRPC | 内部服务间 RPC 契约与调用 | AmemService 定义 RecordBehavior、GetMusicProfile 等方法；AmemGrpcBridge 使用生成 stub 并传 timeout |
| MCP | AI 宿主与工具/上下文提供方之间的标准发现和调用接口 | 当前主链路没有要求用 MCP 暴露或接入工具，可在需要互操作时增加适配 |

Function Calling 与 MCP 可以配合：宿主把 MCP tools 元数据转换成模型可用的工具定义，再把模型请求分发到 MCP server。MCP 官方架构明确描述了 AI 应用获取工具定义并把工具结果送回模型的模式。gRPC 也可以继续保留在 MCP server 后面的业务服务层。

参考：[MCP 架构](https://modelcontextprotocol.io/docs/learn/architecture)、[MCP Tools](https://modelcontextprotocol.io/specification/latest/server/tools)、[gRPC 核心概念](https://grpc.io/docs/what-is-grpc/core-concepts/)。

## 4. 为什么这里保留 gRPC 和直接工具注册

这是基于当前架构的选型解释，不能声称已查到项目创建时的全部历史动机。

### 内部记忆服务的接口已知且受控

RecordBehavior、RecordProfileStatement、GetMusicProfile 等是固定业务服务接口，也供确定性代码调用。独立部署时，用 proto 定义方法、生成 stub、显式传 deadline/timeout，便于维护内部调用契约。无需先把每次内部服务读取包装成通用 AI 工具发现。

proto 中仍有 payload_json/profile_json 等 bytes 字段，所以应表述为“有明确方法与部分强类型字段”，不要说所有业务 payload 都已经完全由 protobuf 强类型约束。

### 工具集合由同一应用管理

当前 runtime 已有 ToolRegistry、JSON Schema 校验、工具策略、审批和执行流水。对于应用自身的有限工具集，直接注册可以减少额外的协议接入与工具映射工作；工具发现互操作的收益尚未成为主诉求。

这不是 MCP 无法执行这些工具，而是当前没有足够的跨宿主复用需求来抵消新增适配与维护成本。不能在没有基准测试时给出 MCP 延迟更高或 gRPC 提升某个百分比的结论。

### 可靠性是领域层的责任

无论工具经进程内调用、gRPC 还是 MCP，外部副作用已发生但结果尚未落库的窗口都需要业务幂等或对账。接入协议、RPC deadline、JSON-RPC 请求 ID 都不能直接证明某个副作用只发生了一次。

MCP 本身有丰富的协议能力，不应绝对表述为“不支持长任务、取消或鉴权”。面试应聚焦：即使换协议，本项目仍需要保留领域 checkpoint、稳定业务幂等键、权限与副作用处理规则。

## 5. 什么时候采用 MCP

当 AMEM 能力需要被多个 AI 宿主复用，或者 Agent 要接入多家已有 MCP 工具服务时，标准工具发现、输入输出约定和生态互操作会产生明确收益。此时优先增加边界适配，复用现有业务服务和可靠执行层。

```text
AI 宿主 / 模型工具调用
  -> MCP 适配与工具暴露
  -> 权限与可靠执行层
  -> 现有业务逻辑 / gRPC 服务
```

适配时需要解决用户身份映射、工具名与 schema 版本、超时取消传递、稳定业务 call_id、错误映射及跨调用 trace；不能只把 RPC 请求参数转成 JSON 就认为集成完成。

## 6. 可直接用于回答选型的表述

> 我把它们按边界区分：Function Calling 是模型表达工具调用意图的接口，gRPC 是内部服务 RPC，MCP 负责 AI 宿主与工具提供方互操作。当前记忆服务的方法固定，业务工具由同一个应用管理，所以保留 gRPC 服务接口与直接注册的工具执行链路。这样能复用现有契约、超时和执行流水。
>
> 如果未来需要把记忆能力提供给多个 AI 宿主，或者接入已有 MCP 工具，我会在边界增加 MCP 适配。底层服务仍可以继续用 gRPC；幂等、权限和恢复仍由领域层负责。

## 7. 当前实现必须讲准

- `amem_runtime.py` 默认 AMEM_TRANSPORT=embedded，当前 Compose 也设置 embedded；gRPC 是可选独立服务接入路径。不要说默认部署所有记忆读写都经过 gRPC。
- `music_agent.py` 的 OperationGateway 是确定性阶段路由器，标记 execution_kind=control。不要把它描述为 LLM 自主选择下一阶段。
- 通用 `OpenAICompatibleModelGateway` 支持真正的模型 tool_calls；具体业务路径是否使用它，要按入口说明。
- 当前客户端用 insecure_channel，不能把 TLS/mTLS 描述为项目已经落地的能力。
- 不要宣称 exactly-once、集群高可用、线上时延收益或用户收益，除非另有对应的部署与实验记录。

代码依据：[amem.proto](../recommend-radio/api/proto/amem/v1/amem.proto)、[amem_runtime.py](../recommend-radio/backend/amem_runtime.py)、[amem_grpc_bridge.py](../recommend-radio/backend/amem_grpc_bridge.py)、[model_gateway.py](../src/agent_memory_runtime/agent/model_gateway.py)、[tool_runtime.py](../src/agent_memory_runtime/agent/tool_runtime.py)、[music_agent.py](../recommend-radio/backend/music_agent.py)。
