# MCP Gateway

Rust 实现的 MCP 工具聚合网关。客户端连接一个网关，网关从已配置的 MCP 后端读取真实工具定义，并转发经过参数校验的工具调用。

## 当前支持

- 客户端入口：无会话 Streamable HTTP `POST /mcp`、旧 HTTP+SSE，以及 stdio → HTTP 桥接。
- 后端：Streamable HTTP、旧 HTTP+SSE、持久 stdio 进程；调用前完成 MCP 初始化。
- 工具发现：保留描述、真实 inputSchema、工具扩展字段，读取后端分页列表，工具目录缓存 60 秒。
- 工具调用：校验参数、匹配响应 ID，保留 JSON-RPC 错误码/data 和工具 structuredContent。
- 鉴权：API Key；可选外部 OAuth 授权服务签发的 RS256 JWT，校验签名、issuer、audience、exp、nbf、sub 和 scope。
- 缓存：仅显式允许的只读工具，按后端、凭据/主体、权限范围、工具定义和参数隔离；合并相同缓存未命中请求。
- 运维：全局请求与后端并发限制、超时、SSE 背压和清理、JSON 日志、就绪检查、指标、30 秒优雅停机。

网关按工具名路由。多个后端可声明相同非空 `replica_group`，工具的完整定义（含 Schema、注解）必须一致，才允许共用工具名；目录只展示一次，调用按正整数 `weight` 做加权轮询。不同组的重名工具仍被拒绝。缓存按具体副本隔离，不假定不同副本的数据一致。所有副本仍必须成功发现目录，调用失败不会自动重放到另一副本。`router/engine.rs` 中的语义路由等历史预留类型尚未接入运行链路。

## 构建与运行

仓库固定 Rust 1.99.0，并提交 Cargo.lock。

```bash
cargo test --workspace --locked
cargo run -p mcp-gateway-server -- validate-config -c config/gateway.toml
cargo run -p mcp-gateway-server -- run -c config/gateway.toml
```

默认示例监听 `127.0.0.1:8080`，后端地址是示例，需要替换成真实 MCP 服务。HTTP 后端必须支持初始化以及规范的 JSON/SSE 响应。旧 SSE 后端的 endpoint 必须指向事件流入口；网关会从 `endpoint` 事件获得 POST 地址。

工具配置 `tools` 是允许暴露的工具名单；为空则发现并暴露该后端全部工具。不同后端的同名工具会被拒绝，避免调用时静默选择错误后端。配置中的工具必须真实存在于后端目录中。

```toml
[gateway]
name = "mcp-gateway"
listen_addr = "127.0.0.1:8080"
allowed_origins = ["https://your-client.example"]
max_inflight_requests = 256
max_sse_sessions = 1024
sse_ttl_seconds = 1800
sse_queue_capacity = 32

[[backends]]
name = "business-tools"
transport = "streamable-http"
endpoint = "https://backend.example/mcp"
tools = ["get_profile"]
max_connections = 10
# 包含等待后端并发配额、初始化和后端交互的时间。
timeout_ms = 30000
headers = { Authorization = "${BUSINESS_BACKEND_AUTHORIZATION}" }
```

`${ENV_NAME}` 占位符支持 backend headers/env 的完整字段值。它不做任意字符串模板替换。后端鉴权使用这些固定凭据，客户端凭据不会透传到后端；若需要逐用户的后端权限映射，应增加专门的凭据交换实现。

## 客户端传输

Streamable HTTP 请求必须带 `Content-Type: application/json`，以及包含 `application/json` 和 `text/event-stream` 的 Accept。通知返回 HTTP 202 空响应。网关对外不分配 MCP Session ID；`GET /mcp` 和 `DELETE /mcp` 返回 405，是该无会话模式的预期行为。

已支持协议协商：`2025-06-18`、`2025-03-26`、`2024-11-05`。后端 HTTP 会话头和协议版本头由持久后端客户端管理。未支持的客户端首选版本会协商为 `2025-06-18`。

旧 SSE 客户端连接 `GET /mcp/sse`，使用 endpoint 事件中的相对 URL POST 消息，结果从 SSE 流返回。每个会话绑定调用身份，有 TTL 和队列限制；429 表示请求未获准进入处理。断开连接会清理会话，不会自动重放已执行的工具调用。

stdio 桥接示例：

```bash
export MCP_GATEWAY_API_KEY='your-key'
cargo run -p mcp-gateway-stdio -- --gateway http://127.0.0.1:8080/mcp
```

stdout 只输出 JSON-RPC 响应；通知不会生成额外响应，错误保留原始请求 ID 类型。桥接当前按输入顺序处理请求。单条 stdio 输入上限 2 MiB，后端消息/HTTP 响应上限 8 MiB。

## API Key 和 OAuth

API Key 可使用配置中的头（默认 x-api-key）或 `Authorization: Bearer`。

```toml
[auth]
enabled = true
api_key_env = ["MCP_GATEWAY_API_KEY"]
api_key_header = "x-api-key"
```

OAuth 示例见 `config/oauth.example.toml`。网关是资源服务器，登录、同意授权、PKCE、令牌签发/刷新和客户端注册由 Ory Hydra 等外部授权服务完成。

- 公钥来自显式配置的 JWKS 地址；不信任 token 自带的公钥 URL。
- JWKS 缓存 300 秒；未知 kid 可触发刷新，刷新间隔至少 5 秒，失败关闭访问。
- `/.well-known/oauth-protected-resource` 公开资源发现；401 响应的 WWW-Authenticate 指向该地址。
- 全局 scope 不足返回 403；工具 scope 不足时从 tools/list 隐藏，并拒绝调用。
- API Key 与 OAuth 可以同时开启，用于迁移。有效 API Key 具有全部已配置工具权限。
- OAuth 的 issuer、resource_url、JWKS URL 使用 HTTPS；本地开发允许 localhost HTTP。
- 当前只支持 RS256 JWT；Opaque Token introspection 和 OAuth 登录页面尚未实现。

反向代理部署时，resource_url 必须填写客户端访问的公开 MCP 地址。请求携带 Origin 时，必须与 allowed_origins 中的值精确匹配；没有 Origin 的服务端客户端可正常访问。

## 缓存

只有同时开启全局缓存、并将工具加入后端 `cache_tools` 后才会缓存。不要把有写入、副作用或不可共享结果的工具加入名单。工具 `isError=true`、RPC/传输失败和超大结果不缓存。

```toml
[[backends]]
name = "read-tools"
transport = "streamable-http"
endpoint = "https://backend.example/mcp"
cache_tools = ["get_profile"]

[cache]
enabled = true
max_capacity = 10000
max_bytes = 67108864
max_result_bytes = 1048576
ttl_seconds = 300
```

缓存是每个网关进程独立的。max_bytes 按序列化结果和 key 大小计权，不代表整个进程 RSS 上限；Moka 淘汰和条目数统计是最终一致的。TTL 是全局策略。缓存等待有后端 timeout_ms 上限，后端调用另有相同期限；工具目录首次发现也可能增加冷启动耗时。

## 健康检查、指标与停机

| 地址 | 用途 | 鉴权 |
|---|---|---|
| `/health` | 进程存活 | 无 |
| `/ready` | 获取全部后端工具目录，5 秒内完成返回 200，否则 503 | 无 |
| `/metrics` | JSON 计数与耗时总和 | 与 MCP 相同 |
| `/metrics/prometheus` | Prometheus counters 和 HTTP 耗时 histogram | 与 MCP 相同 |

就绪检查成功表示目录发现可用，目录缓存期间不代表实时后端探活。JSON-RPC 错误即使处于 HTTP 200 中也单独统计。日志不记录认证头和 URL 查询字符串。

SIGINT/SIGTERM 会停止接收新连接，关闭 SSE 会话，并等待在途 HTTP 请求至多 30 秒，再清理后端连接和子进程。没有自动重试写入工具，不提供 exactly-once 保证。

## 验证与能力边界

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

测试覆盖 HTTP/SSE 会话头、错误响应识别、通知语义、真实工具发现、Schema 校验、stdio 进程复用/超时、SSE 身份隔离、缓存合并、指标和 OAuth 校验。stdio 集成测试需要 Python 3。

目前聚焦工具聚合：resources、prompts、sampling、elicitation、进度转发、SSE 断线恢复、多实例会话共享尚未实现。`notifications/cancelled` 按身份和请求 ID 取消在途调用，旧 SSE 额外按会话隔离。initialize 不接受取消；同一身份的在途 ID 不可重复。网关取消等待并向原后端连接发送取消通知（最多等待 1 秒），超时也触发该通知；后端是否停止操作取决于其实现，取消不能撤销已产生的副作用。容量耗尽时保留 16 个控制请求槽，取消消息限制为 4 KiB、读取限时 2 秒。无鉴权 HTTP 使用共享 public 身份，多客户端应使用独立凭据和不重复的 ID。后端通知不会向下游客户端转发，因此不宣告相关能力。所有后端必须成功发现目录，tools/list 才会返回，后端故障时不会静默提供不完整目录。

### 显式副本组

```toml
[[backends]]
name = "echo-a"
endpoint = "http://127.0.0.1:9001/mcp"
replica_group = "echo"
weight = 1

[[backends]]
name = "echo-b"
endpoint = "http://127.0.0.1:9002/mcp"
replica_group = "echo"
weight = 3
```

连续 4 次路由中，a 获得 1 次、b 获得 3 次。该策略分配请求数量，不感知实时负载；不提供故障自动摘除、跨副本重放或会话粘滞。副本组适用于可独立处理调用的等价后端。

### 后端配置重载

Unix/Linux 中使用 `mcp-gateway run --config ...` 启动后，可修改配置文件的 `[[backends]]` 部分并向主进程发送 `SIGHUP`。网关读取同一文件、重新解析环境变量，校验配置，建立候选连接并发现所有工具目录；成功后原子替换。失败或超过 30 秒保留旧目录和连接，不自动重试。旧请求保留原后端连接直至结束；新请求使用新目录，缓存按后端配置隔离。

监听地址、鉴权、Origin、缓存全局参数、并发容量和日志设置的变更会被拒绝，需重启。Windows 暂不提供信号重载；没有公开配置写入接口。目录变更不主动通知客户端，客户端需重新调用 tools/list。配置重载成功/失败写入日志，不输出配置值或凭据。
