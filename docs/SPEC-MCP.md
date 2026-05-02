# Spec: MCP Support (Phase 2)

## Overview

Add Model Context Protocol support to tau in two directions:

- **2a — MCP Client**: tau connects to external MCP servers, surfaces
  their tools/resources/prompts to the LLM alongside built-in tools
- **2b — MCP Server**: tau exposes its own capabilities as an MCP server
  for other agents/editors to consume

MCP is the single highest-leverage gap — it moves tau from closed tool
set to open platform with one integration.

## Dependencies

- `rmcp` crate (MIT) — Rust MCP SDK, stdio + HTTP transports
- No new external services required

---

## 2a: MCP Client

### Config

New file: `mcp.toml`, loaded via config chain.

**Security:** `allow_project_tier = false` — project `.tau/mcp.toml` is
skipped. Only operator (`~/.config/tau/projects/{name}/mcp.toml`) and
global (`~/.config/tau/mcp.toml`) tiers are loaded. Rationale: MCP
servers execute arbitrary code; a malicious repo could register a
backdoor server via project-tier config.

```toml
# ~/.config/tau/mcp.toml

[servers.filesystem]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/home/user/projects"]
env = { NODE_ENV = "production" }
enabled = true

[servers.postgres]
command = "uvx"
args = ["mcp-server-postgres", "--connection-string", "postgresql://..."]
enabled = true

[servers.remote-api]
transport = "http-sse"
url = "https://api.example.com/mcp"
headers = { Authorization = "Bearer ${MCP_API_TOKEN}" }
enabled = true

# Per-server tool filtering
[servers.filesystem.tools]
include = ["read_file", "list_directory"]   # whitelist (if set, only these)
exclude = []                                 # blacklist (applied after include)

# Per-server resource filtering
[servers.filesystem.resources]
include = []
exclude = []
```

**Environment variable expansion:** `${VAR_NAME}` syntax in string
values, resolved at server spawn time. Unresolved vars are an error (do
not silently pass empty strings).

**Merge strategy:** `load_first()` — operator tier wins entirely over
global if present (same as `plugins.toml`).

### MCP Server Lifecycle

New module: `tau-agent-lib::mcp_client`

**Startup:** When `PluginManager` initializes (or on config reload),
parse `mcp.toml` and spawn each enabled server.

For stdio servers:
- Spawn child process with configured `command`, `args`, `env`
- Connect via `rmcp` stdio transport
- Run MCP `initialize` handshake
- Store the `rmcp::Client` handle

For HTTP+SSE servers:
- Connect via `rmcp` HTTP transport to configured `url` with `headers`
- Same initialize handshake

**Health check:** Periodic ping (every 30s). If a server fails to
respond within 5s, mark it unhealthy. After 3 consecutive failures,
attempt restart (max 3 restarts, then mark permanently failed and log
error).

**Shutdown:** On daemon shutdown, send MCP `shutdown` + `exit` to each
server. Kill after 5s grace period.

**Hot reload:** On `/config reload`, diff the new `mcp.toml` against
running servers. Stop removed servers, start added ones, restart changed
ones.

### Tool Surfacing

After MCP `initialize`, call `tools/list` on each server. For each MCP
tool:

1. **Create `Tool` schema** for the LLM API:
   - `name`: `"mcp_{server_name}_{tool_name}"` (namespaced to avoid
     collisions with built-in tools)
   - `description`: from MCP tool schema
   - `parameters`: from MCP tool `inputSchema` (JSON Schema)

2. **Create `ToolPrompt`** for the system prompt:
   - `prompt_snippet`: `"{description} (via {server_name} MCP server)"`
   - `prompt_guidelines`: empty unless manually configured

3. **Apply tool filtering** from config (`include`/`exclude`)

4. **Register** via a new `McpToolProvider` that implements the same
   interface as `PluginHandle::tool_schemas()` and
   `PluginHandle::tool_prompts()`

Integration point: `PluginManager::tool_schemas()` (plugin.rs:1324) and
`PluginManager::tool_prompts()` (plugin.rs:1343) — extend both to also
include MCP tools.

### Tool Execution

When the LLM calls an `mcp_*` tool:

1. `PluginManager::execute_tool()` checks tool name prefix
2. If `mcp_` prefix, route to `McpToolProvider::execute()`
3. Parse server name and tool name from the prefixed name
4. Call `tools/call` on the appropriate `rmcp::Client` with the
   arguments
5. Convert MCP `CallToolResult` → tau `ToolResult`:
   - `content[].text` → `ToolResult::text()`
   - `content[].image` → `ToolResult::image()` (base64)
   - `isError: true` → `ToolResult::error()`
6. Return to agent loop

**Timeout:** Per-tool-call timeout of 120s (configurable per-server in
`mcp.toml`). On timeout, return error result to LLM.

**Cancellation:** If the agent loop cancels a tool call, send MCP
`notifications/cancelled` to the server.

### Resource Surfacing

After `initialize`, call `resources/list` on each server. Resources
become available as attachable context:

- New tool: `mcp_read_resource` — takes `server` and `uri`, calls
  `resources/read`, returns content
- Resources listed in system prompt under a new section:
  `"Available MCP resources:"` with `server_name/resource_name:
  description`
- Apply resource filtering from config

Resources are **not** auto-injected into context (too expensive). The
LLM decides when to read them.

### Prompt Surfacing

After `initialize`, call `prompts/list` on each server. MCP prompts
become slash commands:

- `/mcp:{server_name}:{prompt_name}` — calls `prompts/get` with any
  required arguments, injects the returned messages into the
  conversation
- Listed via `/mcp` command (shows all available MCP prompts)

### Dynamic Tool Updates

Subscribe to MCP `notifications/tools/list_changed`. On notification,
re-fetch `tools/list` and update the tool registry. The next agent turn
picks up the new tools.

Same for `notifications/resources/list_changed`.

### Where to Build

| Component | Location |
|---|---|
| Config types | `tau-agent-base::mcp_config` (new module) |
| Server lifecycle | `tau-agent-lib::mcp_client` (new module) |
| Tool/resource provider | `tau-agent-lib::mcp_client::McpToolProvider` |
| Integration into PluginManager | `tau-agent-lib::plugin.rs` — extend `tool_schemas()`, `tool_prompts()`, `execute_tool()` |
| Slash commands | `tau-agent-lib::dispatch.rs` — handle `/mcp` prefix |
| Config chain entry | `tau-agent-base::config_chain` — add `mcp.toml` |

### Error Handling

- Server spawn failure → log error, skip server, continue with others
- Tool call to unhealthy server → return error result to LLM with
  message "MCP server {name} is unavailable"
- Malformed tool response → return error result with raw content
- Config parse failure → reject reload, keep running config, log error

---

## 2b: MCP Server

Expose tau's capabilities as an MCP server so other agents and editors
can use tau as a tool provider.

### Transports

**stdio** (primary): `tau mcp-server` subcommand. Reads MCP JSON-RPC
from stdin, writes to stdout. For integration with editors (VS Code,
Zed) and other agent harnesses.

**HTTP+SSE** (secondary): `tau mcp-server --http :8080`. For web
integrations and remote access. Gated behind explicit opt-in flag.

### Exposed Tools

Map tau's built-in tools to MCP tools:

| MCP Tool | Tau Tool | Notes |
|---|---|---|
| `bash` | `bash` | Shell execution in tau's context |
| `read_file` | `read` | File reading with line ranges |
| `write_file` | `write` | File creation/overwrite |
| `edit_file` | `edit` | Multi-location editing |
| `get_file_skeleton` | `get_file_skeleton` | Tree-sitter outline |
| `get_function` | `get_function` | Extract function bodies |
| `diagnostics_scan` | `diagnostics_scan` | Lint/compile checks |
| `session_spawn` | `session_spawn` | Start a tau session |
| `session_join` | `session_join` | Wait for session result |
| `session_message` | `session_message` | Send message to session |
| `task_create` | `task_create` | Create task on board |
| `task_list` | `task_list` | List tasks |
| `task_get` | `task_get` | Get task details |

Tool schemas generated from the existing `PluginToolDef` definitions —
single source of truth, no duplication.

### Exposed Resources

| MCP Resource | Source |
|---|---|
| `tau://sessions` | List of active sessions |
| `tau://sessions/{id}` | Session transcript |
| `tau://tasks` | Task board |
| `tau://tasks/{id}` | Task details + messages |
| `tau://project/instructions` | Loaded instructions.toml content |

Resources use URI templates. `resources/read` fetches live data from
tau's DB/state.

### Exposed Prompts

| MCP Prompt | Effect |
|---|---|
| `code-review` | Returns a prompt for reviewing a diff |
| `implement` | Returns a prompt for implementing a spec |
| `debug` | Returns a prompt for debugging an issue |

Prompts are optional — useful for editors that support MCP prompt
selection.

### Authentication

- **stdio**: inherits process permissions, no auth needed (same trust
  model as current plugin protocol)
- **HTTP**: bearer token auth. Token generated on first start, stored
  in `~/.config/tau/mcp-server-token`. Logged to stderr on startup.
  Configurable via `TAU_MCP_TOKEN` env var.

### Session Binding

The MCP server operates within a tau session context. On first tool
call, if no session exists, auto-create one. Session ID can be specified
via MCP `initialize` params (`tau_session_id` extension field) or
auto-generated.

This means an external agent using tau via MCP gets a full session with
history, compaction, and all the usual lifecycle.

### Where to Build

| Component | Location |
|---|---|
| MCP server binary entry point | `tau-agent/src/mcp_server.rs` (new) |
| Tool schema conversion | `tau-agent-lib::mcp_server` (new module) — convert `PluginToolDef` → MCP tool schemas |
| Resource handlers | `tau-agent-lib::mcp_server::resources` |
| Transport setup | Use `rmcp` server-side stdio/HTTP |
| CLI subcommand | `tau-agent/src/cli.rs` — add `mcp-server` subcommand |

### Implementation Order

1. stdio transport + core tools (bash, read, write, edit) — minimum
   viable MCP server
2. Resource exposure (sessions, tasks)
3. Full tool set including session/task management
4. HTTP transport
5. Prompts

---

## Open Questions

- [ ] Should MCP server tools be available only when tau daemon is
  running, or should `tau mcp-server` be standalone with its own agent
  loop?
- [ ] Should MCP tool names be configurable (allow users to alias
  `mcp_filesystem_read_file` to `fs_read`)?
- [ ] Per-session vs global MCP server config — should different
  sessions be able to connect to different MCP servers?
- [ ] Should MCP resources support subscriptions
  (`resources/subscribe`)?
- [ ] Rate limiting on HTTP transport?
