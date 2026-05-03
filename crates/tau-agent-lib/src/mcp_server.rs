//! MCP server — exposes tau's tools, sessions, and tasks as an MCP server.
//!
//! Runs as `tau mcp-server` (stdio transport). Connects to the running tau
//! daemon via Unix socket and translates MCP JSON-RPC calls into tau protocol
//! requests.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use tau_agent_base::paths::socket_path;
use tau_agent_base::protocol::{Request, Response};

/// Tau MCP server handler — bridges MCP tool calls to the tau daemon.
#[derive(Clone)]
pub struct TauMcpServer {
    #[allow(dead_code)]
    tool_router: ToolRouter<TauMcpServer>,
    session_id: Arc<Mutex<Option<String>>>,
    cwd: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BashArgs {
    #[schemars(description = "Shell command to execute")]
    pub command: String,
    #[schemars(description = "Timeout in milliseconds (default: 120000)")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadFileArgs {
    #[schemars(description = "Path to the file to read")]
    pub path: String,
    #[schemars(description = "Start line (1-based)")]
    pub start_line: Option<u64>,
    #[schemars(description = "End line (1-based, inclusive)")]
    pub end_line: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WriteFileArgs {
    #[schemars(description = "Path to the file to write")]
    pub path: String,
    #[schemars(description = "Content to write")]
    pub content: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EditFileArgs {
    #[schemars(description = "JSON array of file edits: [{path, edits: [{old_text, new_text}]}]")]
    pub files: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DiagnosticsScanArgs {
    #[schemars(description = "Paths to scan (empty = project root)")]
    pub paths: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetSkeletonArgs {
    #[schemars(description = "File paths to outline")]
    pub paths: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetFunctionArgs {
    #[schemars(description = "File path")]
    pub path: String,
    #[schemars(description = "Dot-path function name (e.g. MyStruct.method)")]
    pub name: String,
}

// ---- Tokio-native daemon connection (bypasses the smol-based client crate) ----

struct DaemonConn {
    stream: BufReader<UnixStream>,
}

impl DaemonConn {
    async fn connect() -> Result<Self, ErrorData> {
        let path = socket_path();
        let stream = UnixStream::connect(&path).await.map_err(|e| {
            ErrorData::internal_error(
                format!("failed to connect to tau daemon at {}: {e}", path.display()),
                None,
            )
        })?;
        Ok(Self {
            stream: BufReader::new(stream),
        })
    }

    async fn send(&mut self, req: &Request) -> Result<(), ErrorData> {
        let mut line =
            serde_json::to_string(req).map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
        line.push('\n');
        self.stream
            .get_mut()
            .write_all(line.as_bytes())
            .await
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
        self.stream
            .get_mut()
            .flush()
            .await
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
        Ok(())
    }

    async fn recv_streaming<F>(&mut self, mut on_response: F) -> Result<(), ErrorData>
    where
        F: FnMut(&Response),
    {
        let mut line_buf = String::new();
        loop {
            line_buf.clear();
            let n = self
                .stream
                .read_line(&mut line_buf)
                .await
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
            if n == 0 {
                break;
            }
            let trimmed = line_buf.trim();
            if trimmed.is_empty() {
                continue;
            }
            let resp: Response = serde_json::from_str(trimmed)
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
            let is_terminal = !matches!(&resp, Response::Stream { .. });
            on_response(&resp);
            if is_terminal {
                break;
            }
        }
        Ok(())
    }
}

#[tool_router]
impl TauMcpServer {
    pub fn new(cwd: String) -> Self {
        Self {
            tool_router: Self::tool_router(),
            session_id: Arc::new(Mutex::new(None)),
            cwd,
        }
    }

    async fn ensure_session(&self) -> Result<String, ErrorData> {
        let mut session_id = self.session_id.lock().await;
        if let Some(id) = session_id.as_ref() {
            return Ok(id.clone());
        }
        let id = self.create_session().await?;
        *session_id = Some(id.clone());
        Ok(id)
    }

    async fn create_session(&self) -> Result<String, ErrorData> {
        let mut conn = DaemonConn::connect().await?;
        conn.send(&Request::CreateSession {
            model: None,
            provider: None,
            system_prompt: Some("MCP bridge session".into()),
            cwd: Some(self.cwd.clone()),
            parent_id: None,
            child_budget: 0,
            tagline: Some("mcp-server".into()),
            auto_archive: false,
            notify_parent: false,
            project_name: None,
            sandbox_profile: None,
        })
        .await?;

        let mut created_id = None;
        conn.recv_streaming(|resp| {
            if let Response::SessionCreated { session_id } = resp {
                created_id = Some(session_id.clone());
            }
        })
        .await?;

        created_id
            .ok_or_else(|| ErrorData::internal_error("failed to create tau session", None))
    }

    async fn call_tool_on_daemon(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Result<CallToolResult, ErrorData> {
        let session_id = self.ensure_session().await?;
        let mut conn = DaemonConn::connect().await?;
        conn.send(&Request::ExecuteTool {
            session_id,
            tool_name: tool_name.to_string(),
            arguments,
        })
        .await?;

        let mut result_text = String::new();
        let mut is_error = false;
        conn.recv_streaming(|resp| match resp {
            Response::ToolExecuted {
                content,
                is_error: err,
            } => {
                result_text = content.clone();
                is_error = *err;
            }
            Response::Error { message } => {
                result_text = message.clone();
                is_error = true;
            }
            _ => {}
        })
        .await?;

        if is_error {
            Ok(CallToolResult::error(vec![Content::text(result_text)]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(result_text)]))
        }
    }

    #[tool(description = "Execute a shell command")]
    async fn bash(
        &self,
        Parameters(args): Parameters<BashArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut tool_args = serde_json::json!({ "command": args.command });
        if let Some(timeout) = args.timeout_ms {
            tool_args["timeout_ms"] = serde_json::json!(timeout);
        }
        self.call_tool_on_daemon("bash", tool_args).await
    }

    #[tool(description = "Read a file's contents")]
    async fn read_file(
        &self,
        Parameters(args): Parameters<ReadFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut tool_args = serde_json::json!({ "paths": [args.path] });
        if let Some(start) = args.start_line {
            tool_args["start_line"] = serde_json::json!(start);
        }
        if let Some(end) = args.end_line {
            tool_args["end_line"] = serde_json::json!(end);
        }
        self.call_tool_on_daemon("read", tool_args).await
    }

    #[tool(description = "Write content to a file (creates or overwrites)")]
    async fn write_file(
        &self,
        Parameters(args): Parameters<WriteFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool_on_daemon(
            "write",
            serde_json::json!({ "path": args.path, "content": args.content }),
        )
        .await
    }

    #[tool(description = "Edit files with precise text replacements")]
    async fn edit_file(
        &self,
        Parameters(args): Parameters<EditFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool_on_daemon("edit", serde_json::json!({ "files": args.files }))
            .await
    }

    #[tool(description = "Run diagnostics (lint/compile checks)")]
    async fn diagnostics_scan(
        &self,
        Parameters(args): Parameters<DiagnosticsScanArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let tool_args = match args.paths {
            Some(paths) => serde_json::json!({ "paths": paths }),
            None => serde_json::json!({}),
        };
        self.call_tool_on_daemon("diagnostics_scan", tool_args)
            .await
    }

    #[tool(description = "Get a tree-sitter code outline of files")]
    async fn get_file_skeleton(
        &self,
        Parameters(args): Parameters<GetSkeletonArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool_on_daemon(
            "get_file_skeleton",
            serde_json::json!({ "paths": args.paths }),
        )
        .await
    }

    #[tool(description = "Extract a specific function body by name")]
    async fn get_function(
        &self,
        Parameters(args): Parameters<GetFunctionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool_on_daemon(
            "get_function",
            serde_json::json!({ "path": args.path, "name": args.name }),
        )
        .await
    }
}

#[tool_handler]
impl ServerHandler for TauMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new("tau-mcp-server", env!("CARGO_PKG_VERSION")))
        .with_instructions("tau agent MCP server — exposes shell, file, and diagnostic tools from a running tau daemon")
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(ListResourcesResult::with_all_items(vec![
            RawResource::new("tau://sessions", "Sessions")
                .with_description("List of active tau sessions")
                .with_mime_type("application/json")
                .no_annotation(),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        match request.uri.as_str() {
            "tau://sessions" => {
                let mut conn = DaemonConn::connect().await?;
                conn.send(&Request::ListSessions {
                    include_archived: false,
                    project_name: None,
                })
                .await?;

                let mut sessions_json = String::from("[]");
                conn.recv_streaming(|resp| {
                    if let Response::Sessions { sessions } = resp {
                        sessions_json =
                            serde_json::to_string_pretty(sessions).unwrap_or_default();
                    }
                })
                .await?;

                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    sessions_json,
                    "tau://sessions",
                )]))
            }
            uri => Err(ErrorData::invalid_params(
                format!("unknown resource: {uri}"),
                None,
            )),
        }
    }
}

/// Run the MCP server on stdio. Blocks until the client disconnects.
pub async fn run_stdio(cwd: String) -> Result<(), Box<dyn std::error::Error>> {
    use rmcp::ServiceExt;
    use rmcp::transport::stdio;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("tau_mcp=info".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    tracing::info!("tau MCP server starting (stdio)");

    let server = TauMcpServer::new(cwd);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;

    tracing::info!("tau MCP server stopped");
    Ok(())
}
