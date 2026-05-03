//! MCP client manager — connects to external MCP servers, surfaces their tools.
//!
//! Runs MCP clients on a dedicated tokio runtime (rmcp requires tokio) and
//! bridges to tau's smol-based server via synchronous method calls that
//! `tokio::runtime::Handle::block_on()` into the tokio runtime.

use std::collections::HashMap;
use std::time::Duration;

use tau_agent_base::mcp_config::{
    McpConfig, McpServerConfig, McpTransport, expand_env_vars, load_mcp_config,
};
use tau_agent_base::tool_prompt::ToolPrompt;
use tau_agent_base::types::{Tool, ToolCall, ToolResultContent, ToolResultMessage, TextContent, ImageContent};

/// Status of an MCP server connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerStatus {
    Running,
    Failed(String),
    Stopped,
}

/// Handle to a single MCP server connection.
struct McpServerHandle {
    #[allow(dead_code)]
    name: String,
    config: McpServerConfig,
    status: McpServerStatus,
    client: Option<rmcp::service::RunningService<rmcp::RoleClient, ()>>,
    cached_tools: Vec<rmcp::model::Tool>,
}

/// MCP client manager — lifecycle, tool surfacing, tool execution.
pub struct McpManager {
    servers: HashMap<String, McpServerHandle>,
    tool_cache: Vec<(Vec<Tool>, Vec<ToolPrompt>)>,
    rt: tokio::runtime::Runtime,
}

impl McpManager {
    /// Create a new McpManager with a dedicated tokio runtime.
    pub fn new() -> crate::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("tau-mcp")
            .build()
            .map_err(|e| crate::Error::Io(format!("failed to create tokio runtime for MCP: {e}")))?;
        Ok(Self {
            servers: HashMap::new(),
            tool_cache: Vec::new(),
            rt,
        })
    }

    /// Load config and connect to all enabled MCP servers.
    pub fn load_and_connect(
        &mut self,
        project_name: Option<&str>,
        project_path: Option<&str>,
    ) {
        let config = load_mcp_config(project_name, project_path);
        self.connect_servers(config);
    }

    /// Connect to servers defined in the given config.
    fn connect_servers(&mut self, config: McpConfig) {
        // Stop any existing servers
        self.stop_all();

        for (name, server_config) in config.servers {
            if !server_config.enabled {
                tracing::info!(server = %name, "MCP server disabled, skipping");
                continue;
            }
            match self.connect_server(&name, &server_config) {
                Ok(handle) => {
                    let tool_count = handle.cached_tools.len();
                    tracing::info!(
                        server = %name,
                        tools = tool_count,
                        "MCP server connected"
                    );
                    self.servers.insert(name, handle);
                }
                Err(e) => {
                    tracing::warn!(server = %name, %e, "MCP server failed to connect");
                    self.servers.insert(
                        name.clone(),
                        McpServerHandle {
                            name,
                            config: server_config,
                            status: McpServerStatus::Failed(e.to_string()),
                            client: None,
                            cached_tools: Vec::new(),
                        },
                    );
                }
            }
        }
        self.rebuild_tool_cache();
    }

    /// Connect to a single MCP server.
    fn connect_server(
        &self,
        name: &str,
        config: &McpServerConfig,
    ) -> crate::Result<McpServerHandle> {
        let handle = self.rt.handle();

        match config.transport {
            McpTransport::Stdio => {
                let command = config
                    .command
                    .as_deref()
                    .ok_or_else(|| crate::Error::Io(format!("MCP server '{name}' has no command")))?;

                let command = expand_env_vars(command)
                    .map_err(|e| crate::Error::Io(format!("MCP server '{name}' env error: {e}")))?;

                let args: Vec<String> = config
                    .args
                    .iter()
                    .map(|a| expand_env_vars(a))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| crate::Error::Io(format!("MCP server '{name}' env error: {e}")))?;

                let mut env: HashMap<String, String> = HashMap::new();
                for (k, v) in &config.env {
                    let expanded = expand_env_vars(v)
                        .map_err(|e| crate::Error::Io(format!("MCP server '{name}' env error in {k}: {e}")))?;
                    env.insert(k.clone(), expanded);
                }

                let config_clone = config.clone();
                let name_str = name.to_string();

                let (client, tools) = handle
                    .block_on(async {
                        Self::connect_stdio(&name_str, &command, &args, &env).await
                    })
                    .map_err(|e| crate::Error::Io(format!("MCP server '{name}' connection failed: {e}")))?;

                let filtered_tools: Vec<rmcp::model::Tool> = tools
                    .into_iter()
                    .filter(|t| config_clone.tools.allows(&t.name))
                    .collect();

                Ok(McpServerHandle {
                    name: name.to_string(),
                    config: config_clone,
                    status: McpServerStatus::Running,
                    client: Some(client),
                    cached_tools: filtered_tools,
                })
            }
            McpTransport::HttpSse => {
                let url = config
                    .url
                    .as_deref()
                    .ok_or_else(|| crate::Error::Io(format!("MCP server '{name}' has no url")))?;

                let url = expand_env_vars(url)
                    .map_err(|e| crate::Error::Io(format!("MCP server '{name}' env error: {e}")))?;

                let config_clone = config.clone();
                let name_str = name.to_string();

                let (client, tools) = handle
                    .block_on(async {
                        Self::connect_http(&name_str, &url).await
                    })
                    .map_err(|e| crate::Error::Io(format!("MCP server '{name}' connection failed: {e}")))?;

                let filtered_tools: Vec<rmcp::model::Tool> = tools
                    .into_iter()
                    .filter(|t| config_clone.tools.allows(&t.name))
                    .collect();

                Ok(McpServerHandle {
                    name: name.to_string(),
                    config: config_clone,
                    status: McpServerStatus::Running,
                    client: Some(client),
                    cached_tools: filtered_tools,
                })
            }
        }
    }

    /// Connect to an MCP server via stdio (child process).
    async fn connect_stdio(
        name: &str,
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> Result<
        (
            rmcp::service::RunningService<rmcp::RoleClient, ()>,
            Vec<rmcp::model::Tool>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        use rmcp::ServiceExt;
        use rmcp::transport::TokioChildProcess;

        let env_clone = env.clone();
        let args_owned: Vec<String> = args.to_vec();
        let command_owned = command.to_string();

        let child = TokioChildProcess::new({
            let mut cmd = tokio::process::Command::new(&command_owned);
            cmd.args(&args_owned);
            for (k, v) in &env_clone {
                cmd.env(k, v);
            }
            cmd
        })?;

        let client = ().serve(child).await?;

        let tools = client.list_all_tools().await.unwrap_or_else(|e| {
            tracing::warn!(server = %name, %e, "failed to list MCP tools");
            Vec::new()
        });

        Ok((client, tools))
    }

    /// Connect to an MCP server via HTTP+SSE.
    async fn connect_http(
        name: &str,
        url: &str,
    ) -> Result<
        (
            rmcp::service::RunningService<rmcp::RoleClient, ()>,
            Vec<rmcp::model::Tool>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        use rmcp::ServiceExt;
        use rmcp::transport::StreamableHttpClientTransport;

        let transport = StreamableHttpClientTransport::from_uri(url);
        let client = ().serve(transport).await?;

        let tools = client.list_all_tools().await.unwrap_or_else(|e| {
            tracing::warn!(server = %name, %e, "failed to list MCP tools");
            Vec::new()
        });

        Ok((client, tools))
    }

    /// Convert MCP tools to tau's Tool and ToolPrompt types, rebuilding the cache.
    fn rebuild_tool_cache(&mut self) {
        let mut cache = Vec::new();
        for (server_name, handle) in &self.servers {
            if handle.status != McpServerStatus::Running {
                continue;
            }
            let mut tools = Vec::new();
            let mut prompts = Vec::new();
            for mcp_tool in &handle.cached_tools {
                let prefixed_name = format!("mcp_{server_name}_{}", mcp_tool.name);
                let description = mcp_tool
                    .description
                    .as_deref()
                    .unwrap_or("(no description)")
                    .to_string();

                let parameters = serde_json::Value::Object(
                    mcp_tool.input_schema.as_ref().clone(),
                );

                tools.push(Tool {
                    name: prefixed_name.clone(),
                    description: description.clone(),
                    parameters,
                });
                prompts.push(ToolPrompt {
                    name: prefixed_name,
                    snippet: format!("{description} (via {server_name} MCP server)"),
                    guidelines: Vec::new(),
                });
            }
            if !tools.is_empty() {
                cache.push((tools, prompts));
            }
        }
        self.tool_cache = cache;
    }

    /// Get cached tool schemas for all running MCP servers.
    pub fn tool_schemas(&self) -> Vec<Tool> {
        self.tool_cache
            .iter()
            .flat_map(|(tools, _)| tools.iter().cloned())
            .collect()
    }

    /// Get cached tool prompts for all running MCP servers.
    pub fn tool_prompts(&self) -> Vec<ToolPrompt> {
        self.tool_cache
            .iter()
            .flat_map(|(_, prompts)| prompts.iter().cloned())
            .collect()
    }

    /// Check whether a tool name is an MCP tool.
    pub fn has_tool(&self, name: &str) -> bool {
        name.starts_with("mcp_")
            && self
                .tool_cache
                .iter()
                .any(|(tools, _)| tools.iter().any(|t| t.name == name))
    }

    /// Execute an MCP tool call.
    pub fn execute_tool(
        &self,
        tool_call: &ToolCall,
        on_output: &mut dyn FnMut(&str),
    ) -> crate::Result<ToolResultMessage> {
        let start = std::time::Instant::now();

        // Parse prefixed name: mcp_{server}_{tool}
        let unprefixed = tool_call
            .name
            .strip_prefix("mcp_")
            .ok_or_else(|| crate::Error::Io(format!("not an MCP tool: {}", tool_call.name)))?;

        // Find the server by checking which server has a tool that matches
        let (server_name, mcp_tool_name) = self
            .find_server_for_tool(unprefixed)
            .ok_or_else(|| {
                crate::Error::Io(format!("no MCP server provides tool '{}'", tool_call.name))
            })?;

        let handle = self.servers.get(&server_name).ok_or_else(|| {
            crate::Error::Io(format!("MCP server '{}' not found", server_name))
        })?;

        if handle.status != McpServerStatus::Running {
            return Ok(ToolResultMessage {
                tool_call_id: tool_call.id.clone(),
                tool_name: tool_call.name.clone(),
                content: vec![ToolResultContent::Text(TextContent {
                    text: format!("MCP server '{}' is not running: {:?}", server_name, handle.status),
                    text_signature: None,
                })],
                details: None,
                is_error: true,
                timestamp: tau_agent_base::types::timestamp_ms(),
                duration_ms: Some(start.elapsed().as_millis() as u64),
                summary: None,
                post_persist_actions: Vec::new(),
            });
        }

        let client = handle.client.as_ref().ok_or_else(|| {
            crate::Error::Io(format!("MCP server '{}' has no active client", server_name))
        })?;

        on_output(&format!("Calling MCP tool {mcp_tool_name} on {server_name}..."));

        let timeout = Duration::from_secs(handle.config.timeout_secs);
        let arguments = tool_call
            .arguments
            .as_object()
            .cloned()
            .unwrap_or_default();

        let rt_handle = self.rt.handle();
        let result = rt_handle.block_on(async {
            let params = rmcp::model::CallToolRequestParams::new(mcp_tool_name.clone())
                .with_arguments(arguments);
            tokio::time::timeout(timeout, client.call_tool(params)).await
        });

        let duration_ms = Some(start.elapsed().as_millis() as u64);

        match result {
            Ok(Ok(call_result)) => {
                let is_error = call_result.is_error == Some(true);
                let mut content = Vec::new();
                for c in call_result.content {
                    match c.raw {
                        rmcp::model::RawContent::Text(tc) => {
                            content.push(ToolResultContent::Text(TextContent {
                                text: tc.text,
                                text_signature: None,
                            }));
                        }
                        rmcp::model::RawContent::Image(ic) => {
                            content.push(ToolResultContent::Image(ImageContent {
                                data: ic.data,
                                mime_type: ic.mime_type,
                            }));
                        }
                        _ => {
                            content.push(ToolResultContent::Text(TextContent {
                                text: "(unsupported MCP content type)".to_string(),
                                text_signature: None,
                            }));
                        }
                    }
                }
                if content.is_empty() {
                    content.push(ToolResultContent::Text(TextContent {
                        text: "(empty result)".to_string(),
                        text_signature: None,
                    }));
                }
                Ok(ToolResultMessage {
                    tool_call_id: tool_call.id.clone(),
                    tool_name: tool_call.name.clone(),
                    content,
                    details: None,
                    is_error,
                    timestamp: tau_agent_base::types::timestamp_ms(),
                    duration_ms,
                    summary: None,
                    post_persist_actions: Vec::new(),
                })
            }
            Ok(Err(e)) => Ok(ToolResultMessage {
                tool_call_id: tool_call.id.clone(),
                tool_name: tool_call.name.clone(),
                content: vec![ToolResultContent::Text(TextContent {
                    text: format!("MCP tool call error: {e}"),
                    text_signature: None,
                })],
                details: None,
                is_error: true,
                timestamp: tau_agent_base::types::timestamp_ms(),
                duration_ms,
                summary: None,
                post_persist_actions: Vec::new(),
            }),
            Err(_) => Ok(ToolResultMessage {
                tool_call_id: tool_call.id.clone(),
                tool_name: tool_call.name.clone(),
                content: vec![ToolResultContent::Text(TextContent {
                    text: format!(
                        "MCP tool call timed out after {}s",
                        handle.config.timeout_secs
                    ),
                    text_signature: None,
                })],
                details: None,
                is_error: true,
                timestamp: tau_agent_base::types::timestamp_ms(),
                duration_ms,
                summary: None,
                post_persist_actions: Vec::new(),
            }),
        }
    }

    /// Find which server provides a tool, given the unprefixed name (after "mcp_").
    ///
    /// The unprefixed name is `{server}_{tool}`. We try matching each known
    /// server name as a prefix, then check whether the remainder is a real tool.
    fn find_server_for_tool(&self, unprefixed: &str) -> Option<(String, String)> {
        for (server_name, handle) in &self.servers {
            let prefix = format!("{server_name}_");
            if let Some(tool_name) = unprefixed.strip_prefix(&prefix) {
                if handle
                    .cached_tools
                    .iter()
                    .any(|t| t.name == tool_name)
                {
                    return Some((server_name.clone(), tool_name.to_string()));
                }
            }
        }
        None
    }

    /// List resources from a specific MCP server.
    pub fn list_resources(
        &self,
        server_name: &str,
    ) -> crate::Result<Vec<(String, String, Option<String>)>> {
        let handle = self.servers.get(server_name).ok_or_else(|| {
            crate::Error::Io(format!("MCP server '{}' not found", server_name))
        })?;
        let client = handle.client.as_ref().ok_or_else(|| {
            crate::Error::Io(format!("MCP server '{}' not running", server_name))
        })?;

        let rt_handle = self.rt.handle();
        let resources = rt_handle
            .block_on(async { client.list_all_resources().await })
            .map_err(|e| crate::Error::Io(format!("failed to list resources: {e}")))?;

        Ok(resources
            .into_iter()
            .filter(|r| handle.config.resources.allows(&r.uri))
            .map(|r| {
                (
                    r.uri.clone(),
                    r.name.clone(),
                    r.description.as_ref().map(|d| d.to_string()),
                )
            })
            .collect())
    }

    /// Read a resource from a specific MCP server.
    pub fn read_resource(
        &self,
        server_name: &str,
        uri: &str,
    ) -> crate::Result<String> {
        let handle = self.servers.get(server_name).ok_or_else(|| {
            crate::Error::Io(format!("MCP server '{}' not found", server_name))
        })?;
        let client = handle.client.as_ref().ok_or_else(|| {
            crate::Error::Io(format!("MCP server '{}' not running", server_name))
        })?;

        let rt_handle = self.rt.handle();
        let result = rt_handle
            .block_on(async {
                client
                    .read_resource(rmcp::model::ReadResourceRequestParams::new(uri))
                    .await
            })
            .map_err(|e| crate::Error::Io(format!("failed to read resource: {e}")))?;

        let mut text = String::new();
        for content in result.contents {
            match content {
                rmcp::model::ResourceContents::TextResourceContents { text: t, .. } => {
                    text.push_str(&t);
                }
                rmcp::model::ResourceContents::BlobResourceContents { blob, .. } => {
                    text.push_str(&format!("(binary blob: {} bytes)", blob.len()));
                }
            }
        }
        Ok(text)
    }

    /// Get status information for all servers.
    pub fn server_statuses(&self) -> Vec<(String, McpServerStatus, usize)> {
        self.servers
            .iter()
            .map(|(name, handle)| {
                (name.clone(), handle.status.clone(), handle.cached_tools.len())
            })
            .collect()
    }

    /// Reload configuration and reconnect servers.
    pub fn reload(
        &mut self,
        project_name: Option<&str>,
        project_path: Option<&str>,
    ) {
        tracing::info!("reloading MCP configuration");
        let config = load_mcp_config(project_name, project_path);
        self.connect_servers(config);
    }

    /// Stop all MCP servers.
    pub fn stop_all(&mut self) {
        for (name, handle) in self.servers.drain() {
            if let Some(client) = handle.client {
                let rt_handle = self.rt.handle();
                if let Err(e) = rt_handle.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), client.cancel()).await
                }) {
                    tracing::warn!(server = %name, %e, "MCP server shutdown timed out");
                }
            }
        }
        self.tool_cache.clear();
    }

    /// Check if any MCP servers are configured and running.
    pub fn has_servers(&self) -> bool {
        self.servers
            .values()
            .any(|h| h.status == McpServerStatus::Running)
    }
}

impl Drop for McpManager {
    fn drop(&mut self) {
        self.stop_all();
    }
}
