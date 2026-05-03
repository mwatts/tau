//! MCP server configuration types.
//!
//! Loaded from `mcp.toml` via the config chain with `allow_project_tier = false`
//! (security-sensitive: MCP servers execute arbitrary code).

use std::collections::HashMap;

use serde::Deserialize;

/// Top-level `mcp.toml` structure.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: HashMap<String, McpServerConfig>,
}

/// Configuration for a single MCP server.
#[derive(Debug, Clone, Deserialize)]
pub struct McpServerConfig {
    /// Transport type. Defaults to `"stdio"` when `command` is set.
    #[serde(default)]
    pub transport: McpTransport,

    // --- stdio transport fields ---
    /// Command to spawn (first element is the executable).
    #[serde(default)]
    pub command: Option<String>,

    /// Arguments to the command.
    #[serde(default)]
    pub args: Vec<String>,

    /// Extra environment variables for the subprocess.
    #[serde(default)]
    pub env: HashMap<String, String>,

    // --- HTTP+SSE transport fields ---
    /// URL for HTTP+SSE transport.
    #[serde(default)]
    pub url: Option<String>,

    /// HTTP headers (e.g. Authorization).
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Whether this server is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Per-tool-call timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Tool filtering.
    #[serde(default)]
    pub tools: ToolFilter,

    /// Resource filtering.
    #[serde(default)]
    pub resources: ResourceFilter,
}

/// Transport selection for an MCP server.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpTransport {
    #[default]
    Stdio,
    HttpSse,
}

/// Tool include/exclude filter.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ToolFilter {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Resource include/exclude filter.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResourceFilter {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl ToolFilter {
    /// Check whether a tool name passes the filter.
    pub fn allows(&self, name: &str) -> bool {
        if !self.include.is_empty() && !self.include.iter().any(|i| i == name) {
            return false;
        }
        if self.exclude.iter().any(|e| e == name) {
            return false;
        }
        true
    }
}

impl ResourceFilter {
    /// Check whether a resource URI passes the filter.
    pub fn allows(&self, uri: &str) -> bool {
        if !self.include.is_empty() && !self.include.iter().any(|i| i == uri) {
            return false;
        }
        if self.exclude.iter().any(|e| e == uri) {
            return false;
        }
        true
    }
}

fn default_true() -> bool {
    true
}

fn default_timeout() -> u64 {
    120
}

/// Load MCP configuration from the config chain.
///
/// Security: `allow_project_tier = false` — project `.tau/mcp.toml` is
/// skipped. Only operator and global tiers are loaded.
pub fn load_mcp_config(
    project_name: Option<&str>,
    project_path: Option<&str>,
) -> McpConfig {
    crate::config_chain::load_first::<McpConfig>(
        project_name,
        project_path,
        "mcp.toml",
        false, // security-sensitive: no project tier
    )
    .unwrap_or_default()
}

/// Expand `${VAR_NAME}` references in a string from the process environment.
///
/// Returns an error message if a referenced variable is not set.
pub fn expand_env_vars(s: &str) -> Result<String, String> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            let mut default_val = None;
            let mut found_close = false;
            while let Some(c) = chars.next() {
                if c == '}' {
                    found_close = true;
                    break;
                }
                if c == ':' && chars.peek() == Some(&'-') {
                    chars.next(); // consume '-'
                    let mut def = String::new();
                    for c2 in chars.by_ref() {
                        if c2 == '}' {
                            found_close = true;
                            break;
                        }
                        def.push(c2);
                    }
                    default_val = Some(def);
                    break;
                }
                var_name.push(c);
            }
            if !found_close {
                result.push_str("${");
                result.push_str(&var_name);
                continue;
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => match default_val {
                    Some(def) => result.push_str(&def),
                    None => return Err(format!("environment variable '{}' is not set", var_name)),
                },
            }
        } else {
            result.push(ch);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let toml_str = r#"
[servers.test]
command = "echo"
args = ["hello"]
"#;
        let cfg: McpConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.servers.contains_key("test"));
        let srv = &cfg.servers["test"];
        assert_eq!(srv.command.as_deref(), Some("echo"));
        assert_eq!(srv.args, vec!["hello"]);
        assert!(srv.enabled);
        assert_eq!(srv.timeout_secs, 120);
    }

    #[test]
    fn parse_http_transport() {
        let toml_str = r#"
[servers.remote]
transport = "http-sse"
url = "https://example.com/mcp"
[servers.remote.headers]
Authorization = "Bearer token123"
"#;
        let cfg: McpConfig = toml::from_str(toml_str).unwrap();
        let srv = &cfg.servers["remote"];
        assert!(matches!(srv.transport, McpTransport::HttpSse));
        assert_eq!(srv.url.as_deref(), Some("https://example.com/mcp"));
        assert_eq!(srv.headers.get("Authorization").unwrap(), "Bearer token123");
    }

    #[test]
    fn tool_filter_allows() {
        let f = ToolFilter {
            include: vec!["read".into(), "write".into()],
            exclude: vec![],
        };
        assert!(f.allows("read"));
        assert!(!f.allows("delete"));

        let f2 = ToolFilter {
            include: vec![],
            exclude: vec!["dangerous".into()],
        };
        assert!(f2.allows("safe"));
        assert!(!f2.allows("dangerous"));
    }

    #[test]
    fn expand_env_vars_basic() {
        unsafe { std::env::set_var("TAU_TEST_VAR_1", "hello") };
        assert_eq!(expand_env_vars("${TAU_TEST_VAR_1} world").unwrap(), "hello world");
        unsafe { std::env::remove_var("TAU_TEST_VAR_1") };
    }

    #[test]
    fn expand_env_vars_default() {
        unsafe { std::env::remove_var("TAU_TEST_MISSING") };
        assert_eq!(expand_env_vars("${TAU_TEST_MISSING:-fallback}").unwrap(), "fallback");
    }

    #[test]
    fn expand_env_vars_missing_errors() {
        unsafe { std::env::remove_var("TAU_TEST_MISSING2") };
        assert!(expand_env_vars("${TAU_TEST_MISSING2}").is_err());
    }

    #[test]
    fn disabled_server() {
        let toml_str = r#"
[servers.off]
command = "echo"
enabled = false
"#;
        let cfg: McpConfig = toml::from_str(toml_str).unwrap();
        assert!(!cfg.servers["off"].enabled);
    }
}
