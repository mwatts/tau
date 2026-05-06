//! TOML config structs for defining background agents in `agents.toml`.
//!
//! Load via [`load_agents_config`], which resolves the file through the
//! standard operator → project → global config chain.

use serde::Deserialize;

/// Top-level container for an `agents.toml` file.
///
/// ```toml
/// [[agent]]
/// name = "code-reviewer"
/// prompt = "Review recent PRs and leave comments"
/// trigger_type = "periodic"
/// trigger_config = "300"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct AgentsConfig {
    #[serde(default)]
    pub agent: Vec<AgentDef>,
}

/// A single agent definition from `agents.toml`.
///
/// Only user-configurable fields are present here; runtime fields such as
/// `id`, `spent_usd`, `created_at`, and `session_id` are managed by the
/// daemon and are not part of the config file format.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentDef {
    pub name: String,
    pub prompt: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_trigger_type")]
    pub trigger_type: String,
    #[serde(default)]
    pub trigger_config: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub project_name: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub budget_usd: Option<f64>,
}

fn default_trigger_type() -> String {
    "persistent".to_string()
}

fn default_enabled() -> bool {
    true
}

/// Load `agents.toml` from the highest-priority config tier available.
///
/// Returns an empty [`AgentsConfig`] (no agents) when no file is found.
pub fn load_agents_config(
    project_name: Option<&str>,
    project_path: Option<&str>,
) -> AgentsConfig {
    crate::config_chain::load_first::<AgentsConfig>(
        project_name,
        project_path,
        "agents.toml",
        true, // allow project tier
    )
    .unwrap_or_else(|| AgentsConfig { agent: Vec::new() })
}
