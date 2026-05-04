use std::collections::HashMap;
use std::path::Path;

use tau_agent_base::tool_prompt::ToolPrompt;

/// Parse a tool_prompts.toml string into a map of tool_name → guidelines.
pub fn parse_tool_prompts_toml(content: &str) -> Result<HashMap<String, Vec<String>>, String> {
    let table: toml::Table = content.parse().map_err(|e| format!("parse error: {}", e))?;
    let mut result = HashMap::new();
    for (tool_name, value) in table {
        let section = value
            .as_table()
            .ok_or_else(|| format!("[{}] must be a table", tool_name))?;
        let guidelines = section
            .get("guidelines")
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("[{}].guidelines must be an array", tool_name))?;
        let strings: Vec<String> = guidelines
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        result.insert(tool_name, strings);
    }
    Ok(result)
}

/// Load overrides from disk. Resolution: global → project (project wins).
pub fn load_overrides(project_path: Option<&str>) -> HashMap<String, Vec<String>> {
    let mut merged = HashMap::new();

    // Global overrides
    let global_path = tau_agent_base::paths::config_dir().join("tool_prompts.toml");
    if let Some(content) = read_file(&global_path) {
        if let Ok(overrides) = parse_tool_prompts_toml(&content) {
            merged.extend(overrides);
        }
    }

    // Project overrides (replace per-tool, not merge)
    if let Some(project) = project_path {
        let project_file = Path::new(project).join(".tau").join("tool_prompts.toml");
        if let Some(content) = read_file(&project_file) {
            if let Ok(overrides) = parse_tool_prompts_toml(&content) {
                merged.extend(overrides);
            }
        }
    }

    merged
}

/// Apply overrides to a set of tool prompts. Per-tool replacement (not merge).
pub fn apply_overrides(
    mut tools: Vec<ToolPrompt>,
    overrides: &HashMap<String, Vec<String>>,
) -> Vec<ToolPrompt> {
    for tool in &mut tools {
        if let Some(new_guidelines) = overrides.get(&tool.name) {
            tool.guidelines = new_guidelines.clone();
        }
    }
    tools
}

/// Write overrides for a specific tool to the project's tool_prompts.toml.
pub fn write_tool_override(project_path: &str, tool_name: &str, guidelines: &[String]) -> std::io::Result<()> {
    let toml_path = Path::new(project_path).join(".tau").join("tool_prompts.toml");
    let mut table: toml::Table = if let Some(content) = read_file(&toml_path) {
        content.parse().unwrap_or_default()
    } else {
        toml::Table::new()
    };

    let mut tool_table = toml::Table::new();
    let arr: Vec<toml::Value> = guidelines.iter().map(|s| toml::Value::String(s.clone())).collect();
    tool_table.insert("guidelines".into(), toml::Value::Array(arr));
    table.insert(tool_name.into(), toml::Value::Table(tool_table));

    std::fs::create_dir_all(toml_path.parent().unwrap())?;
    std::fs::write(&toml_path, toml::to_string_pretty(&table).unwrap_or_default())
}

fn read_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_toml_overrides() {
        let toml_str = r#"
[bash]
guidelines = ["Always use set -e", "Quote all variables"]

[edit]
guidelines = ["Provide 3 lines of context"]
"#;
        let overrides = parse_tool_prompts_toml(toml_str).unwrap();
        assert_eq!(overrides.len(), 2);
        assert_eq!(overrides["bash"].len(), 2);
        assert_eq!(overrides["bash"][0], "Always use set -e");
        assert_eq!(overrides["edit"].len(), 1);
    }

    #[test]
    fn merge_overrides_replaces_per_tool() {
        let defaults = vec![
            ToolPrompt {
                name: "bash".into(),
                snippet: "Run shell commands".into(),
                guidelines: vec!["default guideline".into()],
            },
            ToolPrompt {
                name: "read".into(),
                snippet: "Read files".into(),
                guidelines: vec!["read guideline".into()],
            },
        ];
        let mut overrides = HashMap::new();
        overrides.insert("bash".to_string(), vec!["override guideline".to_string()]);

        let merged = apply_overrides(defaults, &overrides);
        let bash = merged.iter().find(|t| t.name == "bash").unwrap();
        assert_eq!(bash.guidelines, vec!["override guideline"]);
        let read = merged.iter().find(|t| t.name == "read").unwrap();
        assert_eq!(read.guidelines, vec!["read guideline"]);
    }
}
