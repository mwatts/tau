//! Agent-writable persistent memory (SI-3).
//!
//! Memory files are injected into the system prompt at session start.
//! The agent can add, replace, or remove entries mid-session; changes
//! take effect next session.

use std::path::{Path, PathBuf};

const PROJECT_MEMORY_CAP: usize = 2200;
const GLOBAL_MEMORY_CAP: usize = 1400;

/// Resolve path for project memory.
pub fn project_memory_path(project_path: &str) -> PathBuf {
    Path::new(project_path).join(".tau").join("memory.md")
}

/// Resolve path for global memory.
pub fn global_memory_path() -> PathBuf {
    tau_agent_base::paths::config_dir().join("memory.md")
}

/// Load memory content from a file, returning empty string if missing.
pub fn load(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Load both project and global memory for prompt injection.
pub fn load_for_prompt(project_path: Option<&str>) -> String {
    let mut parts = Vec::new();

    if let Some(pp) = project_path {
        let content = load(&project_memory_path(pp));
        if !content.trim().is_empty() {
            parts.push(format!("<project-memory>\n{}\n</project-memory>", content.trim()));
        }
    }

    let global = load(&global_memory_path());
    if !global.trim().is_empty() {
        parts.push(format!("<global-memory>\n{}\n</global-memory>", global.trim()));
    }

    if parts.is_empty() {
        return String::new();
    }

    parts.join("\n\n")
}

/// Execute a memory action. Returns a status message for the tool result.
pub fn execute(action: &str, scope: &str, content: Option<&str>, project_path: &str) -> String {
    let (path, cap) = match scope {
        "global" => (global_memory_path(), GLOBAL_MEMORY_CAP),
        _ => (project_memory_path(project_path), PROJECT_MEMORY_CAP),
    };

    match action {
        "list" => {
            let current = load(&path);
            if current.trim().is_empty() {
                format!("[{scope} memory is empty]")
            } else {
                format!(
                    "[{scope} memory ({}/{} chars)]\n{}",
                    current.len(),
                    cap,
                    current
                )
            }
        }
        "add" => {
            let text = match content {
                Some(t) if !t.trim().is_empty() => t.trim(),
                _ => return "error: 'content' required for add".into(),
            };
            let mut current = load(&path);
            if !current.is_empty() && !current.ends_with('\n') {
                current.push('\n');
            }
            current.push_str(text);
            current.push('\n');

            if current.len() > cap {
                return format!(
                    "error: would exceed {scope} memory cap ({} + {} > {} chars). Remove stale entries first.",
                    current.len() - text.len() - 1,
                    text.len(),
                    cap
                );
            }

            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&path, &current) {
                Ok(()) => format!("added to {scope} memory ({}/{} chars)", current.len(), cap),
                Err(e) => format!("error writing memory: {}", e),
            }
        }
        "replace" => {
            let text = match content {
                Some(t) => t,
                None => return "error: 'content' required for replace".into(),
            };
            if text.len() > cap {
                return format!(
                    "error: content exceeds {scope} memory cap ({} > {} chars)",
                    text.len(),
                    cap
                );
            }
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&path, text) {
                Ok(()) => format!("replaced {scope} memory ({}/{} chars)", text.len(), cap),
                Err(e) => format!("error writing memory: {}", e),
            }
        }
        "remove" => {
            let text = match content {
                Some(t) if !t.trim().is_empty() => t.trim(),
                _ => return "error: 'content' required for remove (exact line match)".into(),
            };
            let current = load(&path);
            let lines: Vec<&str> = current.lines().collect();
            let new_lines: Vec<&str> = lines.into_iter().filter(|l| l.trim() != text).collect();
            let new_content = new_lines.join("\n") + "\n";

            if new_content.trim() == current.trim() {
                return format!("no matching line found in {scope} memory");
            }

            let _ = std::fs::write(&path, &new_content);
            format!(
                "removed from {scope} memory ({}/{} chars)",
                new_content.trim().len(),
                cap
            )
        }
        other => format!("error: unknown action '{other}'. Use: add, replace, remove, list"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn add_and_list() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().to_str().unwrap();

        let result = execute("list", "project", None, project);
        assert!(result.contains("empty"));

        let result = execute("add", "project", Some("user prefers tabs"), project);
        assert!(result.contains("added"));

        let result = execute("list", "project", None, project);
        assert!(result.contains("user prefers tabs"));
    }

    #[test]
    fn remove() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().to_str().unwrap();

        execute("add", "project", Some("fact one"), project);
        execute("add", "project", Some("fact two"), project);

        let result = execute("remove", "project", Some("fact one"), project);
        assert!(result.contains("removed"));

        let result = execute("list", "project", None, project);
        assert!(!result.contains("fact one"));
        assert!(result.contains("fact two"));
    }

    #[test]
    fn cap_enforcement() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().to_str().unwrap();

        let big = "x".repeat(PROJECT_MEMORY_CAP + 1);
        let result = execute("add", "project", Some(&big), project);
        assert!(result.contains("error"));
        assert!(result.contains("cap"));
    }

    #[test]
    fn replace() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().to_str().unwrap();

        execute("add", "project", Some("old content"), project);
        let result = execute("replace", "project", Some("new content"), project);
        assert!(result.contains("replaced"));

        let result = execute("list", "project", None, project);
        assert!(result.contains("new content"));
        assert!(!result.contains("old content"));
    }

    #[test]
    fn load_for_prompt_combines() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().to_str().unwrap();

        execute("add", "project", Some("project fact"), project);

        let prompt = load_for_prompt(Some(project));
        assert!(prompt.contains("<project-memory>"));
        assert!(prompt.contains("project fact"));
    }
}
