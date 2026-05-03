//! SI-4: Meta-cognitive skill patching.
//!
//! Lets the agent read, patch, or delete skill files mid-session.
//! Auto-generated skills (in `auto/` subdirectory) are freely patchable;
//! hand-written skills produce a warning but are still editable.

use std::path::Path;

use crate::skills;
use tau_agent_base::skills::SkillSource;

/// Execute a skill_patch action. Returns a status message for the tool result.
pub fn execute(action: &str, name: Option<&str>, content: Option<&str>, project_path: &str) -> String {
    match action {
        "list" => list_skills(project_path),
        "read" => {
            let name = match name {
                Some(n) if !n.is_empty() => n,
                _ => return "error: 'name' required for read".into(),
            };
            read_skill(name, project_path)
        }
        "patch" => {
            let name = match name {
                Some(n) if !n.is_empty() => n,
                _ => return "error: 'name' required for patch".into(),
            };
            let content = match content {
                Some(c) if !c.is_empty() => c,
                _ => return "error: 'content' required for patch (full file content including frontmatter)".into(),
            };
            patch_skill(name, content, project_path)
        }
        "delete" => {
            let name = match name {
                Some(n) if !n.is_empty() => n,
                _ => return "error: 'name' required for delete".into(),
            };
            delete_skill(name, project_path)
        }
        other => format!("error: unknown action '{other}'. Use: list, read, patch, delete"),
    }
}

fn list_skills(project_path: &str) -> String {
    let all = skills::discover(Some(project_path), None);
    if all.is_empty() {
        return "[no skills found]".into();
    }

    let mut lines = Vec::new();
    lines.push(format!("[{} skills]", all.len()));
    for skill in &all {
        let source_label = match &skill.source {
            SkillSource::Builtin => "builtin",
            SkillSource::Global => "global",
            SkillSource::ProjectAgents => ".agents/skills",
            SkillSource::ProjectTau => ".tau/skills",
            SkillSource::Operator => "operator",
        };
        let auto = skill.path.as_ref().map_or(false, |p| is_auto_skill(p));
        let marker = if auto { " [auto]" } else { "" };
        lines.push(format!("  {} ({}){}",  skill.name, source_label, marker));
    }
    lines.join("\n")
}

fn read_skill(name: &str, project_path: &str) -> String {
    let all = skills::discover(Some(project_path), None);
    let skill = match all.iter().find(|s| s.name == name) {
        Some(s) => s,
        None => return format!("error: skill '{}' not found", name),
    };

    match &skill.path {
        Some(p) => match std::fs::read_to_string(p) {
            Ok(content) => {
                let auto = is_auto_skill(p);
                format!(
                    "[skill: {} | source: {:?} | patchable: {}]\n{}",
                    skill.name,
                    skill.source,
                    if auto { "yes" } else { "yes (hand-written, be careful)" },
                    content,
                )
            }
            Err(e) => format!("error reading skill file: {}", e),
        },
        None => format!(
            "[skill: {} | source: builtin | patchable: no]\n\n{}",
            skill.name, skill.body
        ),
    }
}

fn patch_skill(name: &str, content: &str, project_path: &str) -> String {
    let all = skills::discover(Some(project_path), None);
    let skill = match all.iter().find(|s| s.name == name) {
        Some(s) => s,
        None => return format!("error: skill '{}' not found", name),
    };

    let path = match &skill.path {
        Some(p) => p.clone(),
        None => return "error: builtin skills cannot be patched".into(),
    };

    // Validate the new content has frontmatter
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return "error: content must include YAML frontmatter (start with ---)".into();
    }

    let auto = is_auto_skill(&path);
    let warning = if !auto {
        " (warning: this is a hand-written skill — consider editing the source file directly)"
    } else {
        ""
    };

    match std::fs::write(&path, content) {
        Ok(()) => format!(
            "patched skill '{}' ({} chars){}",
            name,
            content.len(),
            warning
        ),
        Err(e) => format!("error writing skill: {}", e),
    }
}

fn delete_skill(name: &str, project_path: &str) -> String {
    let all = skills::discover(Some(project_path), None);
    let skill = match all.iter().find(|s| s.name == name) {
        Some(s) => s,
        None => return format!("error: skill '{}' not found", name),
    };

    let path = match &skill.path {
        Some(p) => p.clone(),
        None => return "error: builtin skills cannot be deleted".into(),
    };

    if !is_auto_skill(&path) {
        return format!(
            "error: refusing to delete hand-written skill '{}'. Only auto-generated skills (.tau/skills/auto/) can be deleted via this tool.",
            name
        );
    }

    match std::fs::remove_file(&path) {
        Ok(()) => format!("deleted auto-generated skill '{}'", name),
        Err(e) => format!("error deleting skill: {}", e),
    }
}

/// Check if a skill path is in an `auto/` subdirectory.
fn is_auto_skill(path: &Path) -> bool {
    path.components().any(|c| c.as_os_str() == "auto")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_skills(tmp: &TempDir) -> String {
        let project = tmp.path().to_str().unwrap().to_string();
        let auto_dir = tmp.path().join(".tau").join("skills").join("auto");
        fs::create_dir_all(&auto_dir).unwrap();

        // Auto-generated skill
        fs::write(
            auto_dir.join("test-pattern.md"),
            "---\nname: test-pattern\ndescription: A test pattern\ntriggers:\n  keywords: [test]\n---\n\nUse pattern X when doing Y.\n",
        ).unwrap();

        // Hand-written skill
        let skills_dir = tmp.path().join(".tau").join("skills");
        fs::write(
            skills_dir.join("manual-skill.md"),
            "---\nname: manual-skill\ndescription: Hand-written\ntriggers:\n  keywords: [manual]\n---\n\nDo things manually.\n",
        ).unwrap();

        project
    }

    #[test]
    fn list_shows_skills() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let result = execute("list", None, None, &project);
        assert!(result.contains("test-pattern"));
        assert!(result.contains("[auto]"));
        assert!(result.contains("manual-skill"));
    }

    #[test]
    fn read_auto_skill() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let result = execute("read", Some("test-pattern"), None, &project);
        assert!(result.contains("patchable: yes"));
        assert!(result.contains("Use pattern X"));
    }

    #[test]
    fn read_hand_written_skill() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let result = execute("read", Some("manual-skill"), None, &project);
        assert!(result.contains("hand-written"));
    }

    #[test]
    fn patch_auto_skill() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let new_content = "---\nname: test-pattern\ndescription: Updated\ntriggers:\n  keywords: [test]\n---\n\nNew content here.\n";
        let result = execute("patch", Some("test-pattern"), Some(new_content), &project);
        assert!(result.contains("patched"));
        assert!(!result.contains("warning"));

        // Verify content changed
        let read_result = execute("read", Some("test-pattern"), None, &project);
        assert!(read_result.contains("New content here"));
    }

    #[test]
    fn patch_hand_written_warns() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let new_content = "---\nname: manual-skill\ndescription: Updated\ntriggers:\n  keywords: [manual]\n---\n\nUpdated.\n";
        let result = execute("patch", Some("manual-skill"), Some(new_content), &project);
        assert!(result.contains("patched"));
        assert!(result.contains("warning"));
    }

    #[test]
    fn patch_requires_frontmatter() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let result = execute("patch", Some("test-pattern"), Some("no frontmatter"), &project);
        assert!(result.contains("error"));
        assert!(result.contains("frontmatter"));
    }

    #[test]
    fn delete_auto_skill() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let result = execute("delete", Some("test-pattern"), None, &project);
        assert!(result.contains("deleted"));

        let list = execute("list", None, None, &project);
        assert!(!list.contains("test-pattern"));
    }

    #[test]
    fn delete_hand_written_refused() {
        let tmp = TempDir::new().unwrap();
        let project = setup_skills(&tmp);
        let result = execute("delete", Some("manual-skill"), None, &project);
        assert!(result.contains("error"));
        assert!(result.contains("refusing"));
    }

    #[test]
    fn not_found() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().to_str().unwrap();
        let result = execute("read", Some("nonexistent"), None, project);
        assert!(result.contains("not found"));
    }
}
