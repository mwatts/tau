//! Skill types and frontmatter parsing.
//!
//! A skill is a markdown file with YAML frontmatter that injects domain
//! knowledge, workflows, and constraints into the system prompt.

use serde::Deserialize;
use std::path::PathBuf;

/// Where a skill was loaded from, in priority order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SkillSource {
    /// Built into the tau binary (lowest priority).
    Builtin,
    /// `~/.config/tau/skills/`
    Global,
    /// `{project}/.agents/skills/`
    ProjectAgents,
    /// `{project}/.tau/skills/`
    ProjectTau,
    /// `~/.config/tau/projects/{name}/skills/`
    Operator,
}

/// Trigger conditions for automatic skill activation.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SkillTriggers {
    /// Slash commands that activate this skill (e.g., "/commit").
    #[serde(default)]
    pub commands: Vec<String>,
    /// Keywords in user message that activate (case-insensitive, whole-word).
    #[serde(default)]
    pub keywords: Vec<String>,
    /// File glob patterns that activate when matching files are in context.
    #[serde(default)]
    pub file_globs: Vec<String>,
}

impl SkillTriggers {
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty() && self.keywords.is_empty() && self.file_globs.is_empty()
    }
}

/// YAML frontmatter of a skill file.
#[derive(Debug, Clone, Deserialize)]
struct SkillFrontmatter {
    name: String,
    description: String,
    #[serde(default)]
    triggers: SkillTriggers,
    #[serde(default = "default_priority")]
    priority: u8,
}

fn default_priority() -> u8 {
    50
}

/// A parsed skill ready for matching and injection.
#[derive(Debug, Clone)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub triggers: SkillTriggers,
    /// 0-100, higher = injected earlier.
    pub priority: u8,
    /// Markdown body (everything after frontmatter).
    pub body: String,
    pub source: SkillSource,
    /// Path the skill was loaded from (None for builtins).
    pub path: Option<PathBuf>,
}

impl Skill {
    /// Estimated token count (chars / 4).
    pub fn token_estimate(&self) -> usize {
        self.body.len() / 4
    }
}

/// Parse a markdown file with YAML frontmatter into a Skill.
///
/// Expected format:
/// ```text
/// ---
/// name: skill-name
/// description: One-line description
/// triggers:
///   keywords: [...]
/// priority: 50
/// ---
///
/// Body content here...
/// ```
pub fn parse_skill(content: &str, source: SkillSource, path: Option<PathBuf>) -> Option<Skill> {
    let content = content.trim_start();
    if !content.starts_with("---") {
        return None;
    }

    let after_first_fence = &content[3..];
    let end_fence = after_first_fence.find("\n---")?;
    let yaml_block = &after_first_fence[..end_fence];
    let body_start = 3 + end_fence + 4; // "---" + fence + "\n---"
    let body = if body_start < content.len() {
        content[body_start..].trim().to_string()
    } else {
        String::new()
    };

    let fm: SkillFrontmatter = serde_yaml::from_str(yaml_block).ok()?;

    Some(Skill {
        name: fm.name,
        description: fm.description,
        triggers: fm.triggers,
        priority: fm.priority.min(100),
        body,
        source,
        path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"---
name: conventional-commits
description: Enforce conventional commit format
triggers:
  keywords: ["commit", "merge"]
priority: 80
---

## Rules

- Use format: `type(scope): message`
"#;

    #[test]
    fn parse_valid_skill() {
        let skill = parse_skill(SAMPLE, SkillSource::Global, None).unwrap();
        assert_eq!(skill.name, "conventional-commits");
        assert_eq!(skill.description, "Enforce conventional commit format");
        assert_eq!(skill.priority, 80);
        assert_eq!(skill.triggers.keywords, vec!["commit", "merge"]);
        assert!(skill.triggers.commands.is_empty());
        assert!(skill.body.contains("## Rules"));
        assert!(skill.body.contains("type(scope): message"));
    }

    #[test]
    fn parse_no_frontmatter() {
        assert!(parse_skill("# Just markdown", SkillSource::Global, None).is_none());
    }

    #[test]
    fn parse_no_closing_fence() {
        let bad = "---\nname: x\n# no closing fence\nbody";
        assert!(parse_skill(bad, SkillSource::Global, None).is_none());
    }

    #[test]
    fn parse_minimal_frontmatter() {
        let minimal = "---\nname: foo\ndescription: bar\n---\nbody here";
        let skill = parse_skill(minimal, SkillSource::Builtin, None).unwrap();
        assert_eq!(skill.name, "foo");
        assert_eq!(skill.priority, 50); // default
        assert!(skill.triggers.is_empty());
        assert_eq!(skill.body, "body here");
    }

    #[test]
    fn priority_capped_at_100() {
        let content = "---\nname: x\ndescription: y\npriority: 200\n---\n";
        let skill = parse_skill(content, SkillSource::Global, None).unwrap();
        assert_eq!(skill.priority, 100);
    }

    #[test]
    fn token_estimate() {
        let skill = parse_skill(SAMPLE, SkillSource::Global, None).unwrap();
        assert_eq!(skill.token_estimate(), skill.body.len() / 4);
    }

    #[test]
    fn source_ordering() {
        assert!(SkillSource::Builtin < SkillSource::Global);
        assert!(SkillSource::Global < SkillSource::ProjectAgents);
        assert!(SkillSource::ProjectAgents < SkillSource::ProjectTau);
        assert!(SkillSource::ProjectTau < SkillSource::Operator);
    }
}
