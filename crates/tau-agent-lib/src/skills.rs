//! Skill discovery, matching, and prompt injection.
//!
//! Scans `.tau/skills/`, `.agents/skills/`, and `~/.config/tau/skills/`
//! for markdown skill files, matches them against session context, and
//! produces prompt text for injection.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use globset::{Glob, GlobSet, GlobSetBuilder};
use tau_agent_base::skills::{Skill, SkillSource, parse_skill};

/// Default budget for injected skill content (chars, ~4000 tokens).
const DEFAULT_BUDGET_CHARS: usize = 16000;

/// Context for skill matching decisions.
#[derive(Debug, Default)]
pub struct MatchContext {
    /// Current slash command (e.g., "/commit").
    pub command: Option<String>,
    /// User message text.
    pub user_message: Option<String>,
    /// Files currently in context (edited, read, mentioned).
    pub files: Vec<String>,
    /// Skills explicitly activated by the user via `/skill-name`.
    pub explicit: HashSet<String>,
}

/// Why a skill was activated — used for budget prioritization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ActivationReason {
    AlwaysOn,
    FileGlob,
    Keyword,
    Command,
    Explicit,
}

/// A skill selected for injection.
#[derive(Debug)]
struct SelectedSkill<'a> {
    skill: &'a Skill,
    reason: ActivationReason,
}

// ─── Discovery ──────────────────────────────────────────��────────────────────

/// Discover all skills from the filesystem, deduplicated by name.
///
/// Higher-priority sources win on name collision.
pub fn discover(project_path: Option<&str>, project_name: Option<&str>) -> Vec<Skill> {
    let mut by_name: HashMap<String, Skill> = HashMap::new();

    // Load in priority order (lowest first, so higher overwrites).
    // 1. Builtins
    for skill in builtin_skills() {
        by_name.insert(skill.name.clone(), skill);
    }

    // 2. Global: ~/.config/tau/skills/
    let global_dir = tau_agent_base::paths::config_dir().join("skills");
    load_from_dir(&global_dir, SkillSource::Global, &mut by_name);

    // 3. Project tier
    if let Some(project) = project_path {
        let project = Path::new(project);

        // .agents/skills/ (lower priority within project tier)
        let agents_dir = project.join(".agents").join("skills");
        load_from_dir(&agents_dir, SkillSource::ProjectAgents, &mut by_name);

        // .tau/skills/ (higher priority within project tier)
        let tau_dir = project.join(".tau").join("skills");
        load_from_dir(&tau_dir, SkillSource::ProjectTau, &mut by_name);
    }

    // 4. Operator: ~/.config/tau/projects/{name}/skills/
    if let Some(name) = project_name {
        let operator_dir = tau_agent_base::paths::project_config_dir(name).join("skills");
        load_from_dir(&operator_dir, SkillSource::Operator, &mut by_name);
    }

    by_name.into_values().collect()
}

fn load_from_dir(dir: &Path, source: SkillSource, out: &mut HashMap<String, Skill>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // Recurse into subdirectories.
            load_from_dir(&path, source.clone(), out);
        } else if path.extension().is_some_and(|e| e == "md") {
            if let Ok(content) = std::fs::read_to_string(&path) {
                if let Some(skill) = parse_skill(&content, source.clone(), Some(path)) {
                    out.insert(skill.name.clone(), skill);
                }
            }
        }
    }
}

// ─── Matching ───────────────────────────────────────────────────��────────────

/// Select skills that match the given context, respecting budget.
pub fn select<'a>(skills: &'a [Skill], ctx: &MatchContext) -> Vec<&'a Skill> {
    select_with_budget(skills, ctx, DEFAULT_BUDGET_CHARS)
}

/// Select with configurable budget (for testing).
pub fn select_with_budget<'a>(
    skills: &'a [Skill],
    ctx: &MatchContext,
    budget_chars: usize,
) -> Vec<&'a Skill> {
    let mut selected: Vec<SelectedSkill<'a>> = Vec::new();

    // Build glob set for file matching.
    let glob_sets: Vec<(&Skill, GlobSet)> = skills
        .iter()
        .filter(|s| !s.triggers.file_globs.is_empty())
        .filter_map(|s| {
            let mut builder = GlobSetBuilder::new();
            for pattern in &s.triggers.file_globs {
                if let Ok(glob) = Glob::new(pattern) {
                    builder.add(glob);
                }
            }
            builder.build().ok().map(|gs| (s, gs))
        })
        .collect();

    for skill in skills {
        let reason = match_skill(skill, ctx, &glob_sets);
        if let Some(reason) = reason {
            selected.push(SelectedSkill { skill, reason });
        }
    }

    // Sort: highest activation reason first, then highest priority.
    selected.sort_by(|a, b| {
        b.reason
            .cmp(&a.reason)
            .then(b.skill.priority.cmp(&a.skill.priority))
    });

    // Apply budget.
    let mut used = 0usize;
    let mut result: Vec<&Skill> = Vec::new();
    for s in &selected {
        let cost = s.skill.body.len();
        if used + cost > budget_chars && !result.is_empty() {
            tracing::debug!(
                skill = %s.skill.name,
                "skill dropped: budget exceeded ({used}/{budget_chars} chars used)"
            );
            continue;
        }
        used += cost;
        result.push(s.skill);
    }

    // Re-sort final result by priority (for prompt ordering).
    result.sort_by(|a, b| b.priority.cmp(&a.priority));
    result
}

fn match_skill<'a>(
    skill: &'a Skill,
    ctx: &MatchContext,
    glob_sets: &[(&'a Skill, GlobSet)],
) -> Option<ActivationReason> {
    // Explicit activation always wins.
    if ctx.explicit.contains(&skill.name) {
        return Some(ActivationReason::Explicit);
    }

    // Command trigger.
    if let Some(cmd) = &ctx.command {
        if skill.triggers.commands.iter().any(|c| c == cmd) {
            return Some(ActivationReason::Command);
        }
    }

    // Keyword trigger (whole-word, case-insensitive).
    if let Some(msg) = &ctx.user_message {
        let msg_lower = msg.to_lowercase();
        for kw in &skill.triggers.keywords {
            let kw_lower = kw.to_lowercase();
            if contains_whole_word(&msg_lower, &kw_lower) {
                return Some(ActivationReason::Keyword);
            }
        }
    }

    // File glob trigger.
    if !ctx.files.is_empty() {
        for (gs_skill, gs) in glob_sets {
            if std::ptr::eq(*gs_skill, skill) {
                for file in &ctx.files {
                    if gs.is_match(file) {
                        return Some(ActivationReason::FileGlob);
                    }
                }
            }
        }
    }

    // Always-on (empty triggers).
    if skill.triggers.is_empty() {
        return Some(ActivationReason::AlwaysOn);
    }

    None
}

/// Check if `haystack` contains `needle` as a whole word.
fn contains_whole_word(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();
    let nlen = needle_bytes.len();

    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let abs_pos = start + pos;
        let before_ok =
            abs_pos == 0 || !bytes[abs_pos - 1].is_ascii_alphanumeric() && bytes[abs_pos - 1] != b'_';
        let after_pos = abs_pos + nlen;
        let after_ok = after_pos >= bytes.len()
            || !bytes[after_pos].is_ascii_alphanumeric() && bytes[after_pos] != b'_';

        if before_ok && after_ok {
            return true;
        }
        start = abs_pos + 1;
    }
    false
}

// ─── Prompt injection ────────────────────────��───────────────────────────────

/// Format selected skills into a prompt block for injection.
pub fn format_for_prompt(skills: &[&Skill]) -> String {
    if skills.is_empty() {
        return String::new();
    }

    let mut out = String::from("<skills>\n");
    for (i, skill) in skills.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("## {}: {}\n\n", skill.name, skill.description));
        out.push_str(&skill.body);
        out.push('\n');
    }
    out.push_str("</skills>");
    out
}

/// Format skills listing for the `/skills` command.
pub fn format_listing(all_skills: &[Skill], active_names: &HashSet<String>) -> String {
    let mut skills: Vec<&Skill> = all_skills.iter().collect();
    skills.sort_by(|a, b| a.name.cmp(&b.name));

    let mut out = String::from("Skills:\n");
    for skill in &skills {
        let marker = if active_names.contains(&skill.name) {
            "●"
        } else {
            "○"
        };
        let status = if active_names.contains(&skill.name) {
            "active"
        } else {
            "loaded"
        };
        out.push_str(&format!(
            "  {marker} {:<24} [{status}]   {}\n",
            skill.name, skill.description
        ));
    }

    out.push_str("\nLocations searched:\n");
    out.push_str("  ~/.config/tau/skills/\n");
    out.push_str("  .tau/skills/\n");
    out.push_str("  .agents/skills/\n");
    out
}

// ─── Builtins ───────────────────────��────────────────────────────────────────

fn builtin_skills() -> Vec<Skill> {
    let builtins = [
        (
            "conventional-commits",
            include_str!("skills/conventional-commits.md"),
        ),
        ("pr-creation", include_str!("skills/pr-creation.md")),
        ("code-review", include_str!("skills/code-review.md")),
        ("tdd", include_str!("skills/tdd.md")),
    ];

    builtins
        .into_iter()
        .filter_map(|(_, content)| parse_skill(content, SkillSource::Builtin, None))
        .collect()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tau_agent_base::skills::SkillTriggers;

    fn make_skill(name: &str, priority: u8, triggers: SkillTriggers, body: &str) -> Skill {
        Skill {
            name: name.to_string(),
            description: format!("Test skill {name}"),
            triggers,
            priority,
            body: body.to_string(),
            source: SkillSource::Global,
            path: None,
        }
    }

    #[test]
    fn keyword_matching_whole_word() {
        assert!(contains_whole_word("let's commit this", "commit"));
        assert!(contains_whole_word("commit now", "commit"));
        assert!(contains_whole_word("please commit", "commit"));
        assert!(!contains_whole_word("uncommitted changes", "commit"));
        assert!(!contains_whole_word("committed", "commit"));
    }

    #[test]
    fn select_explicit() {
        let skills = vec![make_skill(
            "foo",
            50,
            SkillTriggers {
                commands: vec!["/bar".into()],
                ..Default::default()
            },
            "body",
        )];
        let ctx = MatchContext {
            explicit: HashSet::from(["foo".into()]),
            ..Default::default()
        };
        let selected = select(&skills, &ctx);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "foo");
    }

    #[test]
    fn select_command_trigger() {
        let skills = vec![make_skill(
            "pr",
            50,
            SkillTriggers {
                commands: vec!["/pr".into()],
                ..Default::default()
            },
            "body",
        )];
        let ctx = MatchContext {
            command: Some("/pr".into()),
            ..Default::default()
        };
        let selected = select(&skills, &ctx);
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn select_keyword_trigger() {
        let skills = vec![make_skill(
            "commits",
            50,
            SkillTriggers {
                keywords: vec!["commit".into()],
                ..Default::default()
            },
            "body",
        )];
        let ctx = MatchContext {
            user_message: Some("please commit this change".into()),
            ..Default::default()
        };
        let selected = select(&skills, &ctx);
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn select_always_on() {
        let skills = vec![make_skill("always", 50, SkillTriggers::default(), "body")];
        let ctx = MatchContext::default();
        let selected = select(&skills, &ctx);
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn budget_drops_low_priority() {
        let skills = vec![
            make_skill("high", 90, SkillTriggers::default(), &"x".repeat(10000)),
            make_skill("low", 10, SkillTriggers::default(), &"y".repeat(10000)),
        ];
        let ctx = MatchContext::default();
        // Budget only fits one.
        let selected = select_with_budget(&skills, &ctx, 12000);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "high");
    }

    #[test]
    fn format_prompt_empty() {
        assert_eq!(format_for_prompt(&[]), "");
    }

    #[test]
    fn format_prompt_structure() {
        let skill = make_skill("foo", 50, SkillTriggers::default(), "Do the thing.");
        let out = format_for_prompt(&[&skill]);
        assert!(out.starts_with("<skills>"));
        assert!(out.ends_with("</skills>"));
        assert!(out.contains("## foo: Test skill foo"));
        assert!(out.contains("Do the thing."));
    }

    #[test]
    fn file_glob_trigger() {
        let skills = vec![make_skill(
            "rust-skill",
            50,
            SkillTriggers {
                file_globs: vec!["**/*.rs".into()],
                ..Default::default()
            },
            "body",
        )];
        let ctx = MatchContext {
            files: vec!["src/main.rs".into()],
            ..Default::default()
        };
        let selected = select(&skills, &ctx);
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn no_match_without_trigger() {
        let skills = vec![make_skill(
            "specific",
            50,
            SkillTriggers {
                keywords: vec!["deploy".into()],
                ..Default::default()
            },
            "body",
        )];
        let ctx = MatchContext {
            user_message: Some("fix the bug".into()),
            ..Default::default()
        };
        let selected = select(&skills, &ctx);
        assert!(selected.is_empty());
    }
}
