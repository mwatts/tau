//! SI-1: Automatic skill extraction from merged tasks.
//!
//! After a task reaches `merged` state, this module spawns a lightweight
//! child session that reads the task's conversation and extracts reusable
//! procedural knowledge into `.tau/skills/auto/`. The extraction is
//! fire-and-forget — it doesn't block the merge path.

use std::io::{BufRead, Write};

use crate::tasks_db::{Task, TasksDb};
use crate::tasks_scheduler::server_request;
use crate::tasks_session::TaskSessionSpec;

/// Minimum number of messages in the task's worker session(s) before
/// skill extraction is worthwhile. Short tasks (e.g., one-liner fixes)
/// rarely produce reusable procedural knowledge.
const MIN_MESSAGES_THRESHOLD: usize = 8;

/// Fire skill extraction for a merged task. Non-blocking: spawns a child
/// session and returns immediately. Errors are logged but never propagated
/// (this is best-effort post-merge work).
pub fn trigger_skill_extraction(
    db: &TasksDb,
    task: &Task,
    project_path: &str,
    writer: &mut impl Write,
    reader: &mut impl BufRead,
) {
    if should_skip(task) {
        return;
    }

    // Check message volume threshold — only extract skills from substantial work.
    let session_ids: Vec<String> = match db.get_sessions(task.id) {
        Ok(sessions) => sessions.into_iter().map(|s| s.session_id).collect(),
        Err(e) => {
            eprintln!(
                "skill autogen: failed to get sessions for task {}: {}",
                task.id, e
            );
            return;
        }
    };

    if session_ids.is_empty() {
        eprintln!("skill autogen: task {} has no sessions, skipping", task.id);
        return;
    }

    // Count messages across all worker sessions for this task.
    // We use GetMessages ServerRequest to check volume.
    let total_messages = count_task_messages(&session_ids, writer, reader);
    if total_messages < MIN_MESSAGES_THRESHOLD {
        eprintln!(
            "skill autogen: task {} has only {} messages (threshold {}), skipping",
            task.id, total_messages, MIN_MESSAGES_THRESHOLD
        );
        return;
    }

    // Spawn the extraction session.
    let session_id = match create_extraction_session(task, project_path, writer, reader) {
        Ok(id) => id,
        Err(e) => {
            eprintln!(
                "skill autogen: failed to create extraction session for task {}: {}",
                task.id, e
            );
            return;
        }
    };

    // Send the extraction prompt.
    let prompt = build_extraction_prompt(task, &session_ids);
    let chat_req = tau_agent_plugin::Request::Chat {
        session_id: session_id.clone(),
        text: prompt,
        attachments: Vec::new(),
    };
    match server_request(writer, reader, chat_req) {
        Ok(tau_agent_plugin::Response::Ok) => {
            eprintln!(
                "skill autogen: extraction session {} spawned for task {} (\"{}\")",
                session_id, task.id, task.title
            );
        }
        Ok(tau_agent_plugin::Response::Error { message }) => {
            eprintln!(
                "skill autogen: chat failed for task {}: {}",
                task.id, message
            );
        }
        _ => {
            eprintln!(
                "skill autogen: unexpected response for task {} chat",
                task.id
            );
        }
    }
}

/// Decide whether to skip extraction for this task.
fn should_skip(task: &Task) -> bool {
    // Skip tasks with certain tags that indicate they're mechanical/automated.
    if let Some(ref tags) = task.tags {
        if let Some(arr) = tags.as_array() {
            for tag in arr {
                if let Some(s) = tag.as_str() {
                    if s == "no-skill-extract" || s == "automated" {
                        eprintln!(
                            "skill autogen: task {} tagged '{}', skipping",
                            task.id, s
                        );
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Count total messages across the given session IDs by querying the server.
fn count_task_messages(
    session_ids: &[String],
    writer: &mut impl Write,
    reader: &mut impl BufRead,
) -> usize {
    let mut total = 0;
    for sid in session_ids {
        let req = tau_agent_plugin::Request::GetSessionInfo {
            session_id: sid.clone(),
        };
        if let Ok(tau_agent_plugin::Response::SessionInfo { info }) =
            server_request(writer, reader, req)
        {
            total += info.message_count as usize;
        }
    }
    total
}

/// Create the extraction session — a light-model leaf session.
fn create_extraction_session(
    task: &Task,
    project_path: &str,
    writer: &mut impl Write,
    reader: &mut impl BufRead,
) -> tau_agent_plugin::Result<String> {
    crate::tasks_session::create_task_session(
        TaskSessionSpec {
            task,
            role: "skill-extract",
            model: Some("light".to_string()),
            cwd: Some(project_path.to_string()),
            parent_id: task.placeholder_session_id.clone(),
            child_budget: 0,
            sandbox_profile: None,
        },
        writer,
        reader,
    )
}

/// Build the prompt that instructs the extraction session to produce a skill.
/// Exposed for testing.
pub(crate) fn build_extraction_prompt(task: &Task, session_ids: &[String]) -> String {
    let session_list = session_ids
        .iter()
        .map(|s| format!("- {}", s))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"You are a skill extraction agent. Your job is to analyze the conversation history
of a completed task and extract reusable procedural knowledge into a skill file.

## Task that was completed

- **ID**: {id}
- **Title**: {title}
- **Project**: {project}

## Session IDs to read

{sessions}

## Instructions

1. Use `session_read` (or `bash` with appropriate commands) to read the messages from the session IDs listed above.
2. Analyze the conversation for reusable procedural knowledge — patterns, techniques, workflows, or conventions that would help a future agent working on similar tasks.
3. If the task was trivial, mechanical, or too domain-specific to generalize, output "NO_SKILL_EXTRACTED" and stop.
4. Otherwise, write a skill file to `.tau/skills/auto/` with the following format:

```markdown
---
name: <short-kebab-case-name>
description: <one-line description>
triggers:
  keywords: [<relevant keywords>]
  file_globs: [<relevant file patterns, if applicable>]
---

<Skill content: concise procedural knowledge that helps an agent perform similar work.
Focus on the HOW — steps, patterns, gotchas, conventions discovered.
Keep it under 800 characters. No preamble or meta-commentary.>
```

5. The filename should be `<name>.md` matching the `name` field in frontmatter.
6. Create the `.tau/skills/auto/` directory if it doesn't exist.

## Quality bar

Only extract a skill if it meets ALL of these criteria:
- **Reusable**: Would help on at least 2-3 different future tasks
- **Procedural**: Describes HOW to do something, not just WHAT was done
- **Non-obvious**: Contains knowledge that wouldn't be apparent from reading the code alone
- **Concise**: Can be expressed in under 800 characters of actionable guidance

If in doubt, don't extract. "NO_SKILL_EXTRACTED" is a perfectly valid outcome."#,
        id = task.id,
        title = task.title,
        project = task.project_name,
        sessions = session_list,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(id: i64, title: &str, tags: Option<serde_json::Value>) -> Task {
        Task {
            id,
            title: title.into(),
            state: crate::tasks_state::TaskState::Merged,
            project_name: "test-project".into(),
            priority: 0,
            parent_id: None,
            tags,
            affected_files: None,
            branch: None,
            merge_target: None,
            worktree_path: None,
            session_id: None,
            skip_review: false,
            require_approval: false,
            sandbox_profile: None,
            held: false,
            placeholder_session_id: None,
            auto_downgraded_from_ready: false,
            filed_by_project: None,
            filed_by_session_id: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn should_skip_no_tags() {
        let task = make_task(1, "Add feature X", None);
        assert!(!should_skip(&task));
    }

    #[test]
    fn should_skip_automated_tag() {
        let task = make_task(
            1,
            "Automated cleanup",
            Some(serde_json::json!(["automated"])),
        );
        assert!(should_skip(&task));
    }

    #[test]
    fn should_skip_no_skill_extract_tag() {
        let task = make_task(
            1,
            "Trivial fix",
            Some(serde_json::json!(["no-skill-extract"])),
        );
        assert!(should_skip(&task));
    }

    #[test]
    fn should_skip_other_tags_ok() {
        let task = make_task(1, "Feature", Some(serde_json::json!(["enhancement", "v2"])));
        assert!(!should_skip(&task));
    }

    #[test]
    fn extraction_prompt_contains_task_info() {
        let task = make_task(1, "Implement auth middleware", None);
        let sessions = vec!["s-abc123".into(), "s-def456".into()];
        let prompt = build_extraction_prompt(&task, &sessions);

        assert!(prompt.contains("Implement auth middleware"));
        assert!(prompt.contains("test-project"));
        assert!(prompt.contains("s-abc123"));
        assert!(prompt.contains("s-def456"));
        assert!(prompt.contains(".tau/skills/auto/"));
        assert!(prompt.contains("NO_SKILL_EXTRACTED"));
    }
}
