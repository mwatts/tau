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
    use std::io::{BufReader, Write};
    use std::sync::{Arc, Mutex};
    use tau_agent_plugin::{PluginMessage, PluginRequest, Response};

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

    // -- Mock writer/reader infrastructure for multi-request tests --

    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Reader that yields pre-canned responses for each successive ServerRequest.
    /// Mirrors the request_id from the writer so the tunnel protocol matches.
    struct SequencingReader {
        writer: Arc<Mutex<Vec<u8>>>,
        responses: Vec<Response>,
        next: usize,
        buf: Vec<u8>,
        seen_requests: usize,
    }

    impl std::io::Read for SequencingReader {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.buf.is_empty() {
                if self.next >= self.responses.len() {
                    return Ok(0); // EOF
                }
                // Wait for a new request we haven't responded to yet.
                let written = self.writer.lock().unwrap().clone();
                let text = String::from_utf8_lossy(&written);
                let mut count = 0;
                let mut last_rid = None;
                for line in text.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if let Ok(PluginMessage::ServerRequest { request_id, .. }) =
                        serde_json::from_str::<PluginMessage>(line)
                    {
                        count += 1;
                        last_rid = Some(request_id);
                    }
                }
                if count <= self.seen_requests {
                    // No new request yet — yield empty to avoid busy spin
                    // (the caller's read_line will retry)
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "waiting for next request",
                    ));
                }
                self.seen_requests = count;
                let rid = last_rid.unwrap();
                let resp_line = serde_json::to_string(&PluginRequest::ServerResponse {
                    request_id: rid,
                    response: self.responses[self.next].clone(),
                })
                .unwrap();
                self.next += 1;
                self.buf = resp_line.into_bytes();
                self.buf.push(b'\n');
            }
            let n = std::cmp::min(out.len(), self.buf.len());
            out[..n].copy_from_slice(&self.buf[..n]);
            self.buf.drain(..n);
            Ok(n)
        }
    }

    fn fake_session_info(id: &str, message_count: usize) -> tau_agent_plugin::SessionInfo {
        tau_agent_plugin::SessionInfo {
            id: id.into(),
            model: "test".into(),
            provider: "test".into(),
            cwd: None,
            message_count,
            stats: tau_agent_base::protocol::SessionStats {
                user_messages: 0,
                assistant_messages: 0,
                tool_calls: 0,
                tool_results: 0,
                tokens: Default::default(),
                cost: 0.0,
                is_subscription: false,
                context_window: 0,
                context_tokens: None,
            },
            last_activity: 0,
            parent_id: None,
            child_count: 0,
            child_budget: 0,
            tagline: None,
            state: "idle".into(),
            context_pct: None,
            archived: false,
            project_name: None,
            last_exit_status: None,
            is_live: false,
            turn_started_at_ms: None,
            phase_started_at_ms: None,
        }
    }

    fn extract_requests(emitted: &[u8]) -> Vec<tau_agent_plugin::Request> {
        let text = String::from_utf8_lossy(emitted);
        let mut reqs = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(PluginMessage::ServerRequest { request: req, .. }) =
                serde_json::from_str::<PluginMessage>(line)
            {
                reqs.push(req);
            }
        }
        reqs
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

    #[test]
    fn trigger_skips_when_below_message_threshold() {
        let db = TasksDb::open_memory().unwrap();
        let task = db
            .create_task(
                "test-project",
                "Small fix",
                None,
                None,
                None,
                false,
                "ready",
                false,
                None,
                None,
                false,
                None,
                false,
                crate::tasks_db::FiledBy { project: None, session_id: Some("s-parent") },
            )
            .unwrap();
        db.record_session(task.id, "s-worker-1", "worker").unwrap();

        // Return session info with only 3 messages (below threshold of 8)
        let shared = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut writer = SharedWriter(shared.clone());
        let reader = SequencingReader {
            writer: shared.clone(),
            responses: vec![Response::SessionInfo {
                info: fake_session_info("s-worker-1", 3),
            }],
            next: 0,
            buf: Vec::new(),
            seen_requests: 0,
        };
        let mut reader = BufReader::new(reader);

        trigger_skill_extraction(&db, &task, "/tmp/project", &mut writer, &mut reader);

        let reqs = extract_requests(&shared.lock().unwrap());
        // Should only have the GetSessionInfo request, no CreateSession
        assert_eq!(reqs.len(), 1);
        assert!(matches!(
            &reqs[0],
            tau_agent_plugin::Request::GetSessionInfo { .. }
        ));
    }

    #[test]
    fn trigger_full_flow_creates_session_and_chats() {
        let db = TasksDb::open_memory().unwrap();
        let task = db
            .create_task(
                "test-project",
                "Implement feature X",
                None,
                None,
                None,
                false,
                "ready",
                false,
                None,
                None,
                false,
                None,
                false,
                crate::tasks_db::FiledBy { project: None, session_id: Some("s-parent") },
            )
            .unwrap();
        db.record_session(task.id, "s-worker-1", "worker").unwrap();

        // Sequence: GetSessionInfo (10 msgs) → CreateSession → Chat Ok
        let shared = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut writer = SharedWriter(shared.clone());
        let reader = SequencingReader {
            writer: shared.clone(),
            responses: vec![
                Response::SessionInfo {
                    info: fake_session_info("s-worker-1", 10),
                },
                Response::SessionCreated {
                    session_id: "s-extract-1".into(),
                },
                Response::Ok,
            ],
            next: 0,
            buf: Vec::new(),
            seen_requests: 0,
        };
        let mut reader = BufReader::new(reader);

        trigger_skill_extraction(&db, &task, "/tmp/project", &mut writer, &mut reader);

        let reqs = extract_requests(&shared.lock().unwrap());
        assert_eq!(reqs.len(), 3, "expected 3 requests: {:?}", reqs);

        // 1. GetSessionInfo
        assert!(matches!(
            &reqs[0],
            tau_agent_plugin::Request::GetSessionInfo { session_id } if session_id == "s-worker-1"
        ));

        // 2. CreateSession with light model
        match &reqs[1] {
            tau_agent_plugin::Request::CreateSession { model, .. } => {
                assert_eq!(model.as_deref(), Some("light"));
            }
            other => panic!("expected CreateSession, got {:?}", other),
        }

        // 3. Chat with extraction prompt
        match &reqs[2] {
            tau_agent_plugin::Request::Chat {
                session_id, text, ..
            } => {
                assert_eq!(session_id, "s-extract-1");
                assert!(text.contains("Implement feature X"));
                assert!(text.contains("s-worker-1"));
            }
            other => panic!("expected Chat, got {:?}", other),
        }
    }

    #[test]
    fn trigger_skips_tagged_tasks_without_any_requests() {
        let db = TasksDb::open_memory().unwrap();
        let task = db
            .create_task(
                "test-project",
                "Automated",
                None,
                None,
                Some(&serde_json::json!(["automated"])),
                false,
                "ready",
                false,
                None,
                None,
                false,
                None,
                false,
                crate::tasks_db::FiledBy { project: None, session_id: Some("s-parent") },
            )
            .unwrap();
        db.record_session(task.id, "s-worker-1", "worker").unwrap();

        let shared = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut writer = SharedWriter(shared.clone());
        // No responses needed — should bail before any request
        let reader = SequencingReader {
            writer: shared.clone(),
            responses: vec![],
            next: 0,
            buf: Vec::new(),
            seen_requests: 0,
        };
        let mut reader = BufReader::new(reader);

        trigger_skill_extraction(&db, &task, "/tmp/project", &mut writer, &mut reader);

        let reqs = extract_requests(&shared.lock().unwrap());
        assert_eq!(reqs.len(), 0);
    }
}
