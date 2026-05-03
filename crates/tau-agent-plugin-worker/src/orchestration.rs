//! Session orchestration tools.
//!
//! Provides tool schemas and prompt guidelines for session management tools
//! (session_spawn, session_join, session_status, session_list_children,
//! session_read, session_cancel, session_archive, session_id). These tools communicate with the server
//! via the plugin protocol's ServerRequest/ServerResponse tunnel.

use tau_agent_plugin::ToolPrompt;
use tau_agent_plugin::{PluginRegistration, PluginToolDef};

/// Tool definitions for session orchestration.
pub fn orchestration_tools() -> Vec<PluginToolDef> {
    vec![
        PluginToolDef {
            name: "session_spawn".into(),
            description: "Spawn a child session to work on a subtask in parallel. Returns the session ID. The child runs independently with its own tools and context.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "task": {
                        "type": "string",
                        "description": "Initial message/task for the child session"
                    },
                    "model": {
                        "type": "string",
                        "description": "Model ID for the child (omit to inherit parent's model)"
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Custom system prompt (omit for default)"
                    },
                    "cwd": {
                        "type": "string",
                        "description": "Working directory (omit to inherit parent's)"
                    },
                    "child_budget": {
                        "type": "integer",
                        "description": "Max descendant sessions the child can spawn (default 4)"
                    },
                    "tagline": {
                        "type": "string",
                        "description": "Short description of what the child session will work on (shown in session picker)"
                    },
                    "auto_archive": {
                        "type": "boolean",
                        "description": "If true, automatically archive this child session after it completes and is joined (default false)"
                    },
                    "notify_parent": {
                        "type": "boolean",
                        "description": "If true, notify the parent session when this child completes (default true). Set to false for task-managed sessions."
                    }
                },
                "required": ["task"]
            }),
            prompt_snippet: Some("Use session_spawn to delegate independent subtasks to child sessions that run in parallel. Each child has its own tools and conversation context. Use session_join to wait for results.".into()),
            prompt_guidelines: vec![
                "Spawn children for parallelizable work (e.g., reviewing multiple files)".into(),
                "Children can spawn grandchildren by default (child_budget=4). Set child_budget=0 to make a leaf.".into(),
                "The task should be self-contained -- the child has no access to the parent's conversation.".into(),
                "When spawning children purely for file reading or code summarization (no reasoning needed), pass model: \"light\" to use a cheaper model. Do NOT use \"light\" for sessions that review, analyse, plan, or generate code — those need the full model.".into(),
            ],
        },
        PluginToolDef {
            name: "session_join".into(),
            description: "Wait for child sessions to complete and get their results. Blocks until all specified sessions are done or timeout is reached.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Session IDs to wait for"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (default 300)"
                    }
                },
                "required": ["session_ids"]
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![
                "Use after spawning children to collect their results.".into(),
                "Returns status and summary (last assistant message) for each session.".into(),
                // Phase 1 seamless-restart caveat: the plugin and the
                // server share a lifetime, so a server restart in the
                // middle of a session_join causes the call to error out.
                // Re-issuing the call after reconnect is safe (state is
                // server-authoritative via the session lock +
                // last_exit_status).
                "If the server restarts during this call, re-issue the same call; the server is authoritative and the response will reflect the latest state.".into(),
            ],
        },
        PluginToolDef {
            name: "session_join_all".into(),
            description: "Wait for all unjoined child sessions to complete. Blocks until every child spawned (that hasn't been joined yet) is done or timeout is reached.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (default 300)"
                    }
                }
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![
                "Waits for all children spawned since the last join. No need to track session IDs.".into(),
                "Returns status and summary for each child session.".into(),
            ],
        },
        PluginToolDef {
            name: "session_join_any".into(),
            description: "Wait for any unjoined child session to complete. Returns as soon as at least one child finishes, with results for all completed children.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (default 300)"
                    }
                }
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![
                "Use for fan-out patterns where children have variable completion times.".into(),
                "Returns results for all children that completed, remaining stay unjoined.".into(),
            ],
        },
        PluginToolDef {
            name: "session_status".into(),
            description: "Check the status of a child session without blocking.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session ID to check"
                    }
                },
                "required": ["session_id"]
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![],
        },
        PluginToolDef {
            name: "session_list_children".into(),
            description: "List direct child sessions of the current session.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![],
        },
        PluginToolDef {
            name: "session_read".into(),
            description: "Read messages from a child session's conversation.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session ID to read from"
                    },
                    "last_n": {
                        "type": "integer",
                        "description": "Number of recent messages to return (omit for all)"
                    }
                },
                "required": ["session_id"]
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![],
        },
        PluginToolDef {
            name: "session_cancel".into(),
            description: "Cancel a running child session.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session ID to cancel"
                    }
                },
                "required": ["session_id"]
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![],
        },
        PluginToolDef {
            name: "session_archive".into(),
            description: "Archive a child session. The session must be a descendant of the current session. Archived sessions are soft-deleted: hidden from listings but preserved in the database.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "oneOf": [
                            { "type": "string", "description": "Session ID to archive" },
                            { "type": "array", "items": { "type": "string" }, "description": "Session IDs to archive" }
                        ]
                    }
                },
                "required": ["session_id"]
            }),
            prompt_snippet: Some("Use session_archive to clean up completed child sessions. Only works on descendants of the current session. Accepts a single session ID or an array of session IDs.".into()),
            prompt_guidelines: vec![
                "Archive children only once you're done with their transcripts — session_read returns nothing useful after archive.".into(),
                "For children whose output you'll only consume once, pass auto_archive=true to session_spawn so cleanup happens automatically on completion.".into(),
            ],
        },
        PluginToolDef {
            name: "session_restore".into(),
            description: "Restore (un-archive) a previously archived session. The session and all its descendants are restored and become visible in session listings again.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session ID to restore"
                    }
                },
                "required": ["session_id"]
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![],
        },
        PluginToolDef {
            name: "session_message".into(),
            description: "Send a message to another session. The message is injected as a user message into the target session's conversation. If the target is idle, it will resume processing. Fire-and-forget: does not wait for a response.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Target session ID to send the message to"
                    },
                    "content": {
                        "type": "string",
                        "description": "Message content to send"
                    },
                    "await_reply": {
                        "type": "boolean",
                        "description": "If true, block until the target session replies via session_reply (default false)"
                    }
                },
                "required": ["session_id", "content"]
            }),
            prompt_snippet: Some("Send a message to another session (parent, child, or sibling) as a user message.".into()),
            prompt_guidelines: vec![
                "Fire-and-forget by default — use session_read to check for the target's response later.".into(),
                "Pass await_reply=true to block until the target calls session_reply; your call returns the reply content directly.".into(),
                // Phase 1 seamless-restart caveat for await_reply=true:
                // if the server restarts while the caller is blocked
                // waiting for a reply, the call errors out. Re-issuing
                // it delivers the message a second time (at-least-once
                // semantics); Phase 2 adds persistent waiter state.
                "If await_reply=true and the server restarts, re-issuing this tool call will deliver the message twice. Until the persistent-waiter work lands, prefer idempotent message content or wait for the child via session_join instead.".into(),
            ],
        },
        PluginToolDef {
            name: "session_reply".into(),
            description: "Reply to a pending await_reply message. The sender of the original message is unblocked and receives this reply.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "msg_id": {
                        "type": "string",
                        "description": "The msg_id from the incoming message (e.g. \"m42\")"
                    },
                    "content": {
                        "type": "string",
                        "description": "Reply content to send back to the sender"
                    }
                },
                "required": ["msg_id", "content"]
            }),
            prompt_snippet: Some("Reply to a session_message that had await_reply=true.".into()),
            prompt_guidelines: vec![
                "Use only when you've received a message containing 'awaits reply, msg_id=...' — extract the msg_id and pass it here.".into(),
            ],
        },
        PluginToolDef {
            name: "session_id".into(),
            description: "Get the current session's ID.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
            prompt_snippet: None,
            prompt_guidelines: vec![],
        },
        PluginToolDef {
            name: "memory".into(),
            description: "Manage persistent memory that carries across sessions. Add, replace, or remove facts the agent should remember.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["add", "replace", "remove", "list"],
                        "description": "Action to perform on memory"
                    },
                    "scope": {
                        "type": "string",
                        "enum": ["project", "global"],
                        "description": "project = .tau/memory.md, global = ~/.config/tau/memory.md (default: project)"
                    },
                    "content": {
                        "type": "string",
                        "description": "For add: text to append. For replace: new full content. For remove: text to remove (exact match of a line)."
                    }
                },
                "required": ["action"]
            }),
            prompt_snippet: Some("Use memory to persist facts, preferences, and decisions across sessions. Project memory is shared by all sessions in this project; global memory is shared across all projects.".into()),
            prompt_guidelines: vec![
                "Add facts when you learn something reusable (user preferences, project conventions, key decisions).".into(),
                "Keep memory concise — it's injected into every session's prompt. Curate actively.".into(),
                "Use 'list' to see current memory before modifying.".into(),
                "Project memory is capped at ~2200 chars, global at ~1400 chars. Remove stale entries to make room.".into(),
            ],
        },
        PluginToolDef {
            name: "skill_patch".into(),
            description: "Read, patch, or delete skill files. Auto-generated skills (.tau/skills/auto/) are freely editable; hand-written skills warn but allow patches.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "read", "patch", "delete"],
                        "description": "Action to perform"
                    },
                    "name": {
                        "type": "string",
                        "description": "Skill name (required for read, patch, delete)"
                    },
                    "content": {
                        "type": "string",
                        "description": "For patch: full new file content including YAML frontmatter"
                    }
                },
                "required": ["action"]
            }),
            prompt_snippet: Some("Use skill_patch to fix or improve auto-generated skills when you discover they contain incorrect or outdated guidance.".into()),
            prompt_guidelines: vec![
                "Use 'list' first to see available skills and which are auto-generated.".into(),
                "Use 'read' to see a skill's current content before patching.".into(),
                "Patches must include the full YAML frontmatter (---...---) plus body.".into(),
                "Only delete auto-generated skills that are actively harmful. Prefer patching over deletion.".into(),
            ],
        },
        PluginToolDef {
            name: "session_search".into(),
            description: "Search across all past session messages using full-text search. Returns matching snippets with session context.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "FTS5 search query (supports AND, OR, NOT, phrase matching with quotes)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default 10, max 50)"
                    },
                    "project_name": {
                        "type": "string",
                        "description": "Restrict search to sessions in this project (omit for all)"
                    }
                },
                "required": ["query"]
            }),
            prompt_snippet: Some("Use session_search to find relevant information from past sessions — useful for recalling previous decisions, approaches, or context.".into()),
            prompt_guidelines: vec![
                "Search before re-solving a problem — past sessions may have the answer.".into(),
                "Use specific keywords or quoted phrases for precise results.".into(),
                "Results include session_id — use session_read to get full context if needed.".into(),
            ],
        },
    ]
}

/// Registration for the orchestration plugin.
pub fn registration() -> PluginRegistration {
    PluginRegistration {
        name: "orchestration".into(),
        tools: orchestration_tools(),
        hooks: vec![],
        commands: vec![],
    }
}

/// Tool prompt entries for system prompt generation.
pub fn tool_prompts() -> Vec<ToolPrompt> {
    orchestration_tools()
        .into_iter()
        .filter_map(|t| {
            t.prompt_snippet.map(|snippet| ToolPrompt {
                name: t.name,
                snippet,
                guidelines: t.prompt_guidelines,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find_tool<'a>(tools: &'a [PluginToolDef], name: &str) -> &'a PluginToolDef {
        tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("tool {name} not found"))
    }

    #[test]
    fn session_message_and_reply_guideline_counts() {
        let tools = orchestration_tools();
        let msg = find_tool(&tools, "session_message");
        let reply = find_tool(&tools, "session_reply");

        assert_eq!(
            msg.prompt_guidelines.len(),
            3,
            "session_message should have exactly 3 guidelines, got {:?}",
            msg.prompt_guidelines,
        );
        assert_eq!(
            reply.prompt_guidelines.len(),
            1,
            "session_reply should have exactly 1 guideline, got {:?}",
            reply.prompt_guidelines,
        );
    }

    #[test]
    fn session_archive_mentions_auto_archive() {
        let tools = orchestration_tools();
        let archive = find_tool(&tools, "session_archive");

        assert_eq!(
            archive.prompt_guidelines.len(),
            2,
            "session_archive should have exactly 2 guidelines, got {:?}",
            archive.prompt_guidelines,
        );
        assert!(
            archive
                .prompt_guidelines
                .iter()
                .any(|g| g.contains("auto_archive")),
            "session_archive guidelines should mention auto_archive, got {:?}",
            archive.prompt_guidelines,
        );
    }

    #[test]
    fn no_warning_prefix_or_auto_dispatch_caps_anywhere() {
        let all_tools: Vec<PluginToolDef> = orchestration_tools()
            .into_iter()
            .chain(tau_agent_plugin_tasks::tasks::plugin_tool_defs())
            .collect();

        for tool in &all_tools {
            for guideline in &tool.prompt_guidelines {
                assert!(
                    !guideline.starts_with("WARNING:"),
                    "tool {} has a guideline starting with 'WARNING:': {:?}",
                    tool.name,
                    guideline,
                );
                assert!(
                    !guideline.contains("AUTO-DISPATCH"),
                    "tool {} has a guideline containing 'AUTO-DISPATCH' (all-caps): {:?}",
                    tool.name,
                    guideline,
                );
            }
        }
    }
}
