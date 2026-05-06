# Phase A: NL Scheduled Tasks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `schedule` LLM-callable tool (create/list/delete) and a `tau schedule` CLI subcommand so users can manage cron schedules through natural language in chat or raw cron expressions from the command line.

**Architecture:** The `schedule` tool is defined in `orchestration.rs` alongside existing session tools. Execution is handled in `worker.rs` via the async `server_request()` tunnel to forward `CreateSchedule`/`ListSchedules`/`DeleteSchedule` requests to the daemon. The CLI subcommand uses the existing `Client::connect_or_start()` pattern. No new protocol changes — all three schedule request/response variants already exist.

**Tech Stack:** Rust, clap (CLI), tau-agent-plugin (tool definitions), tau-agent-client (Unix socket client)

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `crates/tau-agent-plugin-worker/src/orchestration.rs` | Add `schedule` tool definition (schema + prompt guidelines) |
| Modify | `crates/tau-agent-lib/src/worker.rs:211` | Add dispatch branch for `name == "schedule"` |
| Modify | `crates/tau-agent/src/main.rs:13` | Add `Schedule` variant to `Commands` enum |
| Modify | `crates/tau-agent/src/main.rs` (bottom) | Add `ScheduleAction` enum + `cmd_schedule_*` async functions |

---

### Task 1: Add `schedule` tool definition to orchestration.rs

**Files:**
- Modify: `crates/tau-agent-plugin-worker/src/orchestration.rs:12-384` (add to `orchestration_tools()` vec)
- Test: `crates/tau-agent-plugin-worker/src/orchestration.rs:410-486` (add test)

- [ ] **Step 1: Write the failing test**

Add a test at the bottom of the `mod tests` block (before the closing `}`), after the `no_warning_prefix_or_auto_dispatch_caps_anywhere` test:

```rust
#[test]
fn schedule_tool_has_required_actions() {
    let tools = orchestration_tools();
    let schedule = find_tool(&tools, "schedule");

    let params = &schedule.parameters;
    let action_enum = params["properties"]["action"]["enum"]
        .as_array()
        .expect("action should have enum");
    let action_strings: Vec<&str> = action_enum.iter().map(|v| v.as_str().unwrap()).collect();

    assert!(
        action_strings.contains(&"create"),
        "schedule tool must have 'create' action"
    );
    assert!(
        action_strings.contains(&"list"),
        "schedule tool must have 'list' action"
    );
    assert!(
        action_strings.contains(&"delete"),
        "schedule tool must have 'delete' action"
    );

    assert!(
        schedule.prompt_snippet.is_some(),
        "schedule tool should have a prompt snippet"
    );
    assert!(
        !schedule.prompt_guidelines.is_empty(),
        "schedule tool should have prompt guidelines"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tau-agent-plugin-worker schedule_tool_has_required_actions`
Expected: FAIL — `find_tool` panics with "tool schedule not found"

- [ ] **Step 3: Add schedule tool definition**

In `orchestration.rs`, inside the `orchestration_tools()` function, add this `PluginToolDef` to the returned `vec![]` (after the `session_search` tool, before the closing `]`):

```rust
PluginToolDef {
    name: "schedule".into(),
    description: "Manage recurring scheduled tasks. Create, list, or delete cron schedules that automatically run prompts on a schedule.".into(),
    parameters: serde_json::json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["create", "list", "delete"],
                "description": "Action to perform"
            },
            "name": {
                "type": "string",
                "description": "Schedule name (required for create, optional for delete to delete by name)"
            },
            "when": {
                "type": "string",
                "description": "For create: cron expression (5-field: min hour dom month dow). Translate natural language to cron BEFORE calling this tool."
            },
            "prompt": {
                "type": "string",
                "description": "For create: the message sent to a new session each time the schedule fires"
            },
            "model": {
                "type": "string",
                "description": "For create: model ID for the scheduled session (omit for server default)"
            },
            "id": {
                "type": "integer",
                "description": "For delete: schedule ID to delete"
            }
        },
        "required": ["action"]
    }),
    prompt_snippet: Some("Use the schedule tool to set up recurring automated tasks. The user can describe when they want something to run in natural language — translate it to a 5-field cron expression before calling this tool.".into()),
    prompt_guidelines: vec![
        "When the user describes a schedule in natural language (e.g. 'every weekday at 9am'), convert it to a 5-field cron expression (min hour dom month dow) before calling this tool. Pass the cron expression in the 'when' field.".into(),
        "Always confirm the interpreted cron expression with the user before creating. Show the cron expression and the next 3 fire times so they can verify.".into(),
        "Warn the user if the schedule interval is very frequent (more often than every 5 minutes).".into(),
        "Use 'list' to check existing schedules before creating duplicates.".into(),
    ],
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p tau-agent-plugin-worker schedule_tool_has_required_actions`
Expected: PASS

- [ ] **Step 5: Run all orchestration tests to check for regressions**

Run: `cargo test -p tau-agent-plugin-worker`
Expected: All tests pass. The `no_warning_prefix_or_auto_dispatch_caps_anywhere` test should still pass since the new guidelines don't contain "WARNING:" or "AUTO-DISPATCH".

- [ ] **Step 6: Commit**

```bash
git add crates/tau-agent-plugin-worker/src/orchestration.rs
git commit -S -m "feat(schedule): add schedule tool definition to orchestration"
```

---

### Task 2: Add schedule tool execution to worker.rs

**Files:**
- Modify: `crates/tau-agent-lib/src/worker.rs:211` (add dispatch branch)

- [ ] **Step 1: Add the `handle_schedule_tool` function**

Add this function in `worker.rs`, before the `handle_session_tool` function (around line 824):

```rust
async fn handle_schedule_tool(
    args: &serde_json::Value,
    msg_tx: &Sender<PluginMessage>,
    pending: &Arc<Mutex<HashMap<String, Sender<crate::protocol::Response>>>>,
) -> ToolResultMessage {
    let tcid = "";
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match action {
        "create" => {
            let name = match args.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => return ToolResultMessage::error(tcid, "", "missing required field: name"),
            };
            let cron_expr = match args.get("when").and_then(|v| v.as_str()) {
                Some(w) => w.to_string(),
                None => {
                    return ToolResultMessage::error(tcid, "", "missing required field: when")
                }
            };
            let prompt = match args.get("prompt").and_then(|v| v.as_str()) {
                Some(p) => p.to_string(),
                None => {
                    return ToolResultMessage::error(tcid, "", "missing required field: prompt")
                }
            };
            let model = args.get("model").and_then(|v| v.as_str()).map(String::from);

            let req = crate::protocol::Request::CreateSchedule {
                name: name.clone(),
                cron_expr,
                prompt,
                model,
                cwd: None,
                system_prompt: None,
                project_name: None,
            };
            match server_request(msg_tx, pending, req).await {
                Ok(crate::protocol::Response::ScheduleCreated { id }) => {
                    ToolResultMessage::success(
                        tcid,
                        "",
                        &format!("Created schedule '{}' (id: {})", name, id),
                    )
                }
                Ok(crate::protocol::Response::Error { message }) => {
                    ToolResultMessage::error(tcid, "", &format!("failed to create schedule: {}", message))
                }
                Ok(other) => {
                    ToolResultMessage::error(
                        tcid,
                        "",
                        &format!("unexpected response: {:?}", other),
                    )
                }
                Err(e) => ToolResultMessage::error(tcid, "", &format!("server request failed: {}", e)),
            }
        }
        "list" => {
            let req = crate::protocol::Request::ListSchedules;
            match server_request(msg_tx, pending, req).await {
                Ok(crate::protocol::Response::Schedules { schedules }) => {
                    if schedules.is_empty() {
                        ToolResultMessage::success(tcid, "", "No schedules configured.")
                    } else {
                        let mut out = String::from("Schedules:\n");
                        for s in &schedules {
                            let enabled = if s.enabled { "enabled" } else { "disabled" };
                            let next = s
                                .next_run_at
                                .map(|ts| {
                                    chrono::DateTime::from_timestamp(ts, 0)
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                                        .unwrap_or_else(|| ts.to_string())
                                })
                                .unwrap_or_else(|| "—".to_string());
                            let last = s
                                .last_run_at
                                .map(|ts| {
                                    chrono::DateTime::from_timestamp(ts, 0)
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                                        .unwrap_or_else(|| ts.to_string())
                                })
                                .unwrap_or_else(|| "never".to_string());
                            out.push_str(&format!(
                                "\n  #{} '{}' — {} ({})\n    Cron: {}\n    Prompt: {}\n    Next: {}\n    Last: {}",
                                s.id, s.name, s.cron_expr, enabled, s.cron_expr, s.prompt, next, last,
                            ));
                        }
                        ToolResultMessage::success(tcid, "", &out)
                    }
                }
                Ok(crate::protocol::Response::Error { message }) => {
                    ToolResultMessage::error(tcid, "", &format!("failed to list schedules: {}", message))
                }
                Ok(other) => {
                    ToolResultMessage::error(
                        tcid,
                        "",
                        &format!("unexpected response: {:?}", other),
                    )
                }
                Err(e) => ToolResultMessage::error(tcid, "", &format!("server request failed: {}", e)),
            }
        }
        "delete" => {
            let id = if let Some(id) = args.get("id").and_then(|v| v.as_i64()) {
                id
            } else if let Some(name) = args.get("name").and_then(|v| v.as_str()) {
                // Look up schedule by name first
                let list_req = crate::protocol::Request::ListSchedules;
                match server_request(msg_tx, pending, list_req).await {
                    Ok(crate::protocol::Response::Schedules { schedules }) => {
                        match schedules.iter().find(|s| s.name == name) {
                            Some(s) => s.id,
                            None => {
                                return ToolResultMessage::error(
                                    tcid,
                                    "",
                                    &format!("no schedule found with name '{}'", name),
                                );
                            }
                        }
                    }
                    Ok(crate::protocol::Response::Error { message }) => {
                        return ToolResultMessage::error(
                            tcid,
                            "",
                            &format!("failed to look up schedule: {}", message),
                        );
                    }
                    _ => {
                        return ToolResultMessage::error(tcid, "", "failed to look up schedule");
                    }
                }
            } else {
                return ToolResultMessage::error(
                    tcid,
                    "",
                    "delete requires either 'id' (integer) or 'name' (string)",
                );
            };

            let req = crate::protocol::Request::DeleteSchedule { id };
            match server_request(msg_tx, pending, req).await {
                Ok(crate::protocol::Response::ScheduleDeleted) => {
                    ToolResultMessage::success(tcid, "", &format!("Deleted schedule #{}", id))
                }
                Ok(crate::protocol::Response::Error { message }) => {
                    ToolResultMessage::error(
                        tcid,
                        "",
                        &format!("failed to delete schedule: {}", message),
                    )
                }
                Ok(other) => {
                    ToolResultMessage::error(
                        tcid,
                        "",
                        &format!("unexpected response: {:?}", other),
                    )
                }
                Err(e) => {
                    ToolResultMessage::error(tcid, "", &format!("server request failed: {}", e))
                }
            }
        }
        _ => ToolResultMessage::error(
            tcid,
            "",
            &format!(
                "unknown schedule action '{}'. Use 'create', 'list', or 'delete'.",
                action
            ),
        ),
    }
}
```

- [ ] **Step 2: Add dispatch branch in the tool routing (line ~211)**

In the `let result = if name.starts_with("session_") { ... }` chain, add a new branch for `"schedule"` between `session_*` and `bash`:

Change:

```rust
let result = if name.starts_with("session_") {
    handle_session_tool(
        &name,
        &arguments,
        session_id.as_deref(),
        &msg_tx,
        &pending,
        &unjoined,
    )
    .await
} else if name == "bash" {
```

To:

```rust
let result = if name.starts_with("session_") {
    handle_session_tool(
        &name,
        &arguments,
        session_id.as_deref(),
        &msg_tx,
        &pending,
        &unjoined,
    )
    .await
} else if name == "schedule" {
    handle_schedule_tool(
        &arguments,
        &msg_tx,
        &pending,
    )
    .await
} else if name == "bash" {
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p tau-agent-lib`
Expected: Compiles successfully. If `chrono` is not in scope, add `use chrono;` or check the existing imports — `chrono` is already a workspace dependency of `tau-agent-lib`.

- [ ] **Step 4: Commit**

```bash
git add crates/tau-agent-lib/src/worker.rs
git commit -S -m "feat(schedule): add schedule tool execution via server tunnel"
```

---

### Task 3: Add `tau schedule` CLI subcommand

**Files:**
- Modify: `crates/tau-agent/src/main.rs` (three locations: Commands enum, ScheduleAction enum, dispatch + functions)

- [ ] **Step 1: Add `ScheduleAction` enum**

Add this enum after the existing `ProfileAction` enum (around line 113):

```rust
#[derive(Subcommand)]
enum ScheduleAction {
    /// List all schedules
    #[command(alias = "l")]
    List,
    /// Create a new schedule
    #[command(alias = "c")]
    Create {
        /// Schedule name
        name: String,
        /// Cron expression (5-field: min hour dom month dow)
        #[arg(long)]
        cron: String,
        /// Prompt text sent each time the schedule fires
        #[arg(long)]
        prompt: String,
        /// Model ID (optional, defaults to server default)
        #[arg(long)]
        model: Option<String>,
        /// Working directory for the scheduled session
        #[arg(long)]
        cwd: Option<String>,
        /// System prompt override
        #[arg(long)]
        system_prompt: Option<String>,
        /// Project name to associate the schedule with
        #[arg(long)]
        project: Option<String>,
    },
    /// Delete a schedule by ID
    #[command(alias = "d")]
    Delete {
        /// Schedule ID
        id: i64,
    },
}
```

- [ ] **Step 2: Add `Schedule` variant to `Commands` enum**

In the `Commands` enum (line ~13), add after the `Profile` variant (before `McpServer`):

```rust
/// Manage schedules
#[command(alias = "sched")]
Schedule {
    #[command(subcommand)]
    action: ScheduleAction,
},
```

- [ ] **Step 3: Add dispatch arm in `run()`**

In the `run()` function, in the `match command { ... }` block, add before the `Commands::McpServer` arm:

```rust
Commands::Schedule { action } => match action {
    ScheduleAction::List => cmd_schedule_list().await?,
    ScheduleAction::Create {
        name,
        cron,
        prompt,
        model,
        cwd,
        system_prompt,
        project,
    } => {
        cmd_schedule_create(&name, &cron, &prompt, model, cwd, system_prompt, project)
            .await?
    }
    ScheduleAction::Delete { id } => cmd_schedule_delete(id).await?,
},
```

- [ ] **Step 4: Add `cmd_schedule_list` function**

Add at the bottom of the file, before any `#[cfg(test)]` block:

```rust
async fn cmd_schedule_list() -> tau_agent_lib::Result<()> {
    let mut client = tau_agent_lib::client::Client::connect_or_start().await?;
    client
        .send(&tau_agent_lib::protocol::Request::ListSchedules)
        .await?;
    client
        .recv_streaming(|resp| {
            if let tau_agent_lib::protocol::Response::Schedules { schedules } = resp {
                if schedules.is_empty() {
                    println!("no schedules");
                } else {
                    for s in schedules {
                        let enabled = if s.enabled { "" } else { " [disabled]" };
                        let next = s
                            .next_run_at
                            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                            .unwrap_or_else(|| "—".to_string());
                        let last = s
                            .last_run_at
                            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                            .unwrap_or_else(|| "never".to_string());
                        println!(
                            "#{} '{}' — {}{}\n   prompt: {}\n   next: {}  last: {}",
                            s.id, s.name, s.cron_expr, enabled, s.prompt, next, last,
                        );
                    }
                }
            }
        })
        .await?;
    Ok(())
}
```

- [ ] **Step 5: Add `cmd_schedule_create` function**

```rust
async fn cmd_schedule_create(
    name: &str,
    cron: &str,
    prompt: &str,
    model: Option<String>,
    cwd: Option<String>,
    system_prompt: Option<String>,
    project: Option<String>,
) -> tau_agent_lib::Result<()> {
    let mut client = tau_agent_lib::client::Client::connect_or_start().await?;
    client
        .send(&tau_agent_lib::protocol::Request::CreateSchedule {
            name: name.to_string(),
            cron_expr: cron.to_string(),
            prompt: prompt.to_string(),
            model,
            cwd,
            system_prompt,
            project_name: project,
        })
        .await?;
    client
        .recv_streaming(|resp| match resp {
            tau_agent_lib::protocol::Response::ScheduleCreated { id } => {
                eprintln!("created schedule #{} '{}'", id, name);
            }
            tau_agent_lib::protocol::Response::Error { message } => {
                eprintln!("error: {}", message);
            }
            _ => {}
        })
        .await?;
    Ok(())
}
```

- [ ] **Step 6: Add `cmd_schedule_delete` function**

```rust
async fn cmd_schedule_delete(id: i64) -> tau_agent_lib::Result<()> {
    let mut client = tau_agent_lib::client::Client::connect_or_start().await?;
    client
        .send(&tau_agent_lib::protocol::Request::DeleteSchedule { id })
        .await?;
    client
        .recv_streaming(|resp| match resp {
            tau_agent_lib::protocol::Response::ScheduleDeleted => {
                eprintln!("deleted schedule #{}", id);
            }
            tau_agent_lib::protocol::Response::Error { message } => {
                eprintln!("error: {}", message);
            }
            _ => {}
        })
        .await?;
    Ok(())
}
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo check -p tau-agent`
Expected: Compiles successfully.

- [ ] **Step 8: Verify CLI help works**

Run: `cargo run -p tau-agent -- schedule --help`
Expected output includes `list`, `create`, `delete` subcommands.

Run: `cargo run -p tau-agent -- schedule create --help`
Expected output includes `--cron`, `--prompt`, `--model`, `--cwd` flags.

- [ ] **Step 9: Commit**

```bash
git add crates/tau-agent/src/main.rs
git commit -S -m "feat(schedule): add tau schedule CLI subcommand"
```

---

### Task 4: Verify full build and run all tests

**Files:** None (verification only)

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: Builds successfully with no errors.

- [ ] **Step 2: Run all workspace tests**

Run: `cargo test --workspace`
Expected: All tests pass. Pay attention to:
- `tau-agent-plugin-worker` tests (new `schedule_tool_has_required_actions` test)
- `no_warning_prefix_or_auto_dispatch_caps_anywhere` test (should still pass — new guidelines are clean)

- [ ] **Step 3: Verify no clippy warnings on changed files**

Run: `cargo clippy -p tau-agent-plugin-worker -p tau-agent-lib -p tau-agent -- -D warnings`
Expected: No warnings.

- [ ] **Step 4: Commit any fixes if needed, then done**

If any test or clippy fixes were needed:
```bash
git add -u
git commit -S -m "fix(schedule): address test/clippy issues"
```
