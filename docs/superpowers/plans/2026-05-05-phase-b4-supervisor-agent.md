# Phase B4: Supervisor Agent — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `task_decompose` tool that spawns a child session to break specs into task DAGs, and a `tau supervise` CLI command that creates a supervisor session with a monitoring loop.

**Architecture:** Tier 1 is a new tool in `orchestration.rs` + handler in `worker.rs` that spawns a child session with a decomposition system prompt. The child session already has `task_create` with `depends_on` available via server-level tool dispatch. Tier 2 is a CLI command that creates a session with a supervisor system prompt. Tier 3 uses the background agent system from B3 — no new code needed, just config.

**Tech Stack:** Rust, serde_json, tau session_spawn/join pattern

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `crates/tau-agent-plugin-worker/src/orchestration.rs` | Add `task_decompose` PluginToolDef |
| Modify | `crates/tau-agent-lib/src/worker.rs` | Add `handle_task_decompose` handler |
| Modify | `crates/tau-agent/src/main.rs` | Add `tau supervise` CLI command |

---

### Task 1: Add `task_decompose` tool definition + handler

**Files:**
- Modify: `crates/tau-agent-plugin-worker/src/orchestration.rs`
- Modify: `crates/tau-agent-lib/src/worker.rs`

- [ ] **Step 1: Add tool definition to orchestration.rs**

In the `orchestration_tools()` function, add a new `PluginToolDef` before the closing `]` (after the `schedule` tool):

```rust
PluginToolDef {
    name: "task_decompose".into(),
    description: "Decompose a specification into a DAG of sub-tasks. Spawns a child session that reads the spec, breaks it into tasks with dependencies, and creates them via task_create.".into(),
    parameters: serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "string",
                "description": "Specification text or file path to decompose into tasks"
            },
            "project_name": {
                "type": "string",
                "description": "Target project for the created tasks"
            },
            "strategy": {
                "type": "string",
                "enum": ["sequential", "parallel", "auto"],
                "description": "Decomposition strategy: sequential (linear chain), parallel (independent tasks), auto (LLM decides). Default: auto"
            }
        },
        "required": ["spec", "project_name"]
    }),
    prompt_snippet: Some("Use task_decompose to break a specification or feature request into a structured DAG of sub-tasks. Each sub-task gets title, description, affected_files, priority, and dependency edges.".into()),
    prompt_guidelines: vec![
        "Prefer task_decompose over manually creating many tasks — it ensures consistent structure and proper dependency edges.".into(),
        "The spec can be inline text or a file path (the child session will read the file if needed).".into(),
        "After decomposition, review the created tasks with task_list to verify the structure makes sense.".into(),
    ],
},
```

- [ ] **Step 2: Add handler in worker.rs**

In worker.rs, add a new dispatch branch after the `schedule` handler (around line 227):

```rust
} else if name == "task_decompose" {
    handle_task_decompose(
        &arguments,
        session_id.as_deref(),
        &msg_tx,
        &pending,
        &unjoined,
    )
    .await
```

Then add the handler function after `handle_schedule_tool`:

```rust
async fn handle_task_decompose(
    args: &serde_json::Value,
    session_id: Option<&str>,
    msg_tx: &smol::channel::Sender<tau_agent_plugin::PluginMessage>,
    pending: &std::sync::Arc<smol::lock::Mutex<std::collections::HashMap<String, smol::channel::Sender<crate::protocol::Response>>>>,
    unjoined: &std::sync::Arc<smol::lock::Mutex<std::collections::HashSet<String>>>,
) -> tau_agent_plugin::ToolResultMessage {
    let tcid = "".to_string();
    let spec = match args.get("spec").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "", "missing required parameter: spec",
            );
        }
    };
    let project_name = match args.get("project_name").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "", "missing required parameter: project_name",
            );
        }
    };
    let strategy = args
        .get("strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("auto");

    let system_prompt = format!(
        "You are a task decomposition agent. Your job is to break a specification into \
         sub-tasks using the task_create tool.\n\n\
         Rules:\n\
         - Create each sub-task with task_create, setting title, description, affected_files, \
           and priority.\n\
         - Use the depends_on parameter to wire dependency edges between tasks.\n\
         - Strategy: {strategy}\n\
           - sequential: create a linear chain where each task depends on the previous\n\
           - parallel: create independent tasks that can run concurrently\n\
           - auto: analyze the spec and choose the best mix of sequential and parallel\n\
         - Target project: {project_name}\n\
         - Keep tasks small and focused — each should be completable in one session.\n\
         - After creating all tasks, output a summary: task count, dependency graph shape, \
           and estimated parallel depth.",
    );

    // Spawn child session
    let create_req = crate::protocol::Request::CreateSession {
        model: None,
        provider: None,
        system_prompt: Some(system_prompt),
        cwd: None,
        parent_id: session_id.map(String::from),
        child_budget: 4,
        tagline: Some("task-decompose".into()),
        auto_archive: true,
        notify_parent: false,
        project_name: Some(project_name.to_string()),
        sandbox_profile: None,
    };
    let resp = match server_request(msg_tx, pending, create_req).await {
        Ok(r) => r,
        Err(e) => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "", &format!("server request failed: {}", e),
            );
        }
    };
    let child_id = match resp {
        crate::protocol::Response::SessionCreated { session_id } => session_id,
        crate::protocol::Response::Error { message } => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "", &format!("spawn failed: {}", message),
            );
        }
        other => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "", &format!("unexpected response: {:?}", other),
            );
        }
    };

    // Send spec as chat message
    let chat_req = crate::protocol::Request::Chat {
        session_id: child_id.clone(),
        text: format!(
            "Decompose this specification into sub-tasks for project '{project_name}':\n\n{spec}"
        ),
        attachments: Vec::new(),
    };
    match server_request(msg_tx, pending, chat_req).await {
        Ok(crate::protocol::Response::Ok) => {}
        Ok(crate::protocol::Response::Error { message }) => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "",
                &format!("session {} created but chat failed: {}", child_id, message),
            );
        }
        Ok(_) | Err(_) => {
            return tau_agent_plugin::ToolResultMessage::error(
                tcid, "",
                &format!("session {} created but chat failed", child_id),
            );
        }
    }

    // Track as unjoined so session_join_all picks it up
    unjoined.lock().await.insert(child_id.clone());

    tau_agent_plugin::ToolResultMessage::success(
        tcid, "",
        &format!(
            "Spawned decomposition session {}. Use session_join to wait for results.",
            child_id
        ),
    )
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p tau-agent-plugin-worker -p tau-agent-lib`

- [ ] **Step 4: Commit**

```bash
git add crates/tau-agent-plugin-worker/src/orchestration.rs crates/tau-agent-lib/src/worker.rs
git commit -S -m "feat(orchestration): add task_decompose tool for spec decomposition"
```

---

### Task 2: Add `tau supervise` CLI command

**Files:**
- Modify: `crates/tau-agent/src/main.rs`

- [ ] **Step 1: Add Commands::Supervise variant**

In the `Commands` enum, after the `Agent` variant (which was added in B3), add:

```rust
/// Create a supervisor session that decomposes a spec and monitors task execution
#[command(alias = "sup")]
Supervise {
    /// Specification file or inline text
    spec: String,
    /// Target project name
    #[arg(long)]
    project: Option<String>,
    /// Model ID (optional)
    #[arg(long)]
    model: Option<String>,
    /// Decomposition strategy: sequential, parallel, auto
    #[arg(long, default_value = "auto")]
    strategy: String,
},
```

- [ ] **Step 2: Add dispatch arm**

In the main match on `Commands`, after the `Agent` dispatch, add:

```rust
Commands::Supervise {
    spec,
    project,
    model,
    strategy,
} => cmd_supervise(&spec, project, model, &strategy).await?,
```

- [ ] **Step 3: Add cmd_supervise function**

Add before the `#[cfg(test)]` block:

```rust
async fn cmd_supervise(
    spec: &str,
    project: Option<String>,
    model: Option<String>,
    strategy: &str,
) -> tau_agent_lib::Result<()> {
    // Read spec from file if it looks like a path
    let spec_text = if std::path::Path::new(spec).exists() {
        std::fs::read_to_string(spec)?
    } else {
        spec.to_string()
    };

    let project_name = project.unwrap_or_else(|| {
        std::env::current_dir()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
            .unwrap_or_else(|| "default".into())
    });

    let system_prompt = format!(
        "You are a project supervisor. Your workflow:\n\
         1. Use task_decompose to break the spec into sub-tasks with dependencies\n\
         2. Use session_join to wait for the decomposition to complete\n\
         3. Use task_list to monitor progress\n\
         4. Review completed tasks and approve or request revisions\n\
         5. Manage merge ordering to respect DAG dependencies\n\n\
         Strategy: {strategy}\n\
         Project: {project_name}\n\n\
         Break large specs into small reviewable tasks. Set appropriate priorities. \
         Prefer parallel work where files don't conflict.",
    );

    let mut client = tau_agent_lib::client::Client::connect_or_start().await?;
    client
        .send(&tau_agent_lib::protocol::Request::CreateSession {
            model,
            provider: None,
            system_prompt: Some(system_prompt),
            cwd: Some(
                std::env::current_dir()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            ),
            parent_id: None,
            child_budget: 32,
            tagline: Some("supervisor".into()),
            auto_archive: false,
            notify_parent: false,
            project_name: Some(project_name.clone()),
            sandbox_profile: None,
        })
        .await?;

    let mut session_id = String::new();
    client
        .recv_streaming(|resp| {
            if let tau_agent_lib::protocol::Response::SessionCreated {
                session_id: sid,
            } = resp
            {
                session_id = sid.clone();
                eprintln!("supervisor session: {}", sid);
            }
        })
        .await?;

    if session_id.is_empty() {
        eprintln!("error: failed to create supervisor session");
        return Ok(());
    }

    // Send the spec as the initial message
    client
        .send(&tau_agent_lib::protocol::Request::Chat {
            session_id: session_id.clone(),
            text: format!(
                "Supervise this project. Start by decomposing this specification into tasks:\n\n{}",
                spec_text
            ),
            attachments: Vec::new(),
        })
        .await?;

    // Stream output
    client
        .recv_streaming(|resp| match resp {
            tau_agent_lib::protocol::Response::Stream { event } => {
                if let tau_agent_base::types::StreamEvent::ContentDelta { delta, .. } = event.as_ref() {
                    print!("{}", delta);
                }
            }
            tau_agent_lib::protocol::Response::AgentDone => {
                println!("\n--- supervisor session complete ---");
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

- [ ] **Step 4: Verify**

Run: `cargo check -p tau-agent && cargo run -p tau-agent -- supervise --help`

- [ ] **Step 5: Commit**

```bash
git add crates/tau-agent/src/main.rs
git commit -S -m "feat(orchestration): add tau supervise CLI command"
```

---

### Task 3: Full verification

- [ ] **Step 1:** `cargo build --workspace`
- [ ] **Step 2:** `cargo test --workspace`
- [ ] **Step 3:** `cargo clippy -p tau-agent-plugin-worker -p tau-agent-lib -p tau-agent -- -D warnings`
