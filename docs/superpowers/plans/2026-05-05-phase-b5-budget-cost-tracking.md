# Phase B5: Budget & Cost Tracking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-task budget caps and enhance cost reporting with per-agent and per-task cost views.

**Architecture:** Add `budget_usd` and `spent_usd` columns to the tasks table (migration pattern). Add `budget_usd` to the `task_create` tool definition. Extend `tau project stats` to include agent costs. Agent budget enforcement already has `budget_usd`/`spent_usd` columns from B3 — just needs the `tau stats` display.

**Tech Stack:** Rust, rusqlite, tau tasks DB migration pattern

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `crates/tau-agent-plugin-tasks/src/tasks_db.rs` | Add budget_usd/spent_usd columns via migration |
| Modify | `crates/tau-agent-plugin-tasks/src/tasks.rs` | Add budget_usd to task_create tool def + handler |
| Modify | `crates/tau-agent/src/main.rs` | Enhance stats display with agent costs |

---

### Task 1: Add budget columns to tasks table

**Files:**
- Modify: `crates/tau-agent-plugin-tasks/src/tasks_db.rs`

- [ ] **Step 1: Add budget_usd and spent_usd to Task struct**

In the `Task` struct (starting at line 15), add after `filed_by_session_id`:

```rust
pub budget_usd: Option<f64>,
pub spent_usd: f64,
```

- [ ] **Step 2: Add columns to SCHEMA**

In the `SCHEMA` constant (around line 257), add before `created_at`:

```sql
    budget_usd REAL,
    spent_usd REAL NOT NULL DEFAULT 0.0,
```

- [ ] **Step 3: Add migration for existing databases**

In the `migrate()` function, add at the end (following the existing migration pattern):

```rust
let has_budget_usd: bool = conn
    .prepare("SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name = 'budget_usd'")
    .and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, i64>(0)))
    .map(|count| count > 0)
    .unwrap_or(false);

if !has_budget_usd {
    conn.execute_batch(
        "ALTER TABLE tasks ADD COLUMN budget_usd REAL; \
         ALTER TABLE tasks ADD COLUMN spent_usd REAL NOT NULL DEFAULT 0.0;",
    )
    .map_err(plugin_io_err("migrate budget_usd"))?;
}
```

- [ ] **Step 4: Update all row-reading code**

Find every place in tasks_db.rs that reads a `Task` from a row (search for `row.get` sequences that build a `Task`). Add the two new fields to each. They'll be at the end of the column list. Use `row.get::<_, Option<f64>>(N)?` for budget_usd and `row.get::<_, f64>(N).unwrap_or(0.0)` for spent_usd.

Also update the `create_task` function's INSERT statement to include `budget_usd` (take it as a new parameter).

Add `update_task_spent` function:

```rust
pub fn update_task_spent(&self, task_id: i64, spent_usd: f64) -> tau_agent_plugin::Result<()> {
    self.conn
        .execute(
            "UPDATE tasks SET spent_usd = ?1 WHERE id = ?2",
            params![spent_usd, task_id],
        )
        .map_err(plugin_io_err("update task spent_usd"))?;
    Ok(())
}
```

- [ ] **Step 5: Verify**

Run: `cargo check -p tau-agent-plugin-tasks`

- [ ] **Step 6: Commit**

```bash
git add crates/tau-agent-plugin-tasks/
git commit -S -m "feat(tasks): add budget_usd and spent_usd columns to tasks"
```

---

### Task 2: Add budget_usd to task_create tool

**Files:**
- Modify: `crates/tau-agent-plugin-tasks/src/tasks.rs`

- [ ] **Step 1: Add budget_usd to task_create tool definition**

In the `task_create` tool parameters (find the tool definition JSON), add:

```json
"budget_usd": {
    "type": "number",
    "description": "Optional budget cap in USD. Task worker sessions will be paused if accumulated cost exceeds this amount."
},
```

- [ ] **Step 2: Pass budget_usd through to create_task**

In `handle_task_create`, extract `budget_usd` from args and pass to the DB create call:

```rust
let budget_usd = args.get("budget_usd").and_then(|v| v.as_f64());
```

- [ ] **Step 3: Include budget in task display**

In the task_status/task_list display formatting, show budget info when present:

```rust
if let Some(budget) = task.budget_usd {
    // Show: budget: $X.XX / $Y.YY (Z%)
    let pct = if budget > 0.0 { (task.spent_usd / budget * 100.0) as u32 } else { 0 };
    format!("budget: ${:.2} / ${:.2} ({}%)", task.spent_usd, budget, pct)
}
```

- [ ] **Step 4: Verify**

Run: `cargo check -p tau-agent-plugin-tasks`

- [ ] **Step 5: Commit**

```bash
git add crates/tau-agent-plugin-tasks/
git commit -S -m "feat(tasks): add budget_usd to task_create tool"
```

---

### Task 3: Enhance stats CLI with agent costs

**Files:**
- Modify: `crates/tau-agent/src/main.rs`

- [ ] **Step 1: Extend cmd_project_stats**

In `cmd_project_stats`, after printing the existing project stats block, add an agent costs section:

```rust
// Agent costs
let mut client = tau_agent_lib::client::Client::connect_or_start().await?;
client.send(&tau_agent_lib::protocol::Request::ListAgents).await?;
client.recv_streaming(|resp| {
    if let tau_agent_lib::protocol::Response::Agents { agents } = resp {
        if !agents.is_empty() {
            println!("\nAgents:");
            for a in &agents {
                let budget_str = a.budget_usd
                    .map(|b| format!(" / ${:.2}", b))
                    .unwrap_or_default();
                let pct = a.budget_usd
                    .filter(|&b| b > 0.0)
                    .map(|b| format!(" ({}%)", (a.spent_usd / b * 100.0) as u32))
                    .unwrap_or_default();
                let status = if a.enabled { "active" } else { "paused" };
                println!(
                    "  #{} '{}' [{}]: ${:.4}{}{} ({})",
                    a.id, a.name, a.trigger_type, a.spent_usd, budget_str, pct, status,
                );
            }
        }
    }
}).await?;
```

Note: This requires making `cmd_project_stats` async since it now needs to connect to the server for agent data.

- [ ] **Step 2: Verify**

Run: `cargo check -p tau-agent`

- [ ] **Step 3: Commit**

```bash
git add crates/tau-agent/src/main.rs
git commit -S -m "feat(stats): show agent costs in project stats"
```

---

### Task 4: Full verification

- [ ] **Step 1:** `cargo build --workspace`
- [ ] **Step 2:** `cargo test --workspace`
- [ ] **Step 3:** `cargo clippy -p tau-agent-plugin-tasks -p tau-agent -- -D warnings`
