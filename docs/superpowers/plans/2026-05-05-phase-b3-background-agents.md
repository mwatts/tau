# Phase B3: Background Agents — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add background agent support — long-running or periodic agent sessions that persist across server restarts, defined via config files or created dynamically through the protocol.

**Architecture:** Agents are defined in `agents.toml` (project/global) or stored in a new `agents` DB table. The server loads agents on startup and registers them with `BgTaskScheduler`. New `BgTrigger::Persistent` variant handles long-lived sessions with restart backoff. Protocol additions mirror the schedule CRUD pattern. CLI subcommand `tau agent` provides management.

**Tech Stack:** Rust, rusqlite, serde/toml, smol (async), tau config_chain

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `crates/tau-agent-base/src/protocol.rs` | Add agent Request/Response variants + AgentInfo struct |
| Modify | `crates/tau-agent-client/src/lib.rs` | Add agent responses to is_terminal match |
| Modify | `crates/tau-agent-lib/src/db.rs` | Add agents table DDL + CRUD functions |
| Create | `crates/tau-agent-lib/src/server/agent_manager.rs` | Agent lifecycle: config loading, startup registration, persistent restart |
| Modify | `crates/tau-agent-lib/src/server/mod.rs` | Wire agent_manager into server startup |
| Modify | `crates/tau-agent-lib/src/server/dispatch.rs` | Handle CreateAgent/ListAgents/DeleteAgent/PauseAgent/ResumeAgent |
| Modify | `crates/tau-agent-lib/src/server/bg_tasks.rs` | Add BgTrigger::Persistent variant |
| Modify | `crates/tau-agent/src/main.rs` | Add tau agent CLI subcommand |

---

### Task 1: Protocol additions (AgentInfo struct + Request/Response variants)

**Files:**
- Modify: `crates/tau-agent-base/src/protocol.rs`
- Modify: `crates/tau-agent-client/src/lib.rs`

- [ ] **Step 1: Add AgentInfo struct**

After the `ScheduleInfo` struct (around line 949), add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub id: i64,
    pub name: String,
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub trigger_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_config: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_usd: Option<f64>,
    pub spent_usd: f64,
    pub created_at: i64,
}
```

- [ ] **Step 2: Add Request variants**

Before `Shutdown` in the `Request` enum (around line 364), add:

```rust
CreateAgent {
    name: String,
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    trigger_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_config: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_usd: Option<f64>,
},
ListAgents,
DeleteAgent { id: i64 },
PauseAgent { id: i64 },
ResumeAgent { id: i64 },
```

- [ ] **Step 3: Add Response variants**

After `ScheduleDeleted` in the `Response` enum (around line 561), add:

```rust
AgentCreated { id: i64 },
Agents { agents: Vec<AgentInfo> },
AgentDeleted,
AgentPaused,
AgentResumed,
```

- [ ] **Step 4: Update client is_terminal match**

In `crates/tau-agent-client/src/lib.rs`, add to the is_terminal match (after `ScheduleDeleted`):

```rust
| Response::AgentCreated { .. }
| Response::Agents { .. }
| Response::AgentDeleted
| Response::AgentPaused
| Response::AgentResumed
```

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p tau-agent-base -p tau-agent-client`

- [ ] **Step 6: Fix any exhaustive match issues**

The compiler will flag any other exhaustive matches on `Request` or `Response` that need the new variants. Common locations: `dispatch.rs`, `mcp_server.rs`, `tool_dispatch.rs`. Add `_ => {}` or proper handling for each.

- [ ] **Step 7: Commit**

```bash
git add crates/tau-agent-base/ crates/tau-agent-client/ crates/tau-agent-lib/
git commit -S -m "feat(agents): add agent protocol types (Request/Response/AgentInfo)"
```

---

### Task 2: Database layer (agents table + CRUD)

**Files:**
- Modify: `crates/tau-agent-lib/src/db.rs`

- [ ] **Step 1: Add agents table DDL**

In the `open()` function, after the schedules table creation block (around line 313), add:

```rust
let _ = conn.execute_batch(
    "CREATE TABLE IF NOT EXISTS agents (
        id             INTEGER PRIMARY KEY,
        name           TEXT NOT NULL UNIQUE,
        prompt         TEXT NOT NULL,
        model          TEXT,
        trigger_type   TEXT NOT NULL DEFAULT 'periodic',
        trigger_config TEXT,
        system_prompt  TEXT,
        project_name   TEXT,
        enabled        INTEGER NOT NULL DEFAULT 1,
        session_id     TEXT,
        budget_usd     REAL,
        spent_usd      REAL NOT NULL DEFAULT 0.0,
        created_at     INTEGER NOT NULL
    );",
);
```

- [ ] **Step 2: Add CRUD functions**

After the schedule CRUD functions (around line 1795), add:

```rust
pub fn create_agent(
    &self,
    name: &str,
    prompt: &str,
    model: Option<&str>,
    trigger_type: &str,
    trigger_config: Option<&str>,
    system_prompt: Option<&str>,
    project_name: Option<&str>,
    budget_usd: Option<f64>,
) -> crate::Result<i64> {
    let now = chrono::Utc::now().timestamp();
    self.conn
        .execute(
            "INSERT INTO agents (name, prompt, model, trigger_type, trigger_config, \
             system_prompt, project_name, budget_usd, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![name, prompt, model, trigger_type, trigger_config,
                              system_prompt, project_name, budget_usd, now],
        )
        .map_err(db_err("insert agent"))?;
    Ok(self.conn.last_insert_rowid())
}

pub fn list_agents(&self) -> crate::Result<Vec<tau_agent_base::protocol::AgentInfo>> {
    let mut stmt = self
        .conn
        .prepare(
            "SELECT id, name, prompt, model, trigger_type, trigger_config, \
             system_prompt, project_name, enabled, session_id, budget_usd, \
             spent_usd, created_at \
             FROM agents ORDER BY id",
        )
        .map_err(db_err("prepare list agents"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(tau_agent_base::protocol::AgentInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                prompt: row.get(2)?,
                model: row.get(3)?,
                trigger_type: row.get(4)?,
                trigger_config: row.get(5)?,
                system_prompt: row.get(6)?,
                project_name: row.get(7)?,
                enabled: row.get::<_, bool>(8)?,
                session_id: row.get(9)?,
                budget_usd: row.get(10)?,
                spent_usd: row.get::<_, f64>(11)?,
                created_at: row.get(12)?,
            })
        })
        .map_err(db_err("query agents"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(db_err("collect agents"))
}

pub fn delete_agent(&self, id: i64) -> crate::Result<bool> {
    let count = self
        .conn
        .execute("DELETE FROM agents WHERE id = ?1", [id])
        .map_err(db_err("delete agent"))?;
    Ok(count > 0)
}

pub fn set_agent_enabled(&self, id: i64, enabled: bool) -> crate::Result<bool> {
    let count = self
        .conn
        .execute(
            "UPDATE agents SET enabled = ?1 WHERE id = ?2",
            rusqlite::params![enabled, id],
        )
        .map_err(db_err("set agent enabled"))?;
    Ok(count > 0)
}

pub fn set_agent_session_id(&self, id: i64, session_id: Option<&str>) -> crate::Result<()> {
    self.conn
        .execute(
            "UPDATE agents SET session_id = ?1 WHERE id = ?2",
            rusqlite::params![session_id, id],
        )
        .map_err(db_err("set agent session_id"))?;
    Ok(())
}
```

- [ ] **Step 3: Verify**

Run: `cargo check -p tau-agent-lib`

- [ ] **Step 4: Commit**

```bash
git add crates/tau-agent-lib/src/db.rs
git commit -S -m "feat(agents): add agents table and CRUD functions to DB"
```

---

### Task 3: Server dispatch for agent CRUD

**Files:**
- Modify: `crates/tau-agent-lib/src/server/dispatch.rs`

- [ ] **Step 1: Add dispatch arms**

At the end of the `match req` block (after `DeleteSchedule` around line 2574), add:

```rust
crate::protocol::Request::CreateAgent {
    name, prompt, model, trigger_type, trigger_config,
    system_prompt, project_name, budget_usd,
} => {
    let resp = {
        let st = lock_state(&state);
        match st.db.create_agent(
            &name, &prompt, model.as_deref(), &trigger_type,
            trigger_config.as_deref(), system_prompt.as_deref(),
            project_name.as_deref(), budget_usd,
        ) {
            Ok(id) => crate::protocol::Response::AgentCreated { id },
            Err(e) => crate::protocol::Response::Error { message: format!("{}", e) },
        }
    };
    send(&mut writer, &resp).await?;
}
crate::protocol::Request::ListAgents => {
    let resp = {
        let st = lock_state(&state);
        match st.db.list_agents() {
            Ok(agents) => crate::protocol::Response::Agents { agents },
            Err(e) => crate::protocol::Response::Error { message: format!("{}", e) },
        }
    };
    send(&mut writer, &resp).await?;
}
crate::protocol::Request::DeleteAgent { id } => {
    let resp = {
        let st = lock_state(&state);
        match st.db.delete_agent(id) {
            Ok(true) => crate::protocol::Response::AgentDeleted,
            Ok(false) => crate::protocol::Response::Error {
                message: format!("agent {} not found", id),
            },
            Err(e) => crate::protocol::Response::Error { message: format!("{}", e) },
        }
    };
    send(&mut writer, &resp).await?;
}
crate::protocol::Request::PauseAgent { id } => {
    let resp = {
        let st = lock_state(&state);
        match st.db.set_agent_enabled(id, false) {
            Ok(true) => crate::protocol::Response::AgentPaused,
            Ok(false) => crate::protocol::Response::Error {
                message: format!("agent {} not found", id),
            },
            Err(e) => crate::protocol::Response::Error { message: format!("{}", e) },
        }
    };
    send(&mut writer, &resp).await?;
}
crate::protocol::Request::ResumeAgent { id } => {
    let resp = {
        let st = lock_state(&state);
        match st.db.set_agent_enabled(id, true) {
            Ok(true) => crate::protocol::Response::AgentResumed,
            Ok(false) => crate::protocol::Response::Error {
                message: format!("agent {} not found", id),
            },
            Err(e) => crate::protocol::Response::Error { message: format!("{}", e) },
        }
    };
    send(&mut writer, &resp).await?;
}
```

- [ ] **Step 2: Verify**

Run: `cargo check -p tau-agent-lib`

- [ ] **Step 3: Commit**

```bash
git add crates/tau-agent-lib/src/server/dispatch.rs
git commit -S -m "feat(agents): add agent CRUD dispatch handlers"
```

---

### Task 4: CLI subcommand `tau agent`

**Files:**
- Modify: `crates/tau-agent/src/main.rs`

- [ ] **Step 1: Add AgentAction enum and Commands::Agent variant**

Follow the exact pattern used for `ScheduleAction` (which we just added). Add `AgentAction` enum with: `List`, `Create { name, prompt, trigger, ... }`, `Delete { id }`, `Pause { id }`, `Resume { id }`.

Add `Commands::Agent` variant with `alias = "ag"`.

- [ ] **Step 2: Add dispatch arm and async functions**

Add `cmd_agent_list`, `cmd_agent_create`, `cmd_agent_delete`, `cmd_agent_pause`, `cmd_agent_resume` — all following the `cmd_schedule_*` pattern using `Client::connect_or_start()`.

- [ ] **Step 3: Verify**

Run: `cargo check -p tau-agent && cargo run -p tau-agent -- agent --help`

- [ ] **Step 4: Commit**

```bash
git add crates/tau-agent/src/main.rs
git commit -S -m "feat(agents): add tau agent CLI subcommand"
```

---

### Task 5: Full verification

- [ ] **Step 1:** `cargo build --workspace`
- [ ] **Step 2:** `cargo test --workspace`
- [ ] **Step 3:** `cargo clippy -p tau-agent-base -p tau-agent-lib -p tau-agent -- -D warnings`
