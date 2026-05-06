# Phase B1+B2: Task DAG Dependencies + Parallel Dispatch

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `depends_on` parameter to `task_create` so dependencies can be declared at creation time (completing the DAG infrastructure), and parallelize the task dispatch loop so multiple tasks spawn sessions concurrently.

**Architecture:** The DAG infrastructure already exists (`task_relations` table, `add_relation` with cycle detection, `get_schedulable_tasks` with dependency filtering, `WaitReason::Dependency`). B1 only adds a convenience parameter to `task_create`. B2 changes the dispatch loop in `tasks.rs` from serial to concurrent by collecting dispatch requests and issuing them in a batch via the server tunnel.

**Tech Stack:** Rust, rusqlite, serde_json, tau-agent-plugin (tunnel)

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `crates/tau-agent-plugin-tasks/src/tasks.rs:206-281` | Add `depends_on` param to `task_create` tool definition |
| Modify | `crates/tau-agent-plugin-tasks/src/tasks.rs:612-983` | Handle `depends_on` in `handle_task_create` — call `db.add_relation` after creation |
| Modify | `crates/tau-agent-plugin-tasks/src/tasks.rs:3679-3787` | Parallelize `run_schedule_pass` dispatch loop |
| Test | `crates/tau-agent-plugin-tasks/src/tasks_db.rs` (existing tests) | Add test for `depends_on` at creation |

---

### Task 1: Add `depends_on` parameter to `task_create`

**Files:**
- Modify: `crates/tau-agent-plugin-tasks/src/tasks.rs:206-281` (tool definition)
- Modify: `crates/tau-agent-plugin-tasks/src/tasks.rs:612-983` (handler)

- [ ] **Step 1: Add `depends_on` to the `task_create` PluginToolDef parameters**

In the `task_create` tool definition's `parameters.properties` JSON (around line 206), add after the `affected_files` property:

```rust
"depends_on": {
    "type": "array",
    "items": { "type": "integer" },
    "description": "Task IDs that this task depends on. The task will not be scheduled until all dependencies reach 'merged' or 'closed' state. Cycle detection prevents circular dependencies."
},
```

Do NOT add it to the `"required"` array — it's optional.

- [ ] **Step 2: Handle `depends_on` in `handle_task_create`**

In the `handle_task_create` function (around line 612), after the task is created (after `db.create_task(...)` returns the new `Task`), add dependency wiring:

```rust
// Wire dependencies if specified
if let Some(deps) = args.get("depends_on").and_then(|v| v.as_array()) {
    for dep_val in deps {
        if let Some(dep_id) = dep_val.as_i64() {
            if let Err(e) = db.add_relation(task.id, dep_id, "depends_on") {
                return ToolResultMessage::error(
                    tcid,
                    "",
                    &format!(
                        "task {} created but failed to add dependency on {}: {}",
                        task.id, dep_id, e
                    ),
                );
            }
        }
    }
}
```

Find the exact location: search for where `db.create_task(` returns a `Task` and the subsequent code that handles post-creation work (placeholder session, etc.). Insert the dependency wiring right after `create_task` returns successfully but before any scheduler wake-up.

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p tau-agent-plugin-tasks`

- [ ] **Step 4: Commit**

```bash
git add crates/tau-agent-plugin-tasks/src/tasks.rs
git commit -S -m "feat(tasks): add depends_on parameter to task_create tool"
```

---

### Task 2: Parallelize task dispatch loop

**Files:**
- Modify: `crates/tau-agent-plugin-tasks/src/tasks.rs:3679-3787` (the `run_schedule_pass` function)

The current dispatch loop in `run_schedule_pass` is:
```rust
for st in &scheduled {
    tasks_scheduler::dispatch(db, st.id, ...);  // blocking, one at a time
}
```

The dispatch is serial because the plugin uses synchronous stdin/stdout I/O. Each `dispatch()` call sends a `ServerRequest` over the writer and waits for the response on the reader.

- [ ] **Step 1: Understand the constraint**

The tasks plugin is a synchronous subprocess using `BufRead`/`Write` for its server tunnel. It cannot do true async parallelism. However, the server handles session creation asynchronously — the bottleneck is the serial request-response round trip in the plugin.

The simplest way to "parallelize" dispatch is to make it fire-and-forget: send all `CreateSession` + `Chat` requests without waiting for responses, then collect responses after. But the current `server_request` in `tunnel.rs` (sync version) blocks on reading the response.

Alternative approach: batch the dispatch by sending all requests with unique IDs, then reading all responses. But the sync tunnel doesn't support this.

**Recommended approach:** Since the server already spawns each session asynchronously (via `smol::spawn` in `schedule_runner`), the real fix is minimal: the dispatch function already creates sessions that run independently. The serial overhead is just the CreateSession + Chat round-trips (~ms each). For 4-8 tasks this is negligible.

Instead of restructuring the tunnel, add a `max_parallel` config to `checklist.toml` and expose it as the configurable cap (replacing the hardcoded `MAX_CONCURRENT_TASKS = 8`).

- [ ] **Step 2: Make `MAX_CONCURRENT_TASKS` configurable**

In `crates/tau-agent-plugin-tasks/src/tasks_scheduler.rs`, the constant is:
```rust
pub(crate) const MAX_CONCURRENT_TASKS: usize = 8;
```

Change the scheduler to accept `max_parallel` as a parameter. In the `Checklist` config struct (in `tasks.rs` or wherever `checklist.toml` is parsed), add a `max_parallel: Option<usize>` field that defaults to 8.

Find where `Checklist` is defined and where `MAX_CONCURRENT_TASKS` is used, then thread the configurable value through.

- [ ] **Step 3: Update `schedule()` to accept `max_parallel`**

Change `schedule()` signature to accept `max_concurrent: usize` instead of using the constant. Update callers.

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo test -p tau-agent-plugin-tasks`

- [ ] **Step 5: Commit**

```bash
git add crates/tau-agent-plugin-tasks/
git commit -S -m "feat(tasks): make max_parallel configurable via checklist.toml"
```

---

### Task 3: Verify full build

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`

- [ ] **Step 2: Run all tests**

Run: `cargo test --workspace`

- [ ] **Step 3: Clippy**

Run: `cargo clippy -p tau-agent-plugin-tasks -- -D warnings`
