# NL Scheduled Tasks, Multi-Agent Orchestration, and Web UI

Design spec for three features that close tau's remaining gaps against Hermes Agent
and complete ROADMAP Phase 5 + Phase 6 (Web UI).

Single spec, phased implementation: A (schedules) → B (orchestration) → C (web UI).

---

## 1. NL Scheduled Tasks

### Goal

Let users create cron schedules using natural language ("every weekday at 9:30am")
instead of raw cron expressions. The existing `CreateSchedule` protocol, `schedules`
DB table, and `schedule_runner.rs` stay unchanged.

### Approach: LLM-Based

The agent itself interprets NL and emits `CreateSchedule` with the correct cron
expression. No new parsing crate — the LLM already understands cron semantics.

### Changes

#### 1.1 `schedule` tool (worker plugin)

New tool in `tau-agent-plugin-worker/src/orchestration.rs` with three actions:

- **`create`** — params: `name` (string), `when` (NL string), `prompt` (string),
  `model` (optional), `cwd` (optional). The LLM interprets `when` as part of its
  normal reasoning (before calling the tool), converts it to a 5-field cron
  expression, and confirms with the user ("I'll set this to `30 9 * * 1-5` —
  every weekday at 9:30 AM. OK?"). After confirmation, the LLM calls the `schedule`
  tool with the resolved cron expression. The tool itself passes `when` through to
  `CreateSchedule.cron_expr` — it does no NL parsing. The prompt guidelines on the
  tool instruct the LLM to perform the translation and confirmation steps.
- **`list`** — calls `ListSchedules` via tunnel, returns formatted table.
- **`delete`** — calls `DeleteSchedule` by name or id via tunnel.

Prompt guidelines:
- Always confirm the interpreted cron expression before creating.
- Show the next 3 fire times so the user can verify.
- Warn if the interval is very frequent (< 5 minutes).

#### 1.2 `tau schedule` CLI subcommand

Thin wrapper over the protocol for headless management:

```
tau schedule list
tau schedule create --name "nightly-tests" --cron "0 2 * * *" --prompt "Run full test suite"
tau schedule delete --id 3
tau schedule delete --name "nightly-tests"
```

NL parsing not available in headless mode — pass raw cron expressions.

#### 1.3 What stays unchanged

- `schedule_runner.rs` — 60s tick, fire-and-forget sessions
- `db.rs` schedules table schema
- `CreateSchedule` / `ListSchedules` / `DeleteSchedule` protocol variants
- `ScheduleInfo` wire type

---

## 2. Multi-Agent Orchestration

### Goal

Evolve tau's session system to support background agents, parallel task dispatch,
task DAG dependencies, a supervisor agent, and per-task/per-agent budget tracking.
This completes ROADMAP Phase 5.

### 2.1 Background Agents

Long-running or periodic agent sessions that persist across server restarts and
react to events autonomously.

#### Config layer

`.tau/agents.toml` (project-scoped) and `~/.config/tau/agents.toml` (global):

```toml
[agents.ci-watcher]
prompt = "Monitor CI status for this repo. When a build fails, investigate and suggest fixes."
model = "light"
trigger = "periodic"
interval = "5m"
enabled = true

[agents.pr-reviewer]
prompt = "Review new PRs on this repo. Post inline comments."
trigger = "periodic"
interval = "15m"

[agents.supervisor]
prompt = "You are a project supervisor. Watch the task board, assign ready tasks, review completed work."
trigger = "persistent"
role = "supervisor"
```

Supported trigger types:
- `periodic` — run prompt on interval, session created and destroyed each cycle
- `cron` — run on cron schedule (delegates to existing schedule_runner)
- `persistent` — long-lived session, re-spawned if it exits
- `on-demand` — registered but only started via explicit `tau agent start <name>`

#### DB layer

New `agents` table in `tau.db`:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `name` | TEXT UNIQUE | Agent identifier |
| `prompt` | TEXT | Initial/recurring prompt |
| `model` | TEXT | Model ID (nullable, defaults to server default) |
| `trigger_type` | TEXT | periodic / cron / persistent / on-demand |
| `trigger_config` | TEXT | JSON — `{"interval_secs": 300}` or `{"cron_expr": "0 9 * * *"}` |
| `system_prompt` | TEXT | Optional system prompt override |
| `project_name` | TEXT | Associated project (nullable for global agents) |
| `enabled` | BOOLEAN | Active flag |
| `session_id` | TEXT | Current live session ID (nullable) |
| `budget_usd` | REAL | Cost cap (nullable, no limit if null) |
| `spent_usd` | REAL | Accumulated cost |
| `created_at` | INTEGER | Unix timestamp |

#### Protocol additions

```rust
CreateAgent {
    name: String,
    prompt: String,
    model: Option<String>,
    trigger_type: String,       // "periodic" | "cron" | "persistent" | "on-demand"
    trigger_config: Option<String>, // JSON
    system_prompt: Option<String>,
    project_name: Option<String>,
    budget_usd: Option<f64>,
}
ListAgents
DeleteAgent { id: i64 }
PauseAgent { id: i64 }
ResumeAgent { id: i64 }
StartAgent { id: i64 }  // for on-demand agents
```

Responses: `AgentCreated { id }`, `AgentList { agents: Vec<AgentInfo> }`,
`AgentDeleted`, `AgentPaused`, `AgentResumed`, `AgentStarted`.

#### Runtime: `server/agent_manager.rs`

New module responsible for agent lifecycle:

- **Startup**: load agents from config files + DB. For each enabled agent, register
  with `BgTaskScheduler` using the appropriate trigger.
- **`BgTrigger::Persistent`** (new variant): spawns a session that stays alive.
  If the session exits (agent loop completes), re-spawn after a 30s backoff.
  Session receives periodic heartbeat messages with elapsed time and event
  summaries. Max restart count configurable (default 10, then pause with error).
- **Periodic agents**: reuse existing `BgTrigger::Periodic`. Each tick creates a
  new session, runs the prompt, session exits naturally.
- **Cron agents**: delegate to existing `schedule_runner` by creating a schedule
  entry on startup.
- **Session tagging**: agent sessions get `is_agent = true` flag in the sessions
  table so they're distinguishable from user sessions in listings and the Web UI.

#### CLI: `tau agent`

```
tau agent list                      # list all agents with status
tau agent create --name "..." ...   # create DB-backed agent
tau agent delete <name|id>
tau agent pause <name|id>
tau agent resume <name|id>
tau agent start <name|id>           # start on-demand agent
tau agent logs <name|id>            # tail agent session output
```

### 2.2 Parallel Task Dispatch

The scheduler in `tasks_scheduler.rs` already selects a conflict-free batch of
ready tasks. Today `dispatch()` processes them serially. Changes:

- `dispatch()` spawns all batch sessions concurrently via `smol::spawn`
- New config: `max_parallel` in `.tau/checklist.toml` (default 4) — caps
  concurrent worker sessions
- `State` gains `active_task_workers: HashSet<String>` to track in-flight
  task sessions and enforce the cap
- Scheduler respects the cap: `batch.truncate(max_parallel - active_count)`

### 2.3 Task DAG Dependencies

New `task_deps` table in `tasks.db`:

| Column | Type | Description |
|--------|------|-------------|
| `task_id` | TEXT | The dependent task |
| `depends_on` | TEXT | The prerequisite task |

Primary key: `(task_id, depends_on)`.

Changes to task system:
- `task_create` tool gains `depends_on: Vec<String>` parameter
- On insertion, run cycle detection (DFS from the new task through existing edges).
  Reject with error if cycle detected.
- `tasks_scheduler::schedule()` adds dependency filter: a task is only eligible if
  all its `depends_on` tasks are in terminal state (`Merged` or `Closed`)
- `task status` output shows dependency edges
- `SkipReason::UnmetDependencies { blocked_by: Vec<String> }` added to scheduler
  skip reasons

### 2.4 Supervisor Agent

Three tiers, each building on the previous:

#### Tier 1: `task_decompose` tool (implicit)

New tool in worker plugin. Any session can call it.

Parameters:
- `spec` — text content or file path of the specification
- `project_name` — target project
- `strategy` — "sequential" | "parallel" | "auto" (default "auto")

Behavior:
1. If `spec` is a file path, read the file
2. Call the LLM (via a child session with a decomposition system prompt) to break
   the spec into sub-tasks with title, description, affected_files estimate,
   priority, and dependency edges
3. Create all tasks via `task_create` with DAG edges from `depends_on`
4. Return summary: task count, dependency graph, estimated parallel depth

#### Tier 2: `tau supervise` command (explicit)

CLI command that creates a supervisor session with a specialized system prompt:

```
tau supervise spec.md
tau supervise --project myproject < requirements.txt
```

The supervisor session:
1. Reads the spec
2. Calls `task_decompose` to create the task DAG
3. Enters a monitoring loop: polls `task_status` periodically
4. Reviews completed tasks (reads diffs via `session_read` on worker sessions,
   optionally runs validation commands)
5. Approves tasks that pass review, sends revision requests for those that don't
6. Manages merge ordering to respect DAG dependencies

System prompt emphasizes: break large specs into small reviewable tasks, set
appropriate budgets, prefer parallel work where files don't conflict.

#### Tier 3: Background supervisor (daemon)

A background agent (from 2.1) with `trigger = "persistent"` and `role = "supervisor"`.
Watches the task board continuously:

- Auto-assigns ready tasks to available worker slots
- Reviews completed work
- Creates follow-up tasks when issues are found
- Reports status summaries on configurable intervals

Created via config or CLI:
```
tau agent create --name supervisor --trigger persistent --role supervisor \
  --prompt "Supervise the tau project task board..."
```

### 2.5 Budget & Cost Tracking

Extend existing cost infrastructure:

- **Per-task budget**: `budget_usd` field on tasks table. Worker sessions check
  accumulated cost against budget each turn. If exceeded, session saves progress
  and exits with a budget-exceeded status.
- **Per-agent budget**: `budget_usd` / `spent_usd` on agents table (see 2.1).
  Agent pauses when `spent_usd >= budget_usd`.
- **Aggregate reporting**: `tau stats` CLI command shows:
  - Per-project totals (tokens in/out, estimated cost)
  - Per-agent costs (lifetime, last 24h, last 7d)
  - Per-task costs
  - Budget utilization (% used of allocated budgets)

---

## 3. Web UI

### Goal

Optional browser-based client alongside the TUI, started via `tau serve --web`.
Vanilla HTML+JS+CSS SPA with no build toolchain. Connects to the daemon via
the existing Unix socket protocol, bridged through a WebSocket.

### Architecture

```
Browser ──WebSocket──► tau-agent-web ──Unix socket──► tau daemon
         ◄─────────── (axum server)  ◄──────────────
```

`tau-agent-web` is a separate process that acts as a client of the daemon.
The daemon requires zero modifications for web support.

### New crate: `tau-agent-web`

#### Dependencies

| Crate | Purpose |
|-------|---------|
| `axum` | HTTP server + WebSocket upgrade |
| `tokio` | Async runtime (required by axum) |
| `tower-http` | Static file serving, CORS |
| `tau-agent-client` | Unix socket client library |
| `rust-embed` | Embed static assets into binary |
| `serde_json` | JSON serialization |
| `uuid` | Request correlation IDs |

#### Source layout

```
crates/tau-agent-web/
├── Cargo.toml
├── src/
│   ├── main.rs          # CLI args, config, startup
│   ├── ws_bridge.rs     # WebSocket ↔ Unix socket bridge
│   └── routes.rs        # HTTP routes (static files, health, REST API)
└── assets/
    ├── index.html        # SPA shell
    ├── app.js            # Entry point, router, WebSocket manager
    ├── style.css         # All styling
    └── components/
        ├── session-list.js
        ├── chat-view.js
        ├── task-board.js
        ├── schedule-list.js
        └── agent-status.js
```

### WebSocket Protocol

Client connects to `ws://localhost:8080/ws?token=<auth-token>`.

Messages map 1:1 to the existing `Request`/`Response` protocol with a session
multiplexing wrapper:

```json
// Client → Server
{
  "request_id": "r-1",
  "session_id": "s-abc",
  "request": {"Chat": {"content": "fix the bug", "attachments": []}}
}

// Server → Client (streaming events tagged with session)
{
  "request_id": "r-1",
  "session_id": "s-abc",
  "event": {"OutputDelta": {"content": "Looking at the code..."}}
}

// Server → Client (non-session responses)
{
  "request_id": "r-2",
  "event": {"ScheduleList": {"schedules": [...]}}
}
```

The web server maintains one Unix socket connection to the daemon per browser
WebSocket. Each WebSocket message is forwarded as a protocol `Request`; responses
stream back as events.

### Frontend Views

All vanilla JS. No framework, no build step. Markdown rendering via inlined
`marked.js` (~40KB).

#### Session List (sidebar)

- Active sessions with status indicator (idle/running/agent)
- Parent-child tree indentation for session hierarchies
- Badge for unread output on non-focused sessions
- "New Session" button
- Filter: user sessions / agent sessions / all

#### Chat View (main panel)

- Streaming message display with markdown rendering
- Tool calls shown as collapsible cards (name, input, output)
- User input textarea with submit (Enter) and newline (Shift+Enter)
- Session info header: model, session ID, parent, project
- Cancel button (sends cancel request via WebSocket)

#### Task Board

- Kanban columns: Planning → Ready → Active → Review → Merging → Merged
- Task cards: title, priority badge, assignee session (clickable), branch name
- Click card for detail panel: description, affected files, dependency graph,
  budget/cost, worker session link
- DAG visualization: simple left-to-right dependency arrows using CSS/SVG

#### Schedule List

- Table: name, cron expression, human-readable description, next run time,
  last run time, enabled toggle
- Create form: name, when (NL or cron), prompt, model
- Delete button with confirmation

#### Agent Status

- List: name, trigger type, status (running/paused/errored/on-demand),
  current session (clickable), last activity timestamp, cost (budget used/total)
- Controls: pause/resume/delete/start (for on-demand)
- Click agent name to expand: recent log output from agent session

### Security

- **Localhost only** by default — binds `127.0.0.1:8080`
- Optional `--bind 0.0.0.0:8080` for LAN access
- **Auth token**: on startup, generate random 32-byte hex token, write to
  `~/.config/tau/web-token`. Browser sends as `?token=` query param on WebSocket
  connect and as cookie for HTTP requests. Reject connections without valid token.
- No TLS in v1. For remote access, document nginx/caddy reverse proxy with TLS
  termination.

### CLI integration

```
tau serve --web                    # start web server on :8080
tau serve --web --port 3000        # custom port
tau serve --web --bind 0.0.0.0     # LAN access
```

The web server auto-discovers the daemon's Unix socket path (same logic as
`tau-agent-client`).

---

## 4. Implementation Phases

All phases build on `impl-mcp` branch. Each phase is a PR.

### Phase A: NL Scheduled Tasks (~1-2 days)

1. Add `schedule` tool definition to `orchestration.rs`
2. Implement tool execution in worker plugin (tunnel to CreateSchedule/ListSchedules/DeleteSchedule)
3. Add `tau schedule` CLI subcommand to `main.rs`
4. Tests: tool definition schema validation, CLI integration

### Phase B: Multi-Agent Orchestration

#### B1: Task DAG Dependencies (~2 days)
1. Add `task_deps` table to tasks DB, migration
2. Extend `task_create` tool with `depends_on` parameter
3. Cycle detection on insertion
4. Scheduler dependency filter
5. `SkipReason::UnmetDependencies`
6. Tests: cycle detection, scheduler filtering, terminal-state gate

#### B2: Parallel Task Dispatch (~1 day)
1. `max_parallel` config in `checklist.toml`
2. `active_task_workers` tracking in State
3. Concurrent `smol::spawn` in dispatch
4. Batch truncation against cap
5. Tests: cap enforcement, concurrent dispatch

#### B3: Background Agents (~3-4 days)
1. `agents` table in `tau.db`, migration
2. `agents.toml` config loader (project + global)
3. Protocol additions: CreateAgent, ListAgents, DeleteAgent, PauseAgent, ResumeAgent, StartAgent
4. `BgTrigger::Persistent` variant in bg_tasks.rs
5. `agent_manager.rs`: startup loading, lifecycle management, restart backoff
6. `is_agent` flag on sessions table
7. `tau agent` CLI subcommand
8. Tests: config loading, lifecycle transitions, restart backoff

#### B4: Supervisor Agent (~2 days)
1. `task_decompose` tool definition and execution
2. Supervisor system prompt (built-in skill or hardcoded)
3. `tau supervise` CLI command
4. Supervisor role recognition in agent_manager for Tier 3
5. Tests: decomposition output schema, supervisor session creation

#### B5: Budget & Cost Tracking (~1-2 days)
1. `budget_usd` field on tasks table
2. Per-turn budget check in agent loop
3. `spent_usd` tracking on agents table
4. `tau stats` CLI command
5. Tests: budget enforcement, stats aggregation

### Phase C: Web UI (~4-5 days)

#### C1: Scaffold (~1 day)
1. Create `tau-agent-web` crate with axum
2. Static asset embedding with rust-embed
3. Health check endpoint
4. `tau serve --web` CLI integration
5. Auth token generation and validation

#### C2: WebSocket Bridge (~1 day)
1. WebSocket upgrade handler
2. Unix socket client connection per WebSocket
3. Request forwarding and response streaming
4. Session multiplexing wrapper
5. Tests: bridge integration, auth rejection

#### C3: Session List + Chat View (~1-2 days)
1. `index.html` SPA shell
2. `session-list.js`: fetch sessions, render sidebar, new session
3. `chat-view.js`: streaming display, tool call cards, user input
4. `style.css`: layout, dark theme (match TUI aesthetic)
5. Markdown rendering with marked.js

#### C4: Task Board + Remaining Views (~1-2 days)
1. `task-board.js`: kanban columns, task cards, detail panel
2. `schedule-list.js`: table, create form, enable/disable
3. `agent-status.js`: agent list, controls, log preview
4. DAG dependency visualization (CSS/SVG arrows)

---

## 5. Data Flow Summary

```
User (browser/TUI/CLI)
  │
  ├─► "schedule every morning at 9am: run tests"
  │     └─► LLM interprets → CreateSchedule{cron_expr: "0 9 * * *", ...}
  │           └─► schedule_runner fires at 9am → new session → runs prompt
  │
  ├─► "tau supervise spec.md"
  │     └─► supervisor session → task_decompose → creates task DAG
  │           └─► scheduler picks ready tasks (deps met, no file conflicts)
  │                 └─► parallel dispatch (up to max_parallel workers)
  │                       └─► workers complete → supervisor reviews → approve/revise
  │                             └─► merge queue → merged
  │
  └─► browser → WebSocket → tau-agent-web → Unix socket → daemon
        └─► same Request/Response protocol, real-time streaming
```

## 6. Open Questions (deferred to implementation)

- **Heartbeat content for persistent agents**: what context should heartbeat
  messages include? Git log since last heartbeat? Task board diff? TBD during B3.
- **Agent-to-agent messaging**: sessions can already send messages via
  `session_message`. Whether background agents need additional addressing
  (by agent name rather than session ID) can be decided during B3.
- **Web UI authentication UX**: the token-in-URL approach works but is
  clunky. A login page with token input would be smoother — defer to C1.
- **DAG visualization library**: simple CSS arrows may not scale for large
  graphs. Evaluate during C4 whether a small lib (e.g. dagre-d3, ~30KB) is
  warranted.
