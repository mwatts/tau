# tau Roadmap

Phased plan to close feature gaps against the emerging AI agent ecosystem
(Claude Code, Warp/Oz, Codex CLI) while preserving tau's architectural
advantages: MIT license, self-contained operation, Unix daemon model, and
the task board + merge queue.

---

## Completed

### Phase 1: Security & Publish (partial)

- [x] Fix `SYS_close_range` macOS build (`#[cfg(target_os = "linux")]` guard)
- [x] Remove local-path myelin dependency (now `version = "0.1"` from crates.io)

### Phase 2: MCP Support

- [x] Add `rmcp` crate (MIT) as workspace dependency
- [x] Config: `~/.config/tau/mcp.toml` + project `.tau/mcp.toml` (global tier only for security)
- [x] MCP server lifecycle manager (spawn, health-check, restart)
- [x] Surface MCP tools in the LLM tool list alongside built-in tools
- [x] Surface MCP resources as attachable context
- [x] Expose tau's built-in tools (bash, read, write, edit, diagnostics) as MCP tools
- [x] Expose session/task management as MCP resources
- [x] stdio transport (for integration with other agents/editors)

### Phase 3: Skills System

- [x] Skill format: markdown files in `.tau/skills/` and `.agents/skills/` (project) and `~/.config/tau/skills/` (global)
- [x] Skill loading: inject matching skills into system prompt context
- [x] Skill matching: by name (explicit `/skill-name`) or by trigger pattern (file type, keyword)
- [x] Built-in skills: conventional commits, PR creation, code review, TDD
- [x] Skill discovery: `/skills` command lists available skills

### Self-Improvement (SI-1, SI-2, SI-3)

- [x] SI-1: Skill auto-generation — `tasks_skill_autogen.rs` hooks both merge paths, spawns light-model child to extract skills into `.tau/skills/auto/`
- [x] SI-2: Cross-session recall — FTS5 virtual table in `db.rs`, `session_search` tool with project-scoped filtering
- [x] SI-3: Agent-writable memory — `memory.rs` with add/replace/remove/list, bounded capacity (2200 project / 1400 global), injected into system prompt

---

## Remaining Work

### Phase 1: Security & Publish (remaining)

- [ ] Upstream security fixes (shell-escape, path validation, socket perms, plugin auth, prompt append-only, OAuth peer check)
- [ ] Publish patched `tau-agent 0.1.1` to crates.io
- [x] `cargo audit` clean (rustls-webpki 0.103.13, rand 0.10.1)

### Phase 2: MCP (remaining)

- [x] MCP prompts as slash commands (`/prompt` list + invoke)
- [ ] HTTP+SSE transport for MCP server (web integrations)

### SI-4: Meta-Cognitive Skill Patching

- [x] Extend skill tool with `patch` action (`skill_patch` tool: list/read/patch/delete)
- [x] Auto-generated skills patchable freely; hand-written skills require confirmation
- [x] Skills in `.tau/skills/` are git-tracked — natural audit trail

### Phase 4: External Agent Harness

Dispatch task work to external agent harnesses (Claude Code, Codex CLI,
Gemini CLI) as an alternative to tau's built-in worker.

- [ ] Agent harness abstraction: `trait ExternalAgent { fn spawn(...), fn send(...), fn poll(...) }`
- [ ] Claude Code integration (subprocess, stdin/stdout protocol)
- [ ] Codex CLI integration
- [ ] Gemini CLI integration
- [ ] Task assignment: manual (`/task assign --agent claude-code`) or rule-based
- [ ] Output capture: external agent results flow back into tau's session history
- [ ] Merge queue integration: external agent work in worktrees, same merge flow

### Phase 5: Multi-Agent Orchestration

Evolve the task system to support ambient/background agents and parallel
orchestration beyond the current serial dispatch model.

- [ ] Background agents: long-running agents that monitor and react (CI watcher, PR reviewer, dependency updater)
- [ ] Agent-to-agent messaging: sessions can send structured messages to other sessions (already partially implemented via `QueueMessage`)
- [ ] Parallel task execution: dispatch N tasks simultaneously across N worktrees with resource-aware scheduling
- [ ] Task dependencies: DAG-based task ordering (task B blocked on task A)
- [ ] Budget and cost tracking: per-task and per-session token/cost accounting with configurable limits
- [ ] Supervisor agent: meta-agent that breaks specs into tasks, assigns to workers, reviews results, manages the merge queue

### Phase 6: Developer Experience (ongoing)

- [ ] First-run wizard (`tau init` — configure provider, API key, project)
- [ ] Remote mode: `tau serve --ssh` (tau as an SSH-accessible agent server)
- [ ] Session templates: pre-configured system prompts + tool sets for common workflows
- [ ] Conversation export: markdown/JSON export of session transcripts
- [ ] Web UI: optional browser-based client alongside the TUI (via WebSocket to the existing daemon)
- [ ] Computer-use tool: screenshot + click automation for GUI testing workflows
- [ ] Windows support: replace Unix socket with named pipes, gate nix-specific code

---

## Feature Map: Tau vs Hermes Agent

Comparative analysis against [Hermes Agent](https://github.com/NousResearch/hermes-agent)
by Nous Research. Hermes is an open-source, self-improving AI agent framework
(MIT, 130k+ GitHub stars) built around a closed learning loop: the agent
accumulates procedural knowledge, builds a persistent user model, and improves
its own skills autonomously over time without GPU training.

### Where Tau Leads

| Area | Tau | Hermes |
|---|---|---|
| Task board + merge queue | Full lifecycle (planning → review → merge), DAG deps, file-conflict-aware scheduling, checklist-gated fast-forward merge | `todo` tool (in-session only), no merge automation |
| Multi-agent primitives | Session tree with spawn/join/message, persistent hierarchy, blocking await-reply | `delegate_task` ephemeral children, max depth 3, non-durable |
| Anti-stuck detection | Loop-review checkpoint every N turns, separate reviewer model, nudge injection | Not documented |
| Cost tracking | Per-session + project-wide + subscription utilization with 5h/7d buckets | Per-session tokens/cost only |
| Git worktree isolation | Per-task worktrees with branch management | Filesystem snapshots + `/rollback` |
| Daemon architecture | Long-lived Unix daemon, attach/detach, multi-client, hot-reload config | Single-process CLI or gateway |

### Where Hermes Still Leads

| Area | Hermes | Tau |
|---|---|---|
| Scheduled automation | Built-in cron with natural-language spec, multi-platform delivery | ~~Not built-in~~ Done — cron schedules via protocol |
| Sandbox backends | 7 backends (local, Docker, SSH, Modal, Daytona, Vercel, Singularity) | `sandbox.toml` prefix (Docker, SSH) |
| Platform adapters | 21 messaging platforms (Telegram, Discord, Slack, WhatsApp, etc.) | TUI + Unix socket only |
| Smart approval | LLM-assessed risk: auto-approve low-risk, escalate uncertain, deny dangerous | ~~User permission mode only~~ Done — static risk gate blocks dangerous ops |
| Web UI | Ships with web mode | Planned (Phase 6) |
| Provider breadth | 200+ via OpenRouter + custom endpoints | ~~Anthropic native + OpenAI-compat~~ Done — OpenRouter + Bedrock + OpenAI-compat |

### Now Comparable (previously Hermes-only)

| Area | Status |
|---|---|
| MCP integration | Done — full client + server, stdio transport |
| Skills system | Done — 4 built-in, auto-gen from merged tasks, trigger matching |
| Self-improvement | Done — skill auto-gen, FTS5 recall, bounded memory |
| Agent-writable memory | Done — project + global memory, bounded curation |
| Cross-session recall | Done — `session_search` tool with FTS5, project-scoped |

### Roughly Comparable

| Area | Notes |
|---|---|
| Core agent loop | Both streaming, tool dispatch. Hermes broader (61 tools), tau more focused (~12) |
| Context compaction | Both auto-trigger on context limit. Tau uses structured format (Goal/Progress/Decisions/Next). Hermes tracks compression lineage |
| Plugin system | Both process-based. Tau is language-agnostic (JSON-lines). Hermes is Python-native |
| Session persistence | Both SQLite-backed |
| LLM provider support | Both multi-provider. Hermes wider via OpenRouter; tau deeper Anthropic integration |
| Checkpoints | Hermes: filesystem snapshots. Tau: git worktrees. Different strategies, both valid |

### Out of Scope

| Hermes Feature | Why Skip |
|---|---|
| Platform adapters (Telegram, Discord, etc.) | Different design philosophy — tau is a dev tool, not a chatbot platform |
| RL training pipeline (Atropos/GRPO) | Research infrastructure, not agent runtime |
| DSPy+GEPA prompt optimization | ~~Deferred~~ Done — native prompt optimizer with per-project metrics, auto-revert, and global promotion |
| Honcho dialectic user modeling | Over-engineered for a coding agent |
| `execute_code` RPC tool | Tau's session_spawn + bash covers this pattern differently |
| Human delay simulation | Not relevant |

---

## Non-Goals

- **Terminal emulation** — tau runs inside a terminal, it doesn't replace one
- **GPU-rendered UI** — TUI portability (SSH, tmux, any terminal) is a feature
- **Cloud dependency** — self-contained operation is a core principle
- **Proprietary backend** — MIT everything, no phone-home
