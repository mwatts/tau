# tau Roadmap

Phased plan to close feature gaps against the emerging AI agent ecosystem
(Claude Code, Warp/Oz, Codex CLI) while preserving tau's architectural
advantages: MIT license, self-contained operation, Unix daemon model, and
the task board + merge queue.

---

## Phase 1: Security & Publish (1-2 weeks)

Land the security hardening from the audit and unblock clean crates.io install.

- [ ] Upstream security fixes (shell-escape, path validation, socket perms, plugin auth, diagnostics tier, prompt append-only, OAuth peer check)
- [ ] Fix `SYS_close_range` macOS build (already done, needs publish)
- [ ] Remove local-path myelin dependency (already done)
- [ ] Publish patched `tau-agent 0.1.1` to crates.io
- [ ] `cargo audit` clean (rustls-webpki updated)

---

## Phase 2: MCP Support (2-4 weeks)

MCP (Model Context Protocol) is the de-facto standard for extending AI agents
with external tools and context. This is the single highest-leverage gap.

### 2a: MCP Client (tau calls MCP servers)

- [x] Add `rmcp` crate (MIT) as workspace dependency
- [x] Config: `~/.config/tau/mcp.toml` + project `.tau/mcp.toml` (global tier only for security)
- [x] MCP server lifecycle manager (spawn, health-check, restart)
- [x] Surface MCP tools in the LLM tool list alongside built-in tools
- [x] Surface MCP resources as attachable context
- [ ] MCP prompts as slash commands

### 2b: MCP Server (tau exposes itself)

- [x] Expose tau's built-in tools (bash, read, write, edit, diagnostics) as MCP tools
- [x] Expose session/task management as MCP resources
- [x] stdio transport (for integration with other agents/editors)
- [ ] HTTP+SSE transport (for web integrations)

---

## Phase 3: Skills System (1-2 weeks)

Markdown-defined agent behaviors, loadable per-project or globally.
Lightweight alternative to full plugins for customizing agent behavior.

- [x] Skill format: markdown files in `.tau/skills/` (project) and `~/.config/tau/skills/` (global)
- [x] Skill loading: inject matching skills into system prompt context
- [x] Skill matching: by name (explicit `/skill-name`) or by trigger pattern (file type, keyword)
- [x] Built-in skills: conventional commits, PR creation, code review, TDD
- [x] Skill discovery: `/skills` command lists available skills

---

## Phase 4: External Agent Harness (3-4 weeks)

Dispatch task work to external agent harnesses (Claude Code, Codex CLI,
Gemini CLI) as an alternative to tau's built-in worker. Enables using
specialized agents for specific task types.

- [ ] Agent harness abstraction: `trait ExternalAgent { fn spawn(...), fn send(...), fn poll(...) }`
- [ ] Claude Code integration (subprocess, stdin/stdout protocol)
- [ ] Codex CLI integration
- [ ] Gemini CLI integration
- [ ] Task assignment: manual (`/task assign --agent claude-code`) or rule-based
- [ ] Output capture: external agent results flow back into tau's session history
- [ ] Merge queue integration: external agent work in worktrees, same merge flow

---

## Phase 5: Multi-Agent Orchestration (4-6 weeks)

Evolve the task system to support ambient/background agents and parallel
orchestration beyond the current serial dispatch model.

- [ ] Background agents: long-running agents that monitor and react (CI watcher, PR reviewer, dependency updater)
- [ ] Agent-to-agent messaging: sessions can send structured messages to other sessions (already partially implemented via `QueueMessage`)
- [ ] Parallel task execution: dispatch N tasks simultaneously across N worktrees with resource-aware scheduling
- [ ] Task dependencies: DAG-based task ordering (task B blocked on task A)
- [ ] Budget and cost tracking: per-task and per-session token/cost accounting with configurable limits
- [ ] Supervisor agent: meta-agent that breaks specs into tasks, assigns to workers, reviews results, manages the merge queue

---

## Phase 6: Developer Experience (ongoing)

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

Docs: https://hermes-agent.nousresearch.com/docs

### Where Tau Leads

| Area | Tau | Hermes |
|---|---|---|
| Task board + merge queue | Full lifecycle (planning → review → merge), DAG deps, file-conflict-aware scheduling, checklist-gated fast-forward merge | `todo` tool (in-session only), no merge automation |
| Multi-agent primitives | Session tree with spawn/join/message, persistent hierarchy, blocking await-reply | `delegate_task` ephemeral children, max depth 3, non-durable |
| Anti-stuck detection | Loop-review checkpoint every N turns, separate reviewer model, nudge injection | Not documented |
| Cost tracking | Per-session + project-wide + subscription utilization with 5h/7d buckets | Per-session tokens/cost only |
| Git worktree isolation | Per-task worktrees with branch management | Filesystem snapshots + `/rollback` |
| Daemon architecture | Long-lived Unix daemon, attach/detach, multi-client, hot-reload config | Single-process CLI or gateway |

### Where Hermes Leads

| Area | Hermes | Tau |
|---|---|---|
| MCP integration | Full client — MCP tools auto-registered at startup, per-server filtering, `/reload-mcp` | Done (Phase 2) — client + server, stdio transport |
| Skills system | 118 bundled skills, auto-generated from tasks, agentskills.io hub, progressive disclosure | Done (Phase 3) — 4 built-in, auto-gen from merged tasks, trigger matching |
| Self-improvement | Skill auto-gen + patching, memory curation, meta-cognition LLM calls | Done (SI-1/2/3) — skill auto-gen, FTS5 recall, bounded memory |
| Agent-writable memory | MEMORY.md (factual, bounded), USER.md (Honcho dialectic user modeling) | Done — `.tau/memory.md` + `~/.config/tau/memory.md`, bounded curation |
| Cross-session recall | FTS5 search across all past sessions, LLM-summarized results | Done — `session_search` tool with FTS5, project-scoped |
| Scheduled automation | Built-in cron with natural-language spec, multi-platform delivery | Not built-in |
| Sandbox backends | 7 backends (local, Docker, SSH, Modal, Daytona, Vercel, Singularity) | `sandbox.toml` prefix (Docker, SSH) |
| Platform adapters | 21 messaging platforms (Telegram, Discord, Slack, WhatsApp, etc.) | TUI + Unix socket only |
| Smart approval | LLM-assessed risk: auto-approve low-risk, escalate uncertain, deny dangerous | User permission mode only |
| Web UI | Ships with web mode | Planned (Phase 6) |
| Code execution | `execute_code` — Python scripts call agent tools via Unix socket RPC | `bash` tool only |
| Provider breadth | 200+ via OpenRouter + custom endpoints | Anthropic native + OpenAI-compat |

### Roughly Comparable

| Area | Notes |
|---|---|
| Core agent loop | Both streaming, tool dispatch. Hermes broader (61 tools), tau more focused (~12) |
| Context compaction | Both auto-trigger on context limit. Tau uses structured format (Goal/Progress/Decisions/Next). Hermes tracks compression lineage |
| Plugin system | Both process-based. Tau is language-agnostic (JSON-lines). Hermes is Python-native |
| Session persistence | Both SQLite-backed |
| LLM provider support | Both multi-provider. Hermes wider via OpenRouter; tau deeper Anthropic integration |
| Checkpoints | Hermes: filesystem snapshots. Tau: git worktrees. Different strategies, both valid |

### Out of Scope for Tau

| Hermes Feature | Why Skip |
|---|---|
| Platform adapters (Telegram, Discord, etc.) | Different design philosophy — tau is a dev tool, not a chatbot platform |
| RL training pipeline (Atropos/GRPO) | Research infrastructure, not agent runtime |
| Self-evolution (DSPy+GEPA prompt optimization) | Deferred — skill auto-gen covers 80%; DSPy-grade optimization is future work |
| Honcho dialectic user modeling | Over-engineered for a coding agent |
| `execute_code` RPC tool | Tau's session_spawn + bash covers this pattern differently |
| Human delay simulation | Not relevant |

---

## Self-Improvement: Implementation Analysis

Hermes has 5 self-improvement mechanisms. Analysis of each for tau,
ordered by impact and feasibility.

### SI-1: Skill Auto-Generation

**What Hermes does:** After tasks with 5+ tool calls, a background LLM
summarizes the approach into a SKILL.md file. Future similar tasks load
the skill into the system prompt.

**Tau approach:** Extend Phase 3 skills with auto-generation.

- Hook into task completion — when a task reaches `merged` state, fire
  an `after_task_merge` hook
- Spawn a `model: "light"` child session with the task's full
  conversation; prompt: extract reusable procedural knowledge
- Output format: markdown with YAML frontmatter (`name`,
  `trigger_patterns`, `affected_file_globs`, `description`); body:
  approach, edge cases, domain knowledge
- Storage: `.tau/skills/auto/` (project) and
  `~/.config/tau/skills/auto/` (global), separate from hand-written
  skills
- Loading: at session start, match skills against task description and
  file paths; inject into system prompt guidelines
- Where to build: new tool in `tau-agent-plugin-worker` + hook in
  `tau-agent-plugin-tasks` scheduler
- Estimate: ~2-3 weeks (on top of Phase 3 base)

**Decision:** DONE — `tasks_skill_autogen.rs` hooks both merge paths, spawns
light-model child to extract skills into `.tau/skills/auto/`.

### SI-2: Cross-Session Episodic Memory (FTS5 Search)

**What Hermes does:** FTS5 full-text search across all past sessions.
Agent queries own history for continuity.

**Tau approach:**

- Add FTS5 virtual table to existing SQLite schema, index
  `messages.content` for assistant and user messages
- New tool: `session_search` — query string in, top-N matching excerpts
  with session IDs and timestamps out
- Optional: spawn `model: "light"` session to summarize search results
  before injection
- Where to build: schema migration in `tau-agent-lib::db`, new tool in
  `tau-agent-plugin-worker`
- Cost control: cap search results at ~2K tokens, agent decides when to
  search
- Estimate: ~1 week

**Decision:** DONE — FTS5 virtual table in `db.rs`, `session_search` tool in
orchestration plugin, project-scoped filtering.

### SI-3: Agent-Curated Factual Memory

**What Hermes does:** Agent writes to MEMORY.md (facts) and USER.md
(user profile). Changes persist to disk immediately, enter system prompt
next session. Bounded capacity forces active curation.

**Tau approach:**

- New tool: `memory` with `add`/`replace`/`remove` actions targeting
  `~/.config/tau/memory.md` (global) and `.tau/memory.md` (project)
- Bounded capacity: hard character limit (~2200 chars project, ~1400
  user profile) — agent must curate, not dump
- Injection: load into system prompt at session start, after
  instructions but before tools
- Frozen snapshot: no mid-session mutation of prompt content; changes
  apply next session
- Where to build: new tool in `tau-agent-plugin-worker`, prompt assembly
  in `tau-agent-engine`
- Estimate: ~1 week

**Decision:** DONE — `memory.rs` module with add/replace/remove/list,
bounded capacity, injected into system prompt via `load_for_prompt()`.

### SI-4: Meta-Cognitive Skill Patching

**What Hermes does:** When using a skill and finding it wrong/outdated,
patches it in-place during the session.

**Tau approach:**

- Extend skill tool with `patch` action
- Auto-generated skills patchable freely; hand-written skills require
  confirmation
- Skills in `.tau/skills/` are git-tracked — natural audit trail
- Depends on SI-1
- Estimate: ~2-3 days

**Decision:** TODO

### SI-5: User Modeling

**What Hermes does:** Honcho dialectic modeling builds persistent user
profile across sessions — stack preferences, terminology, work style.

**Tau approach:**

- Lower priority — tau is primarily a coding agent, not a conversational
  assistant
- Partially covered by `instructions.toml` (human-written)
- Could add `user_profile.md` that agent populates over time with coding
  preferences, review style, stack familiarity
- Lightweight version: fold into SI-3 memory tool with a `user` scope

**Decision:** TODO

### Recommended Priority

1. **SI-1 + Phase 3 base** — biggest impact, makes tau self-improving
2. **SI-2 FTS5 search** — low effort (SQLite already there), enables cross-session learning
3. **SI-3 curated memory** — complements skills with factual knowledge
4. **SI-4 skill patching** — incremental on top of SI-1
5. **SI-5 user modeling** — defer or fold into SI-3

---

## Non-Goals

Things tau deliberately does not pursue:

- **Terminal emulation** — tau runs inside a terminal, it doesn't replace one
- **GPU-rendered UI** — TUI portability (SSH, tmux, any terminal) is a feature
- **Cloud dependency** — self-contained operation is a core principle
- **Proprietary backend** — MIT everything, no phone-home

---

## Prioritization Rationale

1. **Security first** — the audit findings are real and some are exploitable
2. **MCP second** — unlocks the entire MCP tool ecosystem with minimal code; moves tau from "closed tool set" to "open platform"
3. **Skills third** — low-effort, high-value customization without writing a full plugin
4. **External harness fourth** — lets tau orchestrate best-in-class agents rather than competing with them tool-for-tool
5. **Multi-agent fifth** — builds on top of the harness and task system; the unique architectural bet
6. **DX ongoing** — polish that compounds over time
