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

- [ ] Add `rmcp` crate (MIT) as workspace dependency
- [ ] Config: `~/.config/tau/mcp.toml` + project `.tau/mcp.toml` (global tier only for security)
- [ ] MCP server lifecycle manager (spawn, health-check, restart)
- [ ] Surface MCP tools in the LLM tool list alongside built-in tools
- [ ] Surface MCP resources as attachable context
- [ ] MCP prompts as slash commands

### 2b: MCP Server (tau exposes itself)

- [ ] Expose tau's built-in tools (bash, read, write, edit, diagnostics) as MCP tools
- [ ] Expose session/task management as MCP resources
- [ ] stdio transport (for integration with other agents/editors)
- [ ] HTTP+SSE transport (for web integrations)

---

## Phase 3: Skills System (1-2 weeks)

Markdown-defined agent behaviors, loadable per-project or globally.
Lightweight alternative to full plugins for customizing agent behavior.

- [ ] Skill format: markdown files in `.tau/skills/` (project) and `~/.config/tau/skills/` (global)
- [ ] Skill loading: inject matching skills into system prompt context
- [ ] Skill matching: by name (explicit `/skill-name`) or by trigger pattern (file type, keyword)
- [ ] Built-in skills: conventional commits, PR creation, code review, TDD
- [ ] Skill discovery: `/skills` command lists available skills

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
