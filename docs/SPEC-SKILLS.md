# Spec: Skills System (Phase 3)

## Overview

Markdown-defined agent behaviors loadable per-project or globally.
Skills are a lightweight alternative to full plugins — no code, no
subprocess, just instruction documents that modify the agent's behavior
for specific contexts.

Phase 3 covers two things:
1. **Static skills** — human-written skill files, loaded by name or
   trigger pattern
2. **Auto-generated skills** (SI-1 from roadmap) — agent creates skills
   from completed tasks, building procedural memory over time

This is the primary self-improvement mechanism for tau.

## Design Principles

- Skills are plain markdown — human-readable, auditable,
  version-controllable
- Skills modify the system prompt, not the code — no runtime side
  effects
- Skills are bounded — hard token limit prevents context pollution
- Auto-generated skills are separated from hand-written ones
- The agent can read, create, and patch skills but cannot delete
  hand-written ones

---

## Skill Format

### File Structure

```
~/.config/tau/skills/          # global (user-written)
~/.config/tau/skills/auto/     # global (agent-generated)
.tau/skills/                   # project (user-written)
.tau/skills/auto/              # project (agent-generated)
```

Skills are `.md` files with YAML frontmatter.

### Schema

```markdown
---
name: conventional-commits
description: Write commit messages following the Conventional Commits spec
version: 1
triggers:
  - match: keyword
    pattern: "commit"
  - match: keyword
    pattern: "commit message"
  - match: tool
    pattern: "bash"
    args_contain: "git commit"
  - match: file_glob
    pattern: "*.rs"
tags: [git, workflow]
priority: 0
enabled: true
---

# Conventional Commits

## When to Use

When creating git commits in this project.

## Procedure

1. Use the format: `type(scope): message`
2. Types: feat, fix, docs, refactor, test, chore, perf
3. Scope is optional but preferred
4. Message is lowercase, imperative mood, no period

## Pitfalls

- Don't use past tense ("added" → "add")
- Don't capitalize the message

## Verification

- Commit message matches `^(feat|fix|docs|refactor|test|chore|perf)(\(.+\))?: .+$`
```

### Frontmatter Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | — | Unique identifier, used for explicit invocation |
| `description` | string | yes | — | One-line summary shown in skill listings |
| `version` | integer | no | 1 | Incremented on updates |
| `triggers` | list | no | [] | Auto-matching rules (see below) |
| `tags` | list | no | [] | For organization and search |
| `priority` | integer | no | 0 | Higher = loaded first when multiple match |
| `enabled` | bool | no | true | Toggle without deleting |
| `auto_generated` | bool | no | false | Set by agent; affects patching permissions |
| `source_task` | string | no | — | Task ID that generated this skill (auto only) |
| `affected_file_globs` | list | no | [] | File patterns this skill is relevant to |

### Trigger Types

| Match Type | Description | Example |
|---|---|---|
| `keyword` | Task description or user message contains pattern | `"commit"` |
| `tool` | A tool call matches; optional `args_contain` | tool=`bash`, args_contain=`git commit` |
| `file_glob` | Files in scope match glob pattern | `"*.rs"`, `"src/api/**"` |
| `phase` | Task phase matches | `"review"`, `"planning"` |
| `explicit` | Only loaded via `/skill name` | — |

Triggers use OR logic — any match loads the skill. Keyword matching is
case-insensitive substring.

### Body Conventions

The markdown body is free-form but the recommended structure is:

- **When to Use** — conditions where this skill applies
- **Procedure** — step-by-step instructions
- **Pitfalls** — common mistakes to avoid
- **Verification** — how to check correctness

---

## Skill Loading

### Progressive Disclosure (Three Levels)

Inspired by Hermes but adapted to tau's prompt structure:

**Level 0 — Listing** (~3-5 tokens per skill): All enabled skills are
listed in the system prompt as a compact index:

```
Available skills:
- conventional-commits: Write commit messages following Conventional Commits
- tdd: Test-driven development workflow
- code-review: Structured code review process
```

This is always present. Cost: negligible.

**Level 1 — Injection**: When a skill is triggered (by match or
explicit invocation), its full markdown body is injected into the system
prompt's `append` section. Multiple skills can be active simultaneously.

**Level 2 — Reference**: Skills can reference other files via relative
paths in the body. The agent can use `read` tool to load these on
demand. No automatic injection.

### Trigger Evaluation

Triggers are evaluated at two points:

1. **Session start** (`before_agent_start` hook): Match against session
   context — task description, affected files, phase. Load Level 1 for
   all matching skills.

2. **Per-turn** (optional, configurable): After each user message,
   re-evaluate keyword triggers against the message content. Newly
   matched skills are injected via `before_agent_start` hook's
   `system_prompt` append. Skills loaded mid-session stay loaded for the
   remainder.

Per-turn evaluation is off by default (adds latency). Enable via:

```toml
# .tau/skills.toml or ~/.config/tau/skills.toml
[matching]
per_turn = true
```

### Explicit Invocation

`/skill {name}` — loads the named skill immediately (Level 1 injection).
Available in interactive sessions.

`/skills` — lists all available skills with descriptions and
enabled/disabled status.

### Token Budget

Hard limit on total skill content injected into prompt: **8192 tokens**
(configurable via `skills.toml`). If matched skills exceed budget:

1. Sort by priority (descending), then by specificity (file_glob >
   phase > keyword)
2. Include skills until budget exhausted
3. Log skipped skills as warning

```toml
# .tau/skills.toml
[limits]
max_tokens = 8192
max_skills_per_session = 10
```

### Config Chain

Skills are loaded from all three tiers and merged:

- Operator: `~/.config/tau/projects/{name}/skills/`
- Project: `.tau/skills/`
- Global: `~/.config/tau/skills/`

Name collisions: operator > project > global (highest tier wins).

`skills.toml` config uses `load_first()` (operator > global, project
tier allowed).

---

## Skill Management Tools

New tools available to the LLM, registered by the worker plugin:

### `skill_list`

List available skills with metadata.

```json
{
  "scope": "project" | "global" | "all"
}
```

Returns name, description, enabled, auto_generated, trigger summary.

### `skill_read`

Read full skill content.

```json
{
  "name": "conventional-commits"
}
```

Returns the full markdown content including frontmatter.

### `skill_create`

Create a new skill. Only writes to `auto/` directories.

```json
{
  "name": "rust-error-handling",
  "description": "Pattern for error handling in this project's Rust code",
  "scope": "project",
  "triggers": [
    { "match": "file_glob", "pattern": "*.rs" },
    { "match": "keyword", "pattern": "error handling" }
  ],
  "tags": ["rust", "patterns"],
  "content": "# Rust Error Handling\n\n## Procedure\n..."
}
```

Writes to `.tau/skills/auto/{name}.md` (project) or
`~/.config/tau/skills/auto/{name}.md` (global).

**Validation:**
- Name must be unique across all scopes
- Content must be valid markdown with valid YAML frontmatter
- `auto_generated` is forced to `true`
- Content size limit: 15KB

### `skill_patch`

Modify an existing skill. For auto-generated skills, direct patching.
For hand-written skills, returns an error asking the user to edit
manually.

```json
{
  "name": "rust-error-handling",
  "patches": [
    {
      "section": "Procedure",
      "action": "append",
      "content": "5. Always use `thiserror` for library errors"
    }
  ]
}
```

Alternatively, full-body replace:

```json
{
  "name": "rust-error-handling",
  "content": "# Rust Error Handling\n\n(full new content)"
}
```

Increments `version` automatically.

### `skill_search`

Search skills by keyword across names, descriptions, tags, and content.

```json
{
  "query": "error handling rust"
}
```

Returns matching skills ranked by relevance.

---

## Auto-Generation (SI-1)

### Trigger

When a task reaches `merged` state in the task scheduler, the
`after_task_merge` hook fires. The skill auto-generator evaluates
whether the task is a candidate:

**Criteria (must meet at least one):**
- Task involved 5+ tool calls in the worker session
- Task worker session contained an error-recovery sequence (tool error
  followed by successful retry with different approach)
- Task description contains domain-specific terms not covered by
  existing skills

**Anti-criteria (skip if):**
- A skill already exists that closely matches this task's description
  (fuzzy match on name + description, threshold TBD)
- Task was trivial (< 3 tool calls, single file change)
- Task failed or was closed without merging

### Generation Process

1. **Spawn summarizer session**: `model: "light"`, child of the task's
   creator session. Budget: single turn.

2. **Prompt** (injected as user message):
   ```
   Review the following completed task and extract reusable procedural
   knowledge into a skill document.

   Task: {task_description}
   Files changed: {affected_files}
   Approach taken: (summarized from worker session transcript)

   Write a skill in this format:
   - name: short kebab-case identifier
   - description: one-line summary
   - triggers: what contexts should load this skill
   - content: Procedure, Pitfalls, Verification sections

   Only create a skill if the approach contains genuinely reusable
   knowledge. If this was a straightforward change with no novel
   patterns, respond with "NO_SKILL".
   ```

3. **Parse response**: Extract skill fields from the summarizer's
   output. Validate format.

4. **Deduplicate**: Check existing skills for name collision or high
   description similarity. If collision, either skip or merge (append
   new knowledge to existing skill via `skill_patch`).

5. **Write**: Save to `.tau/skills/auto/{name}.md` (project scope) or
   `~/.config/tau/skills/auto/{name}.md` (global scope, if task touched
   no project-specific files).

6. **Log**: Record skill creation in task's session log.

### Cost Control

- Summarizer uses `model: "light"` — cheapest available
- Single-turn generation, no iteration
- Skip criteria filter out ~70% of tasks (estimate)
- No auto-generation in child/leaf sessions — only merged tasks

### Skill Quality Over Time

Auto-generated skills accumulate. Without curation they'll become noise.
Mitigations:

- **Version tracking**: `version` increments on every patch; skills
  that get patched frequently are actively useful
- **Staleness detection**: Skills not loaded in 30 days get a
  `stale: true` flag (set by a periodic check, not auto-deleted)
- **Manual review**: `/skills audit` command lists auto-generated
  skills sorted by last-used date, with stale ones highlighted
- **Agent curation**: The agent can be instructed (via project
  instructions) to periodically review and prune auto-generated skills
- **Hard cap**: Max 50 auto-generated skills per scope. Oldest stale
  skills are candidates for removal when cap is hit.

---

## Built-In Skills

Ship with tau, installed to `~/.config/tau/skills/builtin/`:

| Skill | Description |
|---|---|
| `conventional-commits` | Conventional commit message format |
| `pr-creation` | Pull request title + body structure |
| `code-review` | Structured review checklist |
| `tdd` | Test-driven development cycle |
| `debugging` | Systematic debugging approach |

Built-in skills are read-only. The agent cannot patch them. Users can
override by creating a same-named skill in project or global scope.

---

## System Prompt Integration

### Prompt Assembly Changes

Extend `system_prompt::build()` in `tau-agent-engine::system_prompt.rs`:

Current `PromptOptions`:
```rust
pub struct PromptOptions {
    pub cwd: Option<String>,
    pub tools: Vec<ToolPrompt>,
    pub extra_guidelines: Vec<String>,
    pub append: Option<String>,
}
```

Add:
```rust
pub struct PromptOptions {
    pub cwd: Option<String>,
    pub tools: Vec<ToolPrompt>,
    pub extra_guidelines: Vec<String>,
    pub append: Option<String>,
    pub skill_listing: Option<String>,      // Level 0: compact index
    pub active_skills: Vec<String>,         // Level 1: full content blocks
}
```

Prompt structure becomes:

1. Identity
2. Available tools
3. **Available skills** (Level 0 listing) — new section
4. Guidelines (general + per-tool)
5. Date, cwd
6. **Active skills** (Level 1 injections) — new section before append
7. Append (instructions, hook injections)

### Hook Integration

The skill loader runs as part of the `before_agent_start` hook flow.
Not a separate plugin — integrated into the worker plugin or engine
directly.

**Option A — Engine-native** (recommended): Skill loading is part of
`system_prompt::build()`. The engine loads and matches skills, injects
them into `PromptOptions`. No plugin involvement.

**Option B — Worker plugin**: Skill loading happens in the worker's
`before_agent_start` hook handler. Returns matched skill content via
`HookResult.system_prompt`.

Option A is simpler and avoids the append-only constraint (hooks can
only append to system prompt, not structure it).

---

## Where to Build

| Component | Location |
|---|---|
| Skill file parser (frontmatter + body) | `tau-agent-base::skills` (new module) |
| Skill config (`skills.toml`) | `tau-agent-base::skills::config` |
| Skill index (load, match, deduplicate) | `tau-agent-engine::skills` (new module) |
| Prompt integration | `tau-agent-engine::system_prompt` — extend `build()` |
| Skill management tools | `tau-agent-plugin-worker::skills_tools` (new module) |
| Auto-generation hook | `tau-agent-plugin-tasks::tasks_scheduler` — extend merge handler |
| `/skill`, `/skills` commands | `tau-agent-lib::dispatch.rs` |
| Built-in skill files | `tau-agent-engine/skills/` (embedded via `include_str!` or shipped alongside binary) |

---

## Config: skills.toml

```toml
# .tau/skills.toml or ~/.config/tau/skills.toml

[matching]
per_turn = false              # Re-evaluate triggers each turn (default: off)
auto_generate = true          # Enable SI-1 auto-generation on task merge
auto_generate_model = "light" # Model for summarizer session

[limits]
max_tokens = 8192             # Total skill content token budget
max_skills_per_session = 10   # Max concurrent Level 1 skills
max_auto_skills = 50          # Per-scope cap on auto-generated skills
max_skill_size = 15360        # Max bytes per skill file (15KB)
stale_days = 30               # Days unused before marking stale

[builtin]
enabled = true                # Load built-in skills
```

---

## Interaction with Existing Systems

### instructions.toml

Skills and instructions coexist. Instructions are phase-scoped
(`worker`, `planning`, `review`) and always loaded. Skills are
context-triggered and optional. No overlap in injection points —
instructions go in `append`, skills get their own section.

If a skill contradicts an instruction, the instruction wins (it appears
later in the prompt, closer to the conversation).

### Plugins

Plugins can register skills by including `.md` files and calling a new
`register_skills` message in the plugin protocol. This is a stretch goal
— not required for initial implementation.

### Task System

Skills interact with tasks in two ways:
1. **Input**: Task description and affected files are used for trigger
   matching, so task workers get relevant skills automatically
2. **Output**: Completed tasks generate new skills (SI-1)

### Context Compaction

Skills are injected into the system prompt, not into messages. They
survive compaction (system prompt is never compacted). This is
intentional — skills should persist for the full session.

---

## Open Questions

- [ ] Should skills support conditional sections (e.g., "if Rust
  project" / "if Python project") or is file_glob triggering sufficient?
- [ ] Should auto-generated skills require human approval before
  becoming active? (Hermes does not require this)
- [ ] Should there be a community skill hub (like agentskills.io)?
  Or is git clone + file copy sufficient for now?
- [ ] Should skills be able to declare tool dependencies (e.g., "this
  skill requires the `bash` tool to be available")?
- [ ] How should skill token counting work — approximate (word count ×
  1.3) or exact (tokenizer call)?
- [ ] Should the agent be able to disable a skill mid-session if it
  determines the skill is not helpful?
