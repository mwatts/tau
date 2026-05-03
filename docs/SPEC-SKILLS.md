# Spec: Skills System (Phase 3)

## Overview

Markdown-defined agent behaviors, loadable per-project or globally.
Skills inject domain knowledge, workflows, and constraints into the
system prompt — a lightweight alternative to full plugins for customizing
agent behavior without writing Rust code.

## Skill Format

A skill is a single markdown file with YAML frontmatter:

```markdown
---
name: conventional-commits
description: Enforce conventional commit message format
triggers:
  - commands: ["/commit", "/task done"]
  - keywords: ["commit", "merge"]
  - file_globs: []
priority: 10
---

## Rules

- Use format: `type(scope): message`
- Types: feat, fix, docs, refactor, test, chore, perf
- Scope is optional but encouraged
- Subject line max 72 chars, no trailing period

## Examples

- `feat(mcp): add resource subscription support`
- `fix: handle empty tool response gracefully`
```

### Frontmatter Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | — | Unique identifier, used in `/skill-name` invocation |
| `description` | string | yes | — | One-line summary shown in `/skills` listing |
| `triggers` | object | no | `{}` | When to auto-load (see Matching) |
| `triggers.commands` | string[] | no | `[]` | Slash commands that activate this skill |
| `triggers.keywords` | string[] | no | `[]` | Keywords in user message that activate |
| `triggers.file_globs` | string[] | no | `[]` | File patterns in context that activate |
| `priority` | int | no | `50` | Higher = injected earlier in prompt (0-100) |

### Body

Everything below the frontmatter is the skill content — injected verbatim
into the system prompt when the skill is active. Markdown formatting is
preserved.

## Skill Locations

Skills are discovered from multiple directories, searched in priority
order. **Both `.tau/skills/` and `.agents/skills/` are supported** at
each tier (`.agents/skills/` is the emerging cross-agent standard).

### Discovery Order (highest priority first)

1. **Operator tier** — `~/.config/tau/projects/{name}/skills/`
2. **Project tier** — `{project}/.tau/skills/` and `{project}/.agents/skills/`
3. **Global tier** — `~/.config/tau/skills/`

Within each directory, all `*.md` files are loaded recursively
(subdirectories allowed for organization).

### Merge Semantics

- All tiers are loaded (not first-wins) — skills accumulate
- If two skills share the same `name`, the higher-priority tier wins
  (operator > project > global)
- `.tau/skills/` and `.agents/skills/` within the same tier have equal
  priority; if both contain a skill with the same name, `.tau/skills/`
  wins (tau-specific overrides generic)

### Security

Project-tier skills are **allowed** (unlike `plugins.toml` or
`mcp.toml`). Rationale: skills only inject text into the system prompt —
they cannot execute code, spawn processes, or access the network. The
worst a malicious skill can do is give the LLM bad instructions, which
the user can observe in agent behavior.

## Skill Matching

A skill becomes active for a session when any of these conditions hold:

1. **Explicit invocation** — user types `/skill-name` (the `name` from
   frontmatter)
2. **Command trigger** — current slash command matches
   `triggers.commands`
3. **Keyword trigger** — user message contains any word from
   `triggers.keywords` (case-insensitive, whole-word match)
4. **File glob trigger** — any file in the current context (edited,
   read, or mentioned) matches `triggers.file_globs`
5. **Always-on** — if `triggers` is empty or omitted, the skill is
   always active

### Activation Budget

To prevent prompt bloat, cap total injected skill content at **4000
tokens** (estimated via `content.len() / 4`). When the budget is
exceeded, skills are selected by priority (highest first), then by
specificity (explicit > command > keyword > file_glob > always-on).
Skills that don't fit are dropped with a log warning.

## Prompt Injection

Active skills are injected into `PromptOptions::extra_guidelines` as a
single block, wrapped with a header:

```
<skills>
## [skill-name]: description

[skill body]

## [another-skill]: description

[skill body]
</skills>
```

Injection point: after built-in guidelines, before `append` (which
carries `instructions.toml` content). This means instructions.toml can
override skill behavior — intentional, since instructions are
operator/project-specific overrides.

## Built-in Skills

Ship with tau in a `skills/` directory at the crate root (compiled in
via `include_str!` or loaded from a known install path):

| Skill | Trigger | Description |
|---|---|---|
| `conventional-commits` | keywords: commit, merge | Enforce conventional commit format |
| `pr-creation` | commands: /pr | Guide PR title/body creation |
| `code-review` | commands: /review | Structured code review checklist |
| `tdd` | keywords: test, tdd | Red-green-refactor workflow |

Built-in skills have lowest priority (below global tier). They can be
overridden by placing a skill with the same name in any user directory.

## Commands

### `/skills`

List all discovered skills with their status:

```
Skills:
  ● conventional-commits  [active]   Enforce conventional commit format
  ○ pr-creation           [loaded]   Guide PR title/body creation
  ● tdd                   [active]   Red-green-refactor workflow
  ○ code-review           [loaded]   Structured code review checklist

Locations searched:
  ~/.config/tau/skills/
  .tau/skills/
  .agents/skills/
```

### `/skill-name`

Explicitly activate a skill for the current session (persists until
session end). If already active, show its content.

### `/skills reload`

Re-scan skill directories and update the active set. Useful after
editing a skill file mid-session.

## Implementation

### Where to Build

| Component | Location |
|---|---|
| Skill types (frontmatter, parsed skill) | `tau-agent-base::skills` (new module) |
| Skill discovery & loading | `tau-agent-lib::skills` (new module) |
| Skill matching engine | `tau-agent-lib::skills::matcher` |
| Prompt injection | `tau-agent-lib::server::agent_runner.rs` — extend `PromptOptions` assembly |
| Slash commands (`/skills`, `/skill-name`) | `tau-agent-lib::server::dispatch.rs` |
| Built-in skills (embedded) | `tau-agent-lib::skills::builtin` |
| Config: enable/disable, budget | `~/.config/tau/skills.toml` (optional) |

### Data Flow

```
Session start / user message
  → SkillLoader::discover(project_path, project_name)
    → scan .tau/skills/, .agents/skills/, ~/.config/tau/skills/
    → parse frontmatter, deduplicate by name
  → SkillMatcher::select(discovered, context)
    → context = { user_message, slash_command, files_in_context, explicit_activations }
    → return Vec<ActiveSkill> sorted by priority, within budget
  → inject into PromptOptions::extra_guidelines
  → system_prompt::build(options)
```

### Key Types

```rust
/// Parsed skill from a markdown file.
pub struct Skill {
    pub name: String,
    pub description: String,
    pub triggers: SkillTriggers,
    pub priority: u8,
    pub body: String,
    pub source: SkillSource,
}

pub struct SkillTriggers {
    pub commands: Vec<String>,
    pub keywords: Vec<String>,
    pub file_globs: Vec<String>,
}

pub enum SkillSource {
    Builtin,
    Global,
    Operator,
    ProjectTau,    // .tau/skills/
    ProjectAgents, // .agents/skills/
}

pub struct SkillLoader {
    skills: Vec<Skill>,
}

pub struct SkillMatcher {
    budget_chars: usize, // default 16000 (~4000 tokens)
    explicit_activations: HashSet<String>,
}
```

### Implementation Order

1. Skill types + frontmatter parser (serde_yaml for frontmatter, split
   on `---` fences)
2. Skill discovery (directory scanning, dedup)
3. Skill matching (trigger evaluation)
4. Prompt injection (wire into agent_runner)
5. `/skills` and `/skill-name` commands
6. Built-in skills (ship 4 skills)
7. `/skills reload` command
8. Optional `skills.toml` config (disable specific skills, adjust
   budget)

### Dependencies

- `serde_yaml` — parse YAML frontmatter (already in workspace or add)
- `globset` — file glob matching (already in workspace via ignore crate,
  or add directly)
- No new external services

## Open Questions

- [ ] Should skills support parameterization (e.g., `/tdd --language rust`)?
- [ ] Should skill activation persist across session compaction (include
  in compaction context)?
- [ ] Should `.agents/skills/` also be searched at operator and global
  tiers (`~/.config/tau/agents/skills/`)?
- [ ] Should skills be able to contribute tools (not just prompt text)?
  — Defer to Phase 3.5 or plugin system.
