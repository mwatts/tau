# Prompt Optimization System

Lightweight DSPy-style prompt optimization for tau. Tracks prompt/tool effectiveness per-project, auto-refines underperformers via LLM-generated variants, applies winners through existing mechanisms (skill_patch, memory, tool_prompts.toml). Stable optimizations promote to global defaults.

## Architecture

Three components:

1. **Metrics Collector** — passive hooks into task completion and tool execution. Records prompt variant fingerprints and outcomes to `prompt_metrics` DB table.
2. **Optimizer** — fires after every 10 completed tasks per project. Loads metrics, builds assessment context, asks LLM to score prompts and propose refinements. Auto-patches low-risk surfaces, writes proposals for high-risk ones.
3. **Promoter** — fires every 50 global task completions or weekly. Graduates per-project optimizations that prove stable across 3+ cycles and 2+ projects into global defaults.

Data flow: `task completion → metrics collector → DB ← optimizer → skill_patch / tool_prompts.toml / proposals`

## Optimization Targets

All textual prompt surfaces:

- **Tool guidelines** — per-tool bullet points in `ToolPrompt.guidelines`
- **Auto-skills** — `.tau/skills/auto/*.md` files injected as `extra_guidelines`
- **System prompt** — preamble/append framing

## Signal Sources

Three tiers of signal:

1. **Task outcomes** — merged (success), failed/closed (failure). Unambiguous macro signal.
2. **Tool error rates** — per-tool `is_error` counts from tool results within each session. Micro signal for tool-level issues.
3. **LLM self-assessment** — after each terminal task state, a light-model call reviews the last few messages and produces:
   - `effectiveness_score` (1-5)
   - `inefficiency_notes` (free text: unnecessary steps, wrong tool choices)
   - `prompt_suggestions` (free text: what would have helped)

Assessment cost: ~500 tokens per completed task.

## Metrics Collection

One row per session completion. Hooked into `notify_parent_of_child_completion` and the task state-change handler.

Fields recorded:

| Field | Source |
|---|---|
| `project_name` | session's project |
| `session_id` | completed session |
| `task_id` | linked task (nullable) |
| `outcome` | merged/failed/closed/completed |
| `prompt_hash` | SHA256 of active system prompt at session start |
| `tool_error_count` | count of is_error=true tool results |
| `tool_call_count` | total tool calls |
| `tool_errors_by_name` | JSON: `{"bash": 3, "edit": 1}` |
| `active_skills` | JSON array of active skill filenames |
| `effectiveness_score` | 1-5 (task sessions only) |
| `inefficiency_notes` | free text |
| `prompt_suggestions` | free text |
| `optimization_id` | FK to optimizations table (nullable) |
| `created_at` | timestamp |

## Optimizer

### Trigger

Per-project counter increments on each terminal task state. At N=10, optimizer fires and resets.

### Input Context

1. Current tool guidelines (from `ToolPrompt.guidelines`)
2. Current active auto-skills (`.tau/skills/auto/*.md` content)
3. Current system prompt append block
4. Metrics window: last 10 tasks' outcomes, tool error rates, LLM self-assessments
5. Aggregated stats: success rate, per-tool error rate, common inefficiency themes

### LLM Call

Single call (~2-4K input, ~1K output). Prompt instructs the model to:

- Identify prompt components correlated with failures or inefficiency
- Propose specific edits (tool guideline rewrites, skill patches, system prompt tweaks)
- Tag each proposal: `risk: low|medium|high`, `target: tool_guideline|skill|system_prompt`

### Output Handling

| Risk / Target | Action |
|---|---|
| low + `tool_guideline` | Auto-apply: write to `.tau/tool_prompts.toml` |
| low + `skill` | Auto-apply: `skill_patch` the auto-skill |
| medium/high OR `system_prompt` | Write to `.tau/optimization_proposals.md`, surface as info message |

### Versioning

Each applied change recorded in `optimizations` table with old/new content hashes. Next cycle checks for regression.

### Location

`crates/tau-agent-lib/src/prompt_optimizer.rs` — a `BgJob` on `BgTaskScheduler`.

## Tool Prompt Overrides

New per-project file: `.tau/tool_prompts.toml` (git-tracked).

```toml
[bash]
guidelines = [
    "Prefer piping commands over temporary files",
    "Always quote variables in shell scripts",
]

[edit]
guidelines = [
    "Include 3+ lines of surrounding context in old_string",
]
```

Resolution order: binary defaults → global `~/.config/tau/tool_prompts.toml` → project `.tau/tool_prompts.toml`. Later sources fully replace that tool's guidelines (not merge).

Loaded at session start when `tool_prompts()` is called. Existing `PromptOptions.tools` pipeline picks them up.

## Promoter

### Trigger

Every 50 completed tasks globally, or weekly via BgJob periodic timer, whichever comes first.

### Promotion Criteria (all required)

1. Optimization active for 3+ cycles in originating project without revert
2. Same or semantically equivalent optimization emerged independently in 2+ projects (exact match for tool guidelines; for skills, LLM judges similarity with >80% confidence)
3. Post-application metrics show improvement (success rate up OR tool error rate down)

### What Gets Promoted

| Source | Promoted to |
|---|---|
| Project `tool_prompts.toml` entries | Global `~/.config/tau/tool_prompts.toml` |
| Project `.tau/skills/auto/*.md` | Global `~/.config/tau/skills/auto/*.md` |
| System prompt proposals | Global memory (advisory) |

### Demotion

If a promoted optimization correlates with failures in inheriting projects, it's removed from global defaults. Per-project overrides always take precedence.

## Auto-Revert & Safety

After each optimization cycle applies changes, the next cycle compares post-change metrics against the pre-change baseline:

- Success rate drops >20% OR tool error rate increases >30% → automatic revert
- Reverted changes marked in DB with reason; same change blocked for 5 cycles

Git safety net: all auto-applied changes land in git-tracked files. Users can `git diff` to inspect and `git checkout` to revert manually.

## DB Schema

```sql
CREATE TABLE prompt_metrics (
    id                  INTEGER PRIMARY KEY,
    project_name        TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    task_id             INTEGER,
    outcome             TEXT NOT NULL,
    prompt_hash         TEXT NOT NULL,
    tool_error_count    INTEGER NOT NULL DEFAULT 0,
    tool_call_count     INTEGER NOT NULL DEFAULT 0,
    tool_errors_by_name TEXT,
    active_skills       TEXT,
    effectiveness_score INTEGER,
    inefficiency_notes  TEXT,
    prompt_suggestions  TEXT,
    optimization_id     INTEGER,
    created_at          INTEGER NOT NULL
);

CREATE TABLE optimizations (
    id              INTEGER PRIMARY KEY,
    project_name    TEXT NOT NULL,
    target          TEXT NOT NULL,
    target_name     TEXT NOT NULL,
    old_hash        TEXT NOT NULL,
    new_hash        TEXT NOT NULL,
    risk            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    reverted_reason TEXT,
    applied_at      INTEGER NOT NULL,
    reverted_at     INTEGER
);

CREATE TABLE promotions (
    id              INTEGER PRIMARY KEY,
    source_project  TEXT NOT NULL,
    optimization_id INTEGER NOT NULL,
    target_path     TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    promoted_at     INTEGER NOT NULL,
    demoted_at      INTEGER
);
```

## Scope: Per-Project with Promotion

- Each project accumulates its own metrics and optimizes independently
- Matches existing per-project scoping of skills and memory
- Stable optimizations promote to global defaults via the Promoter
- New projects inherit promoted globals automatically

## Testing Strategy

- **Unit tests**: metrics collection (mock session data → correct DB rows), optimizer input assembly, revert logic
- **Integration test**: seed project with 10 task metrics (mixed outcomes, deliberate tool errors on bash), fire optimizer, assert tool_prompts.toml patch targeting bash guidelines
- **Regression test**: apply optimization, seed bad metrics, fire next cycle, assert auto-revert
