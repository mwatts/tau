---
name: conventional-commits
description: Enforce conventional commit message format
triggers:
  keywords: ["commit", "merge"]
priority: 40
---

## Format

`type(scope): subject`

## Types

feat, fix, docs, refactor, test, chore, perf, ci, build, style

## Rules

- Subject line: max 72 chars, imperative mood, no trailing period
- Scope: optional, lowercase, names the affected module/area
- Body: separated by blank line, wrap at 72 chars, explain WHY not WHAT
- Breaking changes: append `!` after type/scope (e.g., `feat!: ...`) and add `BREAKING CHANGE:` footer

## Examples

- `feat(skills): add skill discovery and matching`
- `fix(mcp): handle empty tool response`
- `docs: add Phase 3 spec`
- `refactor(engine)!: change PromptOptions API`
