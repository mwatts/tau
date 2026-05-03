---
name: pr-creation
description: Guide pull request title and description
triggers:
  commands: ["/pr"]
  keywords: ["pull request", "PR"]
priority: 40
---

## PR Title

- Under 70 characters
- Format: `type(scope): short description`
- Same types as conventional commits

## PR Body

Structure:
```
## Summary
- 1-3 bullet points explaining WHAT and WHY

## Changes
- Key changes, grouped by area

## Test Plan
- How to verify the changes work
```

## Rules

- Title describes the outcome, not the process
- Summary focuses on user/developer impact
- Link related issues with `Closes #N` or `Relates to #N`
- Keep description scannable — bullets over prose
