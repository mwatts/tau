---
name: tdd
description: Red-green-refactor test-driven development workflow
triggers:
  keywords: ["tdd", "test driven", "test-driven"]
priority: 40
---

## Workflow

1. **Red** — Write a failing test that defines the desired behavior
2. **Green** — Write the minimal code to make the test pass
3. **Refactor** — Clean up while keeping tests green

## Rules

- Never write production code without a failing test first
- Each cycle should be small (under 5 minutes)
- Run the full test suite after each green step
- Refactor only when tests are passing
- Test behavior, not implementation details
- One assertion per test (prefer focused tests over catch-all tests)

## When to Skip

- Pure refactoring of already-tested code
- Trivial one-liners (type aliases, re-exports)
- Generated code (schema migrations, protobuf)
