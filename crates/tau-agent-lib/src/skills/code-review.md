---
name: code-review
description: Structured code review checklist
triggers:
  commands: ["/review"]
  keywords: ["review"]
priority: 40
---

## Review Checklist

1. **Correctness** — Does the code do what it claims? Edge cases handled?
2. **Security** — Injection, auth bypass, secrets in code, unsafe deserialization?
3. **Performance** — Unnecessary allocations, N+1 queries, missing indexes?
4. **Maintainability** — Clear names, minimal complexity, no dead code?
5. **Tests** — New behavior covered? Existing tests still valid?
6. **Scope** — Only changes relevant to the stated goal? No drive-by refactors?

## Feedback Format

- Prefix with severity: `[blocking]`, `[suggestion]`, `[nit]`
- Quote the specific line(s) under discussion
- Explain WHY something is a problem, not just WHAT to change
- Offer a concrete alternative when blocking
