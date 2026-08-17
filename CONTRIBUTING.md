# Contributing

## Commit convention

```
feat|fix|refactor|test|docs|chore|infra|bench(scope): imperative summary

Body: what changed and why. Name the tradeoff if there was one.
Footer: Provenance: <source> when adapted from reference/
```

Example scopes: `infra`, `detectors`, `lineage`, `benchmarks`, `api`.

This isn't enforced by a commit-msg hook — it's reviewed by hand against
this file and against CLAUDE.md's fuller commit discipline: one commit per
logical change, never squash/amend/force-push/rewrite history, and a
PROVENANCE.md entry in the *same* commit as any file adapted from
`reference/`.

See [CLAUDE.md](CLAUDE.md) for the complete set of rules this repository
is built under, and [DEFENSE.md](DEFENSE.md) for the running record of
design decisions and their tradeoffs.
