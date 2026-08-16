# Weir

Streaming data reliability platform. Detects, localizes, and explains data incidents.

**Differentiator:** publishes measured detection rate, false-positive rate, and
detection latency against a reproducible injected-failure catalog. Comparable
OSS tools (Great Expectations, Soda, Deequ, OpenMetadata, Marquez) define
checks; none publish measured detection performance.

**Deadline:** end of August 2026. Sprint 1 scope is fixed. Do not expand it.

## Stack

**Sprint 1:** Kafka, Flink, Iceberg, S3-compatible store, PostgreSQL, FastAPI,
Docker Compose, GitHub Actions.

**Deferred** (Future Work section, not built): Terraform/AWS, AI agent, runtime
lineage emission, distribution drift detector, sensitivity sweep.

**Never:** Spark, Trino, dbt, Airflow, Kubernetes, agent frameworks.

## Hard rules

1. NEVER invent, estimate, or placeholder a metric. Detection rates, FP rates,
   and latencies come only from running `benchmarks/run_benchmark.py`.
2. NEVER let `benchmarks/` import from `incidents/dev/`. Enforce with a test.
3. NEVER tune a detector to pass a benchmark scenario. Misses get reported.
4. Pin every Docker image to an explicit version. No `:latest`, ever.
5. No `MISSING_VALIDATION` / `MISSING_TEST` / `TODO` markers left in shipped
   code. If something is incomplete, it either gets finished or it gets
   removed and listed in README Future Work. There is no third state.
6. Anything adapted from `reference/` gets a PROVENANCE.md entry in the same
   commit.
7. When a design choice has a real alternative, state the alternative and the
   tradeoff BEFORE implementing.
8. Every module ships with tests, and CI runs them. Untested code is not done.

## Commit discipline

- After each logical unit: show me the diff summary and a proposed message,
  then WAIT for my confirmation. Never commit unprompted.
- Conventional Commits: `feat|fix|refactor|test|docs|chore|bench(scope): summary`
- Body explains WHY, including tradeoffs. Footer `Provenance:` where adapted.
- Never squash, amend, or force-push. History is evidence.
- Commit failures and their fixes as separate commits.
