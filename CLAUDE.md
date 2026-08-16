# Weir

Streaming data reliability platform. Detects, localizes, and explains data
incidents.

**Differentiator:** publishes measured detection rate, false-positive rate,
and detection latency against injected failures. No comparable OSS project
does this.

## Locked stack

Kafka, Apache Flink, Apache Iceberg, S3-compatible object store, PostgreSQL,
FastAPI, Terraform, Docker Compose, OpenTelemetry, Prometheus, Grafana.

**Excluded:** Spark, Trino, dbt, Airflow, Kubernetes, Superset, any agent
framework.

## Hard rules

1. NEVER invent, estimate, or placeholder a metric value. Detection rates,
   FP rates, and latencies come only from running
   `benchmarks/run_benchmark.py`. No plausible-looking numbers in READMEs,
   docstrings, comments, or test fixtures that could leak into docs.
2. NEVER let `benchmarks/` import from `incidents/dev/`. Enforce with a
   test.
3. NEVER tune a detector to pass a benchmark scenario. If it misses, it
   misses, and the miss gets reported.
4. Any file adapted from `reference/` gets a PROVENANCE.md entry in the
   same commit.
5. When making a design choice with a real alternative, state the
   alternative and the tradeoff BEFORE implementing.
6. Never add a dependency without asking.
7. Lineage is emitted at runtime. Never fall back to a hand-written YAML
   graph without telling me explicitly and marking it in the README.

## Commit discipline

8. After every logically complete unit of work, propose a commit: show me
   the diff summary and a message, then WAIT for my confirmation. Never
   commit without my explicit go-ahead — I read the diff before it lands.
9. One commit = one logical change. Never bundle unrelated work.
10. Conventional Commits format:
    `feat|fix|refactor|test|docs|chore|infra|bench(scope): summary`
    Body: what changed and WHY. Include the tradeoff if there was one.
    Footer: `Provenance: adapted from streaming-lakehouse-lab <file>` where
    relevant.
11. Never squash, amend, force-push, or rewrite history. The history is
    evidence of how this was built and it stays intact, including the ugly
    parts.
12. Never commit generated benchmark results without me having actually run
    the benchmark that produced them.
