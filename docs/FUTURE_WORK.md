# Future Work

Deferred out of Sprint 1, per CLAUDE.md. Not built, not stubbed — no
placeholder directories or files exist for these.

- **`agent/` (AI agent)** — CLAUDE.md excludes agent frameworks from Sprint 1
  entirely. The differentiator this project ships first is measured
  detection/FP/latency performance against a reproducible failure catalog;
  an agent for triage or explanation is a layer on top of that, not a
  precondition for it.

- **`infrastructure/terraform/` (Terraform/AWS)** — Sprint 1's stack is
  Docker Compose only. Cloud deployment is out of scope until the local,
  reproducible benchmark harness itself is validated.

- **`reliability/distribution/` (distribution drift detector)** — the one
  detector design that reads historical aggregates from Iceberg rather than
  the live stream/Postgres path the rest of the detectors use (see
  DEFENSE.md #1). Building it now would pull in Iceberg-read-path and
  freshness questions ahead of the fixed deadline.

- **Runtime lineage emission** — v1 ships declared/static lineage, not jobs
  instrumented to emit lineage events at execution time. Runtime emission
  needs every ingestion/streaming job instrumented against an event shape
  (see PROVENANCE.md's OpenLineage design reference), which is a larger
  integration surface than Sprint 1's timeline allows.

- **Sensitivity sweep** — the checkpoint interval is already read from an
  env var (`WEIR_CHECKPOINT_INTERVAL`) specifically so this can be explored
  later without touching `config/flink/flink-conf.yaml`. See DEFENSE.md #1.
