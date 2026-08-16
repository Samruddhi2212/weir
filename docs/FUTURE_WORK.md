# Future Work

Deferred out of Sprint 1, per CLAUDE.md. These are in the locked stack —
eventually built — just not now. Not built, not stubbed — no placeholder
directories or files exist for these.

- **`agent/` (Weir-built triage/explanation agent)** — in the locked stack,
  deferred out of Sprint 1. The differentiator this project ships first is
  measured detection/FP/latency performance against a reproducible failure
  catalog; an agent for triage or explanation is a layer on top of that, not
  a precondition for it. When it is built, it's built in-house — CLAUDE.md
  excludes any third-party agent framework, permanently, not just for
  Sprint 1.

- **`infrastructure/terraform/` (Terraform)** — in the locked stack,
  deferred out of Sprint 1. Sprint 1's actual deployment is Docker Compose
  only; cloud deployment is out of scope until the local, reproducible
  benchmark harness itself is validated.

- **Prometheus and Grafana dashboards** — in the locked stack, deferred out
  of Sprint 1. OpenTelemetry instrumentation itself is *not* deferred —
  detection latency is a published metric and must be instrumented from
  the start. What's deferred is wiring that telemetry into Prometheus
  scraping and Grafana dashboards; the data gets emitted in Sprint 1 either
  way.

- **`reliability/distribution/` (distribution drift detector)** — the one
  detector design that reads historical aggregates from Iceberg rather than
  the live stream/Postgres path the rest of the detectors use (see
  DEFENSE.md #1). Building it now would pull in Iceberg-read-path and
  freshness questions ahead of the fixed deadline.

- **Runtime lineage emission** — v1 ships declared/static lineage, not jobs
  instrumented to emit lineage events at execution time. Runtime emission
  needs every ingestion/streaming job instrumented against an event shape
  (see PROVENANCE.md's OpenLineage design reference), which is a larger
  integration surface than Sprint 1's timeline allows. CLAUDE.md rule 7
  requires this fallback be marked in the README, not just here — **not yet
  satisfied**, since no README.md exists in this repo yet.

- **Sensitivity sweep** — the checkpoint interval is already read from an
  env var (`WEIR_CHECKPOINT_INTERVAL`) specifically so this can be explored
  later without touching `config/flink/flink-conf.yaml`. See DEFENSE.md #1.
