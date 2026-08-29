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
  requires this fallback be marked in the README, not just here — see
  README.md's "Lineage: declared, not runtime-emitted" section.

- **Sensitivity sweep** — the checkpoint interval is already read from an
  env var (`WEIR_CHECKPOINT_INTERVAL`) specifically so this can be explored
  later without touching `config/flink/config.yaml`. See DEFENSE.md #1.

- **Object store authentication** — SeaweedFS in `docker-compose.yml` runs
  with no `-s3.config` identity file baked in; `scripts/smoke_test.sh`
  instead configures one real identity live at runtime, via `weed
  shell`'s `s3.configure` (see DEFENSE.md #24). An earlier version of
  this entry claimed SeaweedFS "accepts any access key/secret pair" with
  no identity configured at all — that was wrong, and never actually
  backed by a signed S3 write reaching it; corrected once DEFENSE.md #24
  found the real behavior (it rejects every signed request outright with
  no identity configured). This is still fine for a local dev/CI stack,
  deliberately not fine for production: a real deployment would use IAM
  roles (or equivalent workload identity) for the object store, not a
  single static identity checked against a fixed key/secret pair. Not
  deferred as a "todo" so much as: this repo never intends to build its
  own credential/identity management for local SeaweedFS — that problem
  is solved by the cloud provider once this moves off Docker Compose.

- **Volume detector's sensitivity sweep can't vary `alpha` cheaply** —
  `weir_incidents.scored_windows` stores enough (`observed_value`,
  `baseline_mean_at_time`, `baseline_scale_at_time`) to re-derive a score
  at a different *threshold* as a pure SQL exercise, no baseline rewarm
  needed (DEFENSE.md #45). Varying `alpha` (the EWMA decay rate) is a
  different story: `alpha` controls how `ewma_mean`/`ewma_mad` themselves
  accumulate over time, so a different `alpha` value produces a genuinely
  different baseline trajectory, not just a different comparison against
  an already-computed one — there's no way to derive "what would the
  baseline have looked like under a different alpha" from data computed
  under the original alpha. A sweep over `alpha` needs a full 8-week
  rewarm per sweep point, same as the Flink-keyed-state alternative that
  was rejected for exactly this cost (DEFENSE.md #45). Threshold sweeps
  are cheap; alpha sweeps aren't — a real, stated limitation of this
  design, not an oversight.

- **Whether the volume detector flags a real incident against real,
  fully-warmed data is unverified.** `scripts/verify_volume_detector.py`
  (DEFENSE.md #48) checks the detector's accounting/plumbing against one
  real month of TLC data (2024-11, chosen for its real DST fall-back) -
  every window accounted for, the DST-ambiguous hour correctly excluded,
  zero false `scored` rows - but one month can't warm any bucket's
  8-week baseline (DEFENSE.md #44), so it structurally cannot exercise
  real incident detection. That's `benchmarks/`'s job, once built - full
  multi-month real data against the reproducible failure catalog, not
  this integration check's.

- **`replay_producer.py`/`metrics_job.sql` don't exclude the DST
  fall-back's ambiguous hour (Nov 3 2024, 01:00–01:59 local) the way
  `load_pickup_timestamps.py` does** (DEFENSE.md #44/#45) — so
  `weir_metrics.window_metrics` still contains that hour's data with no
  recoverable true UTC offset. The volume detector's reading adapter
  works around this by excluding those specific windows a second time,
  at its own ingestion boundary, rather than trusting an unverified
  default disambiguation. The inconsistency itself — one loader excludes
  the ambiguous hour, the live replay/metrics pipeline doesn't — is real
  and unaddressed; fixing it means touching `replay_producer.py` and/or
  `metrics_job.sql`, both already shipped and 5/5-verified (Part 2.1/3.1),
  which is a bigger, separate decision than this detector's own scope.
