# CI

Two tiers, not one. C5 ("CI runs every test in `tests/`. Untested code is
not done.") is easy to read as "everything runs on every push" - it
doesn't. This page is the authoritative list of what actually runs
automatically versus what's manual-dispatch-only and why, so that
distinction is written down once instead of reconstructed from each
workflow file's own header comment every time it matters.

## Tier 1: automatic, on every push/PR (`.github/workflows/ci.yml`)

- **`constraints`** - C1 (no gap markers), C4 (Docker images pinned), and
  the no-fabricated-metrics check (DEFENSE.md #9).
- **`benchmark-readme-sync`** - fails if README.md's benchmark table
  doesn't match a fresh regeneration from `benchmarks/results/`.
- **`test`** - brings up the full Docker Compose stack and runs
  `pytest -q tests/` (unit + integration, including `test_smoke.py`).
  This is what actually satisfies C5 for anything under `tests/`.

## Tier 2: `workflow_dispatch`-only, never on push/PR

Each of these needs live infrastructure (Postgres/Kafka/Flink), takes
too long or is too destructive for a per-push gate, or - per V4 - is
meant to be dispatched repeatedly rather than trusted on one green run.
None of these running automatically would still leave them **unrun** by
default; they exist, but only execute when someone dispatches them.

| Workflow | Verifies | Why manual |
|---|---|---|
| `incidents-schema-verify.yml` | `reliability/store/incidents_schema.sql` applies cleanly and its CHECK constraints actually reject the bad writes they exist for (Part 4.1) | Postgres-only, fast and cheap - manual to match `metrics-store-verify.yml`'s pattern, not because it's slow |
| `metrics-store-verify.yml` | `reliability/store/schema.sql` applies and lands correctly | Same reason - fast, Postgres-only, but still not part of the per-push gate |
| `metrics-job-verify.yml` | The Flink metrics job (Kafka -> windowed aggregation -> Postgres, Part 3.1) against real data | Needs Kafka+Postgres+Flink; rebuilding the Flink image and running it is too slow for ci.yml |
| `replay-verify.yml` | `replay_producer.py` - timestamps, compression ratio, resume seam | Downloads a real ~59MB dataset and does three Kafka round trips; V4 wants this dispatched 5x, not trusted on one pass |
| `exactly-once.yml` | Kafka -> Flink -> Iceberg exactly-once delivery + TaskManager recovery | `verify_recovery.sh` SIGKILLs a running container - destructive, has no place in a gate that runs on every push |
| `volume-detector-verify.yml` | The volume detector's real-data accounting (Part 4.2/4.3) - every real window accounted for, the real DST fall-back hour excluded, zero false `scored` rows | Needs the full Kafka/Postgres/Flink stack plus a real ~59MB month download (reuses `verify_metrics_job.py`'s own population step, DEFENSE.md #48) - same cost profile as `metrics-job-verify.yml`, same reason it's manual |
| `freshness-detector-verify.yml` | The freshness detector's real-data accounting, same shape as the volume one | Same cost profile and same reason - full stack plus a real month download |
| `nullrate-detector-verify.yml` | The null-rate detector's real-data accounting, same shape as the volume one | Same cost profile and same reason |
| `benchmark.yml` | Nothing - it *measures*. Clean and injected contiguous replays through the full stack, producing the numbers the README publishes (Part 5) | The heaviest job here by a wide margin: three months of TLC data, two full replays, and two full scoring passes. Manual because it costs hours and because hard rule 12 says its output only lands when someone actually ran it |
| `scoring-probe.yml` | Nothing - it *sizes*. Times detector scoring against a directly-seeded `window_metrics`, isolated from replay | An instrument for choosing `benchmark.yml`'s span, not a gate. Postgres-only and quick, but it asserts nothing, so the merge rule below does not apply to it |

## Merge rule

**Any `workflow_dispatch`-only verification that covers a branch's
changes must be dispatched and green before that branch merges to
main.** The workflow existing, or having passed against some earlier
version of the code, doesn't satisfy C5 for this tier - a stale green
run proves nothing about the diff actually being merged. This is what
makes "manual" mean "run on demand" rather than "deferred
indefinitely."

Applies from this rule's introduction onward, not retroactively.
Work already merged under the previous norm - `exactly-once.yml` and
`metrics-job-verify.yml` were each verified 5x at merge time per V4 -
already satisfies this rule's intent; it isn't a reason to re-dispatch
against closed work.
