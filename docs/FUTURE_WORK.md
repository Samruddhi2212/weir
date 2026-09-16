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

- **When `benchmarks/`'s runner is built, it must never call
  `run.py`/`run_once` with `assume_no_more_arrivals=True`** (DEFENSE.md
  #51). That flag skips the lag buffer entirely - fine for
  `verify_volume_detector.py`'s already-complete historical file, but
  it would make the benchmark's own detection-latency number
  meaningless (every window scored the instant it appears, measuring
  batch-processing speed instead of streaming latency) without
  anyone having tuned or fabricated anything - just by calling this
  flag from the wrong caller. If the benchmark runner hits the same
  non-convergent-tail symptom #51 describes, the fix is a synthetic
  trailing window past the frontier, not disabling the buffer.

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

- **RESOLVED (root cause found, fixed): `metrics_job.sql`'s
  `AVG(passenger_count)` produced a wrong value
  against a real November 2024 window** - confirmed directly against
  the source parquet: 132 real rows, 25 null, non-null values 1-5,
  true mean 1.439; Postgres reported 1.0. `MIN`/`MAX` for the same
  column, same window, were both correct, and `window_metrics.row_count`
  and every null_count matched - so this isn't corrupted source values
  or a window-boundary mismatch, something in the `AVG` computation
  itself. Never surfaced before because CI had only ever exercised this
  job's Stage 8 deep-check against 2025-01. **Root cause, found when the
  benchmark hit it a second time with different numbers: integer
  division.** `passenger_count` is declared `BIGINT` and Flink's `AVG`
  over an integer type returns that integer type, truncating - 154/107
  stored as 1.0, and 13/6 stored as 2.0 in the second occurrence. It is
  the only integer column among the means, which is why every other
  `*_mean` was correct, and why `MIN`/`MAX` on this same column were
  correct throughout: they never divide. Fixed by casting to DOUBLE
  before the aggregate. No detector consumed this value (volume reads
  `row_count`, freshness `window_end`, null-rate `null_count`), so no
  published detector number was affected by it.

- **Event-delay drift detection** - a detector scoring `lateness_p50/
  p99/max_seconds` (actual event-time delay distribution, distinct from
  the freshness detector's arrival-gap signal) is blocked on those
  `window_metrics` columns never being populated by `metrics_job.sql`
  (DEFENSE.md #52). Real, scoped-out work, not forgotten - needs the
  watermark/lateness computation wired into the Flink job first.

- **Repeated-run variance is unmeasured for the published contiguous
  run.** V4's five-run requirement is deliberately relaxed for
  `run_benchmark.py` (CLAUDE.md): the pipeline is deterministic given the
  same input, so repetition re-verifies one scenario rather than sampling
  a distribution. What is known comes from five runs under the superseded
  band-sampled method: scenarios flagged, the spurious-incident count and
  warmed buckets were bit-identical across all five, while time-to-flag
  and the unattributed-incident count were not. That non-determinism is a
  race between replay pacing and watermark advance - whether a given late
  record beats the watermark - so those two figures are the ones a rerun
  would most likely move. Quantifying the spread under contiguous replay
  needs repeated full runs at roughly 4.4h each, which is the cost this
  relaxation is trading away.

- **Attribution cannot separate an injected failure from incidental
  late-drop.** An incident is attributed to a scenario when the detector
  matches and the window falls in the scenario's declared span. Both the
  injected partition degradation and incidental late-drop produce the
  same observable - fewer rows than the baseline expects - so a run that
  happens to lose late data inside a scenario's window will flag earlier,
  and the measured time-to-flag will be shorter for a reason that has
  nothing to do with the scenario. Fixing this needs per-window
  expected-vs-landed accounting inside the injected phase, not just the
  aggregate bound the runner asserts today.

- **The freshness detector cannot see a delay failure that thins a
  stream without stopping it.** Now measured rather than assumed: against
  `freshness_gradual_delay` it reached a strongest score of 0.01 against
  a 3.5 threshold across 763 scored windows - effectively no deviation at
  all, not a near miss. Its signal is the gap between consecutive
  `window_metrics` rows, and as long as some events still land in every
  minute, windows keep closing once a minute no matter how many events
  were dropped for crossing the watermark bound. Catching this class of
  failure needs a signal that reflects *how much* data arrived late or
  not at all, not just whether a window closed - which is the
  event-lateness signal in the "Event-delay drift detection" entry
  above, blocked on the lateness columns the current pipeline never
  populates (DEFENSE.md #52).

- **RESOLVED (root cause found, fixed): benchmark runtime grew with
  window count.** Runs 34757520031 and 34885328884 were both killed at
  the CI ceiling. Cause: `fetch_eligible_windows` left its read
  transaction open, so `process_window`'s `conn.transaction()` nested as
  a SAVEPOINT and every window accumulated in one never-committed
  transaction, pushing Postgres past its 64-subtransaction cache. Scoring
  decayed from ~172/s to ~14/s and reset at each detector boundary, which
  is what identified it. Fixed by `adapter.release_snapshot`; the
  published run then scored a phase in 776s rather than not finishing in
  four and a half hours. The fix also restores the per-window crash
  resumability `process_window` documents, which is now covered by
  `tests/integration/test_detector_crash_resumability.py`. Full account
  in DEFENSE.md #55 and in README.md's design-decisions section.
