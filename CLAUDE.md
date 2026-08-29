# Weir

Streaming data reliability platform. Detects, localizes, and explains data
incidents.

**Differentiator:** publishes measured detection rate, false-positive rate,
and detection latency against injected failures. No comparable OSS project
does this.

## Locked stack

Kafka, Apache Flink, Apache Iceberg, S3-compatible object store, PostgreSQL,
FastAPI, Terraform, Docker Compose, OpenTelemetry, Prometheus, Grafana, and
a Weir-built triage/explanation agent.

Locked means eventually built, not necessarily Sprint 1. Sprint 1 scope
within this stack: Terraform and the agent are deferred (see
docs/FUTURE_WORK.md). OpenTelemetry instrumentation is in Sprint 1 —
detection latency is a published metric and must be instrumented from the
start. Prometheus and Grafana dashboards are deferred.

**Excluded:** Spark, Trino, dbt, Airflow, Kubernetes, Superset, any
third-party agent framework — the agent above is built in-house, never
bought off the shelf.

## Constraints (mechanically enforced)

These are enforced by a pre-commit hook and a CI job, not just this file.
C3 (benchmarks/ never importing incidents/dev/, hard rule 2 below) is also
mechanically enforced but isn't renumbered here since its content already
lives in Hard rules — flagging in case a C3 entry was expected in this
section specifically.

- **C1** — No `MISSING_VALIDATION` / `MISSING_TEST` / `TODO` / `FIXME`
  markers left in shipped code. If something is incomplete, it either gets
  finished or removed and listed in docs/FUTURE_WORK.md. There is no third
  state. Enforced by `scripts/check_no_gap_markers.sh`.
- **C2** — No orphan directories for excluded or deferred scope. A
  directory only exists once real work starts in it.
- **C4** — Pin every Docker image to an explicit version. No `:latest`,
  ever. Enforced by `scripts/check_pinned_images.sh`.
- **C5** — CI runs every test in `tests/`. Untested code is not done.

## CI

Two tiers: what runs automatically on push/PR, and five
`workflow_dispatch`-only verification workflows that don't. See
[docs/CI.md](docs/CI.md) for the full list and why each one is manual.

- **C6** — A `workflow_dispatch`-only verification covering a branch's
  changes must be dispatched and green before that branch merges to
  main. The workflow existing, or a stale green run against older code,
  does not satisfy C5 for this tier.

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

## Process

P5. If a pasted instruction conflicts with CLAUDE.md or DEFENSE.md, stop
    and ask which governs. Never silently follow the newer paste.

(P1–P4 aren't recorded here — only P5 was given. If a fuller Process list
exists, paste it and I'll fill the gap instead of guessing at it.)

## Verification Standard (every script, every test)

Distilled from this week's real bugs, not written speculatively — each
one shipped, was caught, and is recorded in DEFENSE.md under its own
number.

- **V1.** Assertions compare exact parsed values. Never grep a pattern
  against mixed output — we shipped a check matching `2` in jar version
  strings that passed without verifying anything (DEFENSE.md #14).
- **V2.** Every external command gets an explicit timeout. On timeout,
  dump the relevant container logs before exiting non-zero.
- **V3.** Print full raw output on success as well as failure. A
  passing run whose output was never shown cannot be audited
  retroactively.
- **V4.** A result is real only after N clean consecutive runs. N=5 for
  anything labeled a gate. One green run means nothing.
- **V5.** Ground truth comes from the strongest available source. A
  broker-confirmed delivery callback beats "we called `send()`."
- **V6.** Failures and expected-failures must be distinguishable in
  logs. A failed send during a kill test is not data loss and must not
  read as a gap.
- **V7.** Ground truth is computed INDEPENDENTLY of the system under
  test. Expected values come from the source data or the emission log,
  never from the pipeline being verified.
- **V8.** Unaccounted-for records must be explained, not tolerated. If
  713 of 6000 events aren't in the result, the test states why and
  asserts that reason.

## Security

- **S1.** Never paste a token, key, or credential into a message, log,
  script, or commit. Env vars only.
- **S2.** gitleaks or detect-secrets runs in pre-commit and CI.

## Design Before Code

D1. For any non-trivial component, the DEFENSE.md entry explaining the
    design and its rejected alternative is written BEFORE the
    implementation. If the explanation can't be written, the code isn't
    written yet.

## Environment Gotchas (learned the hard way)

- **E1.** Containers run as non-root (`USER flink`). Anything crossing
  host->container must be world-readable or explicitly chowned. Prefer
  committed, bind-mounted files over runtime-generated temp files.
- **E2.** A mutable tag is not a pin. Digest-pin every image.
- **E3.** Flink 2.x uses `config.yaml`, not `flink-conf.yaml`.
- **E4.** Iceberg's Flink catalog factory requires Hadoop's
  `Configuration` class unconditionally; shaded `hadoop-client-api`/
  `-runtime` jars are required.
- **E5.** A Flink source that completes immediately having read zero
  records is a BOUNDED source, not a failing one. Check
  `scan.bounded.mode`, `execution.runtime-mode`, and
  `scan.startup.mode` before log-diving.
- **E6.** Diagnostic settings leak. Anything added to isolate a bug
  gets removed in the same session or explicitly recorded in
  DEFENSE.md.
- **E7.** Mounting a config directory into the Flink container
  *replaces* the image's own conf directory - it does not merge with
  it. This silently discarded the image's default
  `log4j-console.properties` (near-total loss of console logging) and
  its default `env.java.opts.all` (Java 17+/21 `--add-opens`/
  `--add-exports` flags, without which Kryo's reflection-based
  checkpoint serialization throws `InaccessibleObjectException`) - two
  separate real outages from the same cause. Any config file mounted
  this way must carry forward everything the image's own default
  provided, not just the keys this project cares about, and that has
  to be verified explicitly (diff against the image's own default file,
  don't assume a hand-written file is complete).
- **E8.** An idle source subtask holds back the merged watermark. The
  durable fix is `table.exec.source.idle-timeout`, NOT parallelism=1.
- **E9.** Calcite's `INTERVAL` grammar defaults to 2-digit precision.
  Use `SECOND(3)`.
- **E10.** Python in CI needs `-u` or stdout buffering swallows
  diagnostics on failure.
- **E11.** Postgres composite-type inference in
  `UNNEST(ARRAY[ROW(...)])` is unreliable. Use `VALUES (...) AS
  m(cols)`.
