# Defense

A running log of design decisions: what was chosen, the alternative that
lost, the tradeoff, and whether it can be explained cold — out loud, from
memory, without re-deriving it.

**Rule:** if an entry can't be written for a decision, the code implementing
that decision doesn't get committed. An unwritten entry means the tradeoff
hasn't actually been thought through yet.

## 1. Detector read path and detection latency floor

Detectors read from the live Kafka stream and from Postgres, not from
Iceberg. Iceberg is the durable record, not a read path for detection. The
one exception is the distribution-drift detector, which reads historical
aggregates from Iceberg — and that detector is deferred (see CLAUDE.md
Future Work), not built in Sprint 1.

This means the Flink checkpoint interval is **not** the detection-latency
floor for anything Sprint 1 actually ships. It only bounds staleness for the
one detector that reads committed Iceberg data, and that detector doesn't
exist yet. Given that, checkpoint interval was set to `60s`
(`execution.checkpointing.interval`, in `config/flink/config.yaml` via
the `WEIR_CHECKPOINT_INTERVAL` env var) for file sizing and recovery-window
reasons instead: 60s produces reasonably sized checkpoints/Parquet files
without checkpointing so rarely that a TaskManager failure replays an
excessive amount of stream. `min-pause: 30s` prevents checkpoints from
backing up into each other if one runs long under load. `tolerable-failed-
checkpoints: 3` absorbs a transient object-store hiccup (SeaweedFS) without
forcing a job restart.

**Alternative considered and rejected:** tune the interval down (5–10s) now
to pre-emptively lower the future drift detector's staleness floor. Rejected
because (a) that detector isn't in Sprint 1 scope — optimizing for it now is
premature, (b) a shorter interval has a real, immediate cost (smaller
Parquet files, more checkpoint-barrier and object-store PUT overhead) traded
against a latency requirement that doesn't exist yet, and (c) the interval
is read from an env var specifically so the (also deferred) sensitivity
sweep can explore this tradeoff empirically later, instead of us guessing a
number now.

## 2. Watermark bound derivation

## 3. Exactly-once across TaskManager failure

## 4. Baseline strategy and its systematic blind spot

## 5. Benchmark scenarios vs dev triggers

## 6. Declared lineage in v1, and what runtime emission requires

## 7. False-positive tolerance and operating point

## 8. What the benchmark missed and why

## 9. Benchmark-metrics guard: narrow negative check + positive sync check

Chose: narrow `scripts/check_no_metrics_in_markdown.sh` to only flag the
specific phrases "detection rate", "false positive rate" / "false-positive
rate", "detection latency", and "throughput" when followed by a number,
outside `benchmarks/results/`. Paired with a positive guarantee:
`scripts/sync_benchmark_readme.py`, which regenerates the README's
benchmark table directly from the newest `benchmarks/results/*.json`, plus
a CI job that fails if the committed block doesn't match a fresh
regeneration.

Alternative rejected: keep the original broad check — any percentage or
latency-shaped number, anywhere in markdown outside `benchmarks/results/`.
Rejected because it produced false positives on legitimate content: this
file's own entry #1 discusses checkpoint-interval values ("60s", "30s"),
which are config, not a detection-performance claim — but a blunt regex
can't tell the difference between the two.

Tradeoff: the narrowed negative check is now weaker standing alone — a
fabricated number that avoids those exact phrases would slip past it. That
weakness is deliberate, not overlooked: the negative check is a cheap
backstop, not the real guarantee. The real guarantee is the sync script —
it makes it structurally impossible for the README's benchmark table to
say anything other than what the newest actual result file contains,
because the table is generated from that file's content, not typed by
hand. A number that wasn't computed by the sync script from a real result
file doesn't appear in the table at all.

## 10. Host->container file permissions: the Flink image runs as non-root

Chose: any file crossing the host->container boundary into the Flink
image must be world-readable (or explicitly chowned to match the
container's runtime user) at the point of creation, not fixed up after
the fact. In `scripts/smoke_test.sh` this is `make_readable_tmp()` -
`mktemp` followed immediately by `chmod 644`, used for any future
runtime-generated file that needs to reach this container.

Alternative rejected (found the hard way, not anticipated): create temp
files with `mktemp`'s default mode (600, owner-only) and copy them in via
`docker compose cp`. This is what smoke test steps 4 and 5 originally
did, and it broke: `docker compose cp` preserves the source file's
permission bits into the container, and `docker/flink/Dockerfile` ends
with `USER flink` (non-root) - a mode-600 file is unreadable by that user
regardless of which UID actually ends up owning it after the copy.
Confirmed via an actual sql-client crash, not inferred:
`FileNotFoundException: /tmp/weir_smoke_step4.sql (Permission denied)`
immediately after the log showed the same file being copied successfully.

Tradeoff / why this is recorded rather than just fixed silently: this
isn't specific to one script, and it will recur. Any future job, tool, or
CI step that stages a file into this Flink image hits the same wall
unless it either (a) chmods to world-readable at creation, or (b) avoids
the host->container copy entirely. Steps 4 and 5 actually ended up doing
(b): their SQL turned out to be static, not templated, so it moved to
committed files in `scripts/sql/`, bind-mounted read-only at
`/opt/weir/sql` - reviewable DDL in git, not generated at runtime. (a) -
`make_readable_tmp()` - stays in the script for whatever does need a
runtime-generated file later, e.g. step 6's exactly-once verification
script.
