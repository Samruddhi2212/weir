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

## 11. Predicted dependency risk vs. actual dependency risk

`docker/flink/Dockerfile`'s own comments named a specific "known risk"
before this was ever run: whether `flink-sql-connector-kafka`'s `-2.0`
build would load against a 2.1.0 cluster. It did - smoke test step 4
(Flink reads Kafka, no Iceberg) passes clean in CI. The dependency that
actually broke was a different one, assumed away entirely rather than
flagged.

The exact failure, from `CREATE CATALOG ... WITH ('catalog-type'='jdbc',
'io-impl'='...S3FileIO', ...)`:

```
Caused by: java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
    at org.apache.iceberg.flink.FlinkCatalogFactory.clusterHadoopConf(FlinkCatalogFactory.java:214)
    at org.apache.iceberg.flink.FlinkCatalogFactory.createCatalog(FlinkCatalogFactory.java:141)
Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration
```

Before adding anything, checked Iceberg 1.11.0's own source for
`FlinkCatalogFactory.createCatalog()`:

```java
public Catalog createCatalog(Context context) {
    return createCatalog(context.getName(), context.getOptions(), clusterHadoopConf());
}
```

`clusterHadoopConf()` is called unconditionally - no branch on
catalog-type or io-impl. That answers the question directly: it's the
catalog factory requesting the class, not Iceberg's FileIO and not
Flink's own filesystem layer, and S3FileIO does not avoid it, because the
call happens before FileIO is ever consulted. This is also a known,
still-open upstream limitation (`apache/iceberg#7332`, "Flink: Make
Hadoop an optional dependency") - not something introduced by this setup
and not something a config flag turns off.

Fixed by adding Hadoop 3.x's shaded `hadoop-client-api`/`hadoop-client-
runtime` (the minimal-footprint pair that supplies the one needed class
without unshaded `hadoop-common`'s full transitive dependency chain),
pinned to 3.4.3 - verified against Iceberg 1.11.0's own
`gradle/libs.versions.toml` (`hadoop3 = "3.4.3"`), not assumed from Maven
Central's newest (3.5.0). Mixing an unverified newer Hadoop build against
an older-tested Iceberg release is exactly how shaded-jar conflicts
resurface silently.

Why this is recorded rather than just fixed and moved on: it's evidence
about *where* dependency risk actually lives in this stack, not just a
bug. The risk I predicted in advance (Kafka connector versioning) turned
out fine; the risk that actually broke (a transitively-required Hadoop
class inside Iceberg's Flink catalog factory) wasn't on my radar at all.
That's the justification for the layered smoke test (steps 2 through 5
each verified independently) instead of trusting a clean `docker build` -
in this stack, a successful build and even a passing earlier layer (step
4) say nothing about whether the next layer (step 5) works. Each
dependency boundary has to be verified on its own; guessing which one is
risky in advance is not reliable enough to skip verifying the others.

## 12. Kafka CLI scripts need absolute paths - PATH doesn't include them

Kafka's healthcheck (`kafka-broker-api-versions.sh --bootstrap-server
localhost:9092`) never passed, and `scripts/smoke_test.sh`'s calls to
`kafka-topics.sh`/`kafka-console-producer.sh`/`kafka-console-consumer.sh`
would have failed for the identical reason, unverified until this was
actually run.

The tell, not a guess: Kafka's own container logs showed a completely
clean startup - zero errors or warnings, `[BrokerServer id=1] Transition
from STARTING to STARTED`, `Kafka Server started`, listening on
`0.0.0.0:9092`, all within about 5 seconds of container start. Docker
still reported the container unhealthy after exhausting the full retry
budget (30s start_period + 5x10s retries = ~80s). When an app's own logs
show total success but its healthcheck still fails every time, suspect
the healthcheck command before suspecting the app - a broken app usually
leaves evidence in its own logs, a broken healthcheck command leaves
none, because the app was never actually asked anything.

Confirmed directly, via a temporary CI job that exec'd into the running
container (removed once the root cause was found - see git history):
`which kafka-broker-api-versions.sh` returned exit 1; `echo $PATH` was
`/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:
/sbin:/bin` - no `/opt/kafka/bin`; the identical script invoked by
absolute path succeeded in 2.24s with a full, valid API-versions
response. That single data point also ruled out the two other candidate
explanations before spending time on them: not a networking issue (it
connected and got a real response), not a timing issue (2.24s, nowhere
near the 5s timeout).

Fixed by using the absolute path `/opt/kafka/bin/kafka-broker-api-
versions.sh` (and the same prefix for the other three scripts) everywhere
Kafka's CLI is invoked from outside a shell that's already sourced
Kafka's own environment. The reference repo's compose file has the
identical bare-name healthcheck, also never actually run - this bug
would exist there too, just never discovered.

## 13. Flink 2.0+ requires config.yaml, not flink-conf.yaml, and a writable conf mount

`flink-jobmanager` exited immediately on startup:

```
Exception in thread "main" org.apache.flink.configuration.IllegalConfigurationException:
The Flink config file '/opt/flink/conf/config.yaml' (/opt/flink/conf/config.yaml) does not exist.
```

A config file existed at that mount point - just named `flink-conf.yaml`,
matching both this project's own prior file and the name used throughout
`reference/`'s `build.gradle.kts`-adjacent tooling. Per Apache Flink's
FLIP-366 and the 2.0 release notes: `flink-conf.yaml` (the legacy flat
`key: value` format) is not read at all starting in Flink 2.0 - not
deprecated-with-fallback, simply never looked for. `config.yaml`'s
"standard YAML" format still accepts the same flat dotted keys
(`execution.checkpointing.interval: 60s`), so no content restructuring
was needed, only the filename.

The same crash's log carried a second, compounding cause, found together,
not in a separate debugging pass: `config-parser-utils.sh: line 42: ...
Read-only file system`. Flink's docker entrypoint needs to *write* into
the conf directory at container start, to merge the `FLINK_PROPERTIES`
env var (this project's mechanism for an overridable checkpoint interval,
see DEFENSE.md #1) into `config.yaml`. The mount was `:ro`. This matches
a previously-filed, nearly identical upstream issue
(`GoogleCloudPlatform/flink-on-k8s-operator#213`, "cannot create
flink-conf.yaml.tmp: Read-only file system") almost verbatim.

Fixed by renaming `config/flink/flink-conf.yaml` -> `config/flink/
config.yaml` (zero content changes) and dropping `:ro` from both Flink
services' conf mount in `docker-compose.yml`.

Why this is recorded with both causes together: fixing only the filename
would have traded one crash for a different one - the read-only write
failure - on the very next attempt, looking like a second unrelated bug
instead of the second half of the same one. And: never assume a config
file's format or name is stable across a major version bump just because
the material you're adapting from used it - Flink 2.0 is a genuine
breaking change here, not a deprecation warning reference/ would have
caught either, since reference/ was never actually run against Flink
2.1.0 in this form.

## 14. A test that passes for the wrong reason is worse than no test

Step 5's row-count check was `grep -qE '\b2\b'` against the *entire*
captured stdout+stderr of the sql-client invocation - "does the digit 2
appear anywhere in this output." It does, almost unconditionally: jar
version strings like `flink-table-api-java-uber-2.1.0.jar` appear in
essentially any Flink stack trace, error or not, and `\b2\b` matches the
`2` in `2.1.0` (the dot on either side is a non-word character, so it's a
word boundary). A run that printed `PASS: step 5 - wrote 2 rows to
Iceberg, count verified` had, in fact, verified nothing about row count -
it had verified that Flink's own version number appeared in the log,
which it always does.

This was caught by contradiction, not inspection: Iceberg 1.11.0's
`FlinkCatalogFactory.createCatalogLoader()` accepts exactly `hive`,
`hadoop`, or `rest` for `catalog-type`, with a `default` branch that
unconditionally throws `UnsupportedOperationException` for anything else
- `jdbc` (what this project's catalog config uses) has never been a
valid value, under any condition, in that source. A run that used the
identical committed config nonetheless printed a clean pass. The two
facts don't fit together. Direct re-inspection of that run's saved raw
log confirmed zero occurrences of `UnsupportedOperationException` or
`Unknown catalog-type` anywhere in it - so the assertion bug wasn't
masking that specific error in that run, but it was never actually
capable of catching it, or anything else, either.

Step 4 had a milder version of the same class of bug:
`[ -n "$STEP4_OUTPUT" ]` ("did the process print anything at all") is
nearly tautological, since Flink's SQL Client prints substantial banner
and log output regardless of whether the query underneath succeeded.

Fixed by making both queries emit a sentinel-prefixed value instead of a
bare one - `CONCAT('WEIR_ROW_COUNT=', CAST(COUNT(*) AS STRING))` and
`CONCAT('WEIR_MSG=', message)` - so the shell side can `grep -oE` the
exact literal prefix, parse the value after it, and compare it exactly
(`= "2"`, count of matches `= "5"`) rather than pattern-match loosely
against unrelated content. Both now also explicitly fail if the marker
doesn't appear at all, rather than treating "empty" as an implicit pass.

Why this gets its own entry rather than being folded into the fix commit:
a false-positive test is a worse failure mode than a missing test. A
missing test is a known, visible gap - nobody can point to it and claim
step 5 is verified. A test that reports PASS while checking nothing
manufactures false confidence, and every fix built on top of that
confidence (this project was about to move on to building step 6 on the
assumption that step 5 was solid) inherits the same unearned certainty.
The discipline this argues for: when a check's *positive* case can't be
falsified by a wrong answer, the check isn't verifying that case, however
often it happens to be right.

## 15. Digest-pinning every image, and capturing the jar manifest on every run

Prompted by the same step-5 inconsistency as #14, but addressing a
different, still-not-fully-ruled-out hypothesis: two CI runs against an
unchanged `docker/flink/Dockerfile` produced different outcomes for the
identical `catalog-type='jdbc'` config. Fixing the assertion bug (#14)
means the failing run's result is now trustworthy - but it doesn't by
itself explain why an earlier run differed, since that earlier run's own
assertion, though broken, wasn't concealing this specific error (verified
directly against its saved log - see #14).

One remaining, concrete, checkable explanation: every image in this
project, including the `flink:2.1.0-java21` base in `docker/flink/
Dockerfile`, was pinned by tag only. A tag is not a stable identifier -
the same tag can be repushed with different underlying content later,
and neither `docker-compose.yml` nor the Dockerfile would show any
difference in that case, even though the actual bytes pulled could
differ between two runs on two different days. This isn't a hypothetical
concern specific to Flink; it's a general property of registry tags, and
"pin every Docker image to an explicit version" (CLAUDE.md C4) was never
actually enforced to the standard that rules it out, only to the weaker
standard of "not `:latest`."

Fixed by resolving each image's current manifest digest directly from
Docker Hub's registry API (not assumed, not copied from a build log) and
pinning `image:tag@sha256:digest` - Docker's supported combined form,
where the digest determines what's actually pulled and the tag stays for
human readability. Strengthened `scripts/check_pinned_images.sh` (C4) to
require this for every pulled image (excluding `weir/*`, which are built
locally from this repo's own Dockerfile and have no external digest to
pin against - their reproducibility comes from the pinned base image and
pinned jar versions inside the build, not from the resulting local image
having one). Also added an unconditional (not failure-only) CI step that
captures `ls -la /opt/flink/lib/` and `sha256sum` of every jar in it on
every run, so that if this inconsistency recurs, the actual jar content
of the passing and failing runs can be diffed directly against each
other, rather than reasoned about from the outside.

Digest-pinning removes the mutable-tag hypothesis as a possible
explanation going forward. It does not, by itself, prove that mutable
tags caused the *original* inconsistency - that would require having
captured both runs' jar manifests at the time, which this project didn't
do until now. Recorded as ruled out prospectively, not retroactively
confirmed.

## 16. sql-client.sh -f requires an explicit result-mode for non-interactive SELECT

The very first re-run attempting to actually verify #14's fix failed
before step 5 ever ran, at step 4, with a new error the CONCAT-wrapped
query had never hit before:

```
org.apache.flink.table.client.gateway.SqlExecutionException: In
non-interactive mode, it only supports to use TABLEAU as value of
sql-client.execution.result-mode when execute query. Please add 'SET
sql-client.execution.result-mode=TABLEAU;' in the sql file.
```

Checked before assuming the fix: did the earlier, genuinely-passing run
(the one #14/#15 already re-inspected directly) hit this too, silently
swallowed by the old weak assertion? Re-grepped that saved log for
`SqlExecutionException`/`result-mode` - zero occurrences. So this isn't
the same masked bug recurring; it's a real difference in behavior
between a bare `SELECT * ...` (which the earlier run used and which
apparently doesn't require an explicit result-mode) and the sentinel-
wrapped `SELECT CONCAT(...)` query introduced by the #14 fix, run through
the identical `sql-client.sh -f` / non-interactive mechanism both times.

Fixed exactly as the error message itself prescribes: added `SET
'sql-client.execution.result-mode' = 'TABLEAU';` to both
`scripts/sql/smoke_step4.sql` and `smoke_step5.sql`, the latter alongside
its existing `SET 'execution.runtime-mode' = 'batch';`.

Why this matters for the still-open #14/#15 investigation, not just as
its own bug: it's a second, independent case of a SELECT query's success
depending on a setting that wasn't explicit before - one more concrete,
confirmed reason a query can behave differently across two runs that
look identical from the SQL's WITH()-clause content alone. It does not
explain the original catalog-type inconsistency (a completely different
exception, at catalog-creation time, before any SELECT executes), but it
reinforces the same posture #15 already argued for: state every
execution-mode assumption explicitly in the SQL itself, rather than
relying on whatever Flink's client happens to default to.

## 17. Unbounded sql-client invocations turned a hang into a 20-minute silent cancellation

The first attempt at the 5-trial re-verification (after #16's fix)
produced neither a pass nor a clear fail: the CI job ran for 20 minutes -
its full `timeout-minutes` cap - printed zero output from `pytest -q
tests/` the entire time, and was cancelled by GitHub Actions rather than
failing on its own.

Why zero output: `subprocess.run(..., capture_output=True)` in
`tests/integration/test_smoke.py` only surfaces the child process's
stdout/stderr once it *returns* - pytest has nothing to print while the
subprocess is still blocked, pass or fail. A hung subprocess is
indistinguishable from a slow one until something external kills it.
Neither `docker compose exec ... sql-client.sh -f ...` invocation in
`scripts/smoke_test.sh` (step 4 or step 5) had any bound of its own -
each was a bare command substitution, willing to wait forever. Steps 2
and 3 already had bounds (step 2's 180s polling loop, step 3's Kafka
consumer's own `--timeout-ms 15000`); steps 4 and 5 didn't, and this is
where it hung.

Fixed by wrapping both invocations in `timeout 90` and checking for exit
code 124 (timeout's own signal that it killed the child) with a specific
failure message. This does not identify *which* of the two hung, or
*why* - `timeout` kills the local `docker compose exec` client process,
which stops the script from waiting further, but doesn't necessarily
prove what the remote process inside the container was doing when it was
killed.

Why this is worth its own entry: a hang and a failure are not the same
failure mode, and treating them the same (by not bounding anything) means
the *slowest* possible feedback - here, 20 minutes of silence, then a
generic "cancelled" with no diagnostic content at all - instead of the
fastest. Every external call in a verification script needs an answer to
"what happens if this simply never returns," not just "what happens if it
returns an error." This was the second time in two attempts that trying
to actually run the 5-trial verification surfaced a real bug in the
verification tooling itself rather than in the thing being verified
(see #16) - which is its own argument for actually running verification
scripts repeatedly, under real conditions, rather than trusting them once
they're merely written.

## 18. Step 4 never had a verified-working query, and LIMIT vs. a bounded source is why

With #17's timeout wrapper in place, step 4 didn't pass - it timed out at
90s, consistently, on the CONCAT+TABLEAU query. This raised a question
the earlier "PASS" runs can't actually answer: did step 4's *original*
bare `SELECT * ... LIMIT 5` (before any of #14/#16/#17's fixes) ever
really succeed, or was it silently swallowed by the old weak assertion
(`[ -n "$STEP4_OUTPUT" ]`, "any output at all") the same way #14 found
step 5 to be? `smoke_test.sh` only ever echoes a step's full raw output
on failure, never on success, so there's no way to retroactively inspect
what an old passing run's step 4 actually contained. Recorded honestly:
unresolved, not assumed innocent.

What's certain going forward, not inherited from an unverifiable past:
`LIMIT` against Kafka - an inherently unbounded, streaming source - is
the wrong tool for "read N messages and stop." `LIMIT` relies on the
query engine noticing it has enough rows and cancelling the job
underneath it; under `TABLEAU` result-mode (required for any
non-interactive `-f` execution at all, per #16) that cancellation
signal apparently doesn't arrive, or doesn't arrive within any
reasonable bound.

The correct tool, checked against Flink's own Kafka connector docs before
using it: `scan.bounded.mode`. Not the first option reached for -
`latest-offset`, the more commonly-documented bounded mode, has a
confirmed open bug (FLINK-34470): transactional-producer control records
can make its stopping-offset calculation hang indefinitely. Our producer
(`kafka-console-producer.sh`, no `--transactional-id`) probably doesn't
trigger it - but "probably doesn't trigger a known bug" is a reasoned
guess, not the verified standard this project has been holding itself to
all session. Used `specific-offsets` instead (`scan.bounded.specific-
offsets = 'partition:0,offset:10'`): a static, literal stopping point
with no dynamic offset negotiation of any kind, so there's no equivalent
mechanism left to doubt.

To settle the "slow vs. hung" question definitively rather than guess at
it, both queries now run in the same CI job: `smoke_step4.sql` (bounded,
gating, asserted) and the new, explicitly non-gating `smoke_step4_
diagnostic_limit.sql` (the original LIMIT approach, 180s timeout,
reported but not asserted). Whichever way that resolves, the gating
check no longer depends on the answer.

## 19. How Flink's checkpoint barrier coordinates with Iceberg's commit protocol

Written before any of step 6's code, on purpose - if this can't be
explained correctly first, the code that depends on it can't be trusted
either.

**The general mechanism (not Iceberg-specific).** Flink's checkpointing
is a distributed snapshot (Chandy-Lamport style). The JobManager tells
source operators to checkpoint; each source records its own restart
position (for a Kafka source, the consumed offsets) and emits a special
*barrier* record, tagged with a checkpoint ID, into every output channel.
Every downstream operator does *barrier alignment*: on receiving a
barrier on one input channel, it holds back new records on that channel
until the same barrier has arrived on *all* its input channels. Only
once aligned does the operator snapshot its own state and forward the
barrier downstream. This guarantees a single, globally consistent cut
across the whole dataflow graph, not just a per-operator local snapshot.
Once every operator (source through sink) has acknowledged its part of
checkpoint N to the JobManager, the JobManager marks checkpoint N
complete and fires `notifyCheckpointComplete(N)` on every operator.

**What Iceberg's Flink sink does with this, specifically.** It's not one
operator, it's two, chained: `IcebergStreamWriter` (parallel, one
instance per subtask) followed by `IcebergFilesCommitter` (a single,
non-parallel instance). When the writer receives checkpoint N's barrier,
it flushes whatever rows it's buffered into closed, durable data files
(Parquet, in this project's config) on the object store, and emits a
*committable* downstream describing those files - it does **not** touch
the Iceberg catalog at this point. The committer collects committables
from all writer subtasks for checkpoint N and holds them as its own
pending state (itself included in checkpoint N's snapshot, so a crash
here isn't lost - the restored committer still has the pending record).
Only on `notifyCheckpointComplete(N)` - proof that *every* operator's
part of checkpoint N is durably persisted - does the committer actually
commit: it builds a new Iceberg snapshot referencing exactly those data
files and atomically updates the catalog pointer to it. Confirmed against
Iceberg's own docs (`flink-writes.md`): "Iceberg commit happened after
successful Flink checkpoint in the `notifyCheckpointComplete` callback."

**What makes this exactly-once instead of at-least-once.** The commit
that makes data *visible* (the new Iceberg snapshot) is deferred until
Flink's own checkpoint - source offsets included - is fully durable. If
anything fails before `notifyCheckpointComplete` fires, Flink doesn't
try to finish that checkpoint; it restarts the whole job from the last
*completed* checkpoint, replaying the Kafka source from exactly the
offsets recorded there. Whatever data files the writer had already
flushed for the failed attempt are simply never referenced by any
manifest - not rolled back, just never wired in - and the replayed data
gets written and committed fresh. So each Iceberg snapshot corresponds to
exactly one Flink checkpoint's worth of data: a checkpoint either
completes fully (offsets advanced and data committed, as a unit, from
any external reader's point of view) or is treated as if it never ran.
That's what rules out both failure modes at once - no gaps (the next
completed checkpoint always resumes from the last *committed* offset)
and no duplicates (a replayed checkpoint's output either commits once or
not at all, never twice). The second half of "not twice": a *retried*
commit of the same checkpoint (e.g. the JDBC commit to Postgres succeeds
but the acknowledgment back to Flink is lost) also can't double-commit -
Iceberg's snapshot summary records the last successfully committed
checkpoint ID, and the committer checks it before committing again.
Deferred commit prevents a crash from producing a duplicate; checkpoint-
id idempotency prevents a *retry* from producing one.

**Where the vulnerability window actually is.** Between the writer
flushing data files to the object store (on barrier receipt) and the
committer's catalog update actually completing (on
`notifyCheckpointComplete`). Inside that window, the files are real,
durable, and sitting in SeaweedFS - but nothing in the catalog points to
them yet. Anything that fails in that window - JobManager crash,
TaskManager crash, a network partition to Postgres during the commit
transaction - leaves those specific files exactly where an orphan check
should find them.

**What "orphaned file" means, concretely, for
`scripts/verify_exactly_once.py`.** Iceberg tracks table contents through
a metadata tree: table metadata -> snapshot -> manifest list -> manifest
files -> individual data file entries. A file is orphaned if it
physically exists under the table's warehouse path in the object store
but is not reachable by walking that tree from *any* snapshot, past or
present. It's not a duplicate (it's never visible to a reader - nothing
points to it) and it's not a gap (the data it contains, if any was ever
meant to be committed, was re-processed and committed via the replay);
it's wasted storage from an interrupted commit, nothing more. Detecting
it means listing every file actually present under the warehouse prefix
and diffing that against the set of file paths referenced by every
manifest of every snapshot currently in the table's metadata.

**PyFlink or Java - the honest answer.** It depends on which Iceberg API
the job uses, and the two have different language support:

- Iceberg's Table API / SQL integration (`CREATE CATALOG` /
  `CREATE TABLE` / `INSERT INTO ... SELECT ...`) - exactly what
  `scripts/sql/smoke_step4.sql` and `smoke_step5.sql` already use - works
  fully from PyFlink. PyFlink's Table API is a complete wrapper over the
  same Java table planner; there's no capability gap. Iceberg's own docs
  state the exactly-once guarantee applies to both its DataStream and
  Table API integrations, and a real user got PyFlink+Iceberg working
  (blocked only by JAR-packaging constraints on a managed runtime, not by
  any PyFlink API limitation - `apache/iceberg#4633`).
- Iceberg's lower-level DataStream sink builder
  (`org.apache.iceberg.flink.sink.FlinkSink.forRowData(...)`), which
  gives finer-grained programmatic control over the write path, is
  documented only in Java. No PyFlink-native equivalent is documented or
  was found. If that specific API is what's wanted, Java is required.

Given this project's existing, already-debugged classpath (Kafka SQL
connector, Iceberg runtime + JDBC catalog, Hadoop client jars, all
already proven to load correctly in `docker/flink/Dockerfile`'s image),
the recommendation is: don't introduce a new dependency surface (a
PyFlink Python environment, or a Java job compilation/packaging
pipeline) for this validation at all. Express step 6's Flink job as SQL,
submitted via `sql-client.sh` exactly like the smoke test already does,
with `SET 'execution.checkpointing.interval' = '10s';` scoping the
10-second interval to that session's job without touching the cluster-
wide default. This is a recommendation, not a decision made unilaterally
- if a real compiled job (Python or Java) is wanted instead, for its own
reasons, that's a call this project doesn't get to make for you.

## 20. The diagnostic answered its question, then became a resource-contention risk

Ran once, got a clean answer, then got removed. The #18 diagnostic ran in
CI: `smoke_step4.sql` (bounded via `scan.bounded.mode`) passed cleanly;
`smoke_step4_diagnostic_limit.sql` (the original LIMIT approach) timed
out at 180s. That settles "slow vs. hung" definitively - it hangs, it
doesn't just take longer - and closes the question #18 left open about
whether step 4 ever actually worked before this session's fixes.

The same run's step 5 then failed with the exact `Unknown catalog-type:
jdbc` error from #14/#15/#16 - and this run used the digest-pinned image
committed in #15, meaning the mutable-tag hypothesis is now
disconfirmed as the explanation, not just unproven. Checked the jar
manifest #15's CI step captures: one copy each of every Iceberg/Hadoop
jar, correct versions, no duplicates - rules out a classpath conflict as
well.

What's left, not yet proven but the most concrete lead so far: `timeout
180` on the host kills the local `docker compose exec` client process.
It does not cancel the remote Flink job - Flink has no built-in signal
propagation from a killed CLI client to the job running on the cluster.
The diagnostic query, confirmed hanging, most likely kept running as an
orphaned job inside the cluster - consuming a task slot, a Kafka consumer
group, network buffers - for the rest of that CI run, including through
step 5's job submission immediately after. This project's own container
only has `taskmanager.numberOfTaskSlots: 4` configured; a leaked slot is
not nothing.

Removed the diagnostic (`scripts/sql/smoke_step4_diagnostic_limit.sql`
and its call site in `smoke_test.sh`) rather than keep it around: its
question is answered, and every future run would otherwise re-carry the
same resource-leak risk for no further information gained. Left
unresolved and stated as such: this is the most concrete lead for the
step-5 inconsistency so far, not a confirmed root cause. Proving it would
need a run of steps 2-5 with the diagnostic fully absent (as it now is
going forward) to see whether step 5 becomes consistently reliable, or
whether something else is still in play.

## 21. catalog-type=jdbc was never once actually flaky - it was reproducibly wrong

#20's test: run steps 2-5 with the diagnostic (and its zombie-job risk)
fully removed, to see if step 5 becomes reliable. It didn't. Ran five
clean trials. All five failed, identically - same `Unknown catalog-type:
jdbc` exception, same eight-line signature, every time. That's not
flakiness; that's a deterministic result that happened to be interrupted,
exactly once, by something else. The one prior "PASS" is the outlier
needing an explanation now, not the failures - and it doesn't have one
yet. It predates digest-pinning (#15), so it's possible some different
image content was pulled that one time, but nothing in this project's
records proves that; it's recorded as unresolved, not assumed.

What five identical failures did settle, conclusively, backed by
Iceberg's own source (already quoted in #11 and #18):
`FlinkCatalogFactory.createCatalogLoader()` has exactly three valid
values for `catalog-type` - `hive`, `hadoop`, `rest` - with an
unconditional `default` throw for anything else. `jdbc` was never a
fourth option. Not sometimes, not under most conditions - never, for
Flink specifically. (Iceberg's JDBC catalog type is real and used by
other engines; it was just never wired into Iceberg's Flink integration.)
Weir's infrastructure had never actually run any of the three supported
options - no Hive metastore, no REST catalog service.

Fixed by adding an `iceberg-rest` service to `docker-compose.yml`
(`tabulario/iceberg-rest:1.6.0`, digest-pinned, same image `reference/`
used for this) and switching `scripts/sql/smoke_step5.sql`'s catalog
config from `catalog-type=jdbc` / a direct Postgres URI to
`catalog-type=rest` / `uri=http://iceberg-rest:8181`. The metadata store
didn't change - it's still Postgres, now reached through the REST
server's own `CATALOG_URI` instead of a direct connection from Flink.
Flagging the image itself honestly: Docker Hub describes it as a "sample
image for experimentation and testing," last pushed over a year before
this pin - not an actively maintained production artifact, the same
category of caveat this project already applied to MinIO/SeaweedFS/
LocalStack when picking the object store.

Why this is worth stating plainly: every fix from #14 through #20 was
real and necessary, and none of them were the actual bug. The actual bug
was a single wrong config value, present since this catalog was first
written, that happened to coincidentally "work" once. Five for five
identical, reproducible failures - not three, not a majority, all of
them - is what made it possible to stop treating this as intermittent
and go back to what the source code had said all along.

## 22. The new iceberg-rest healthcheck used a tool that isn't in that image

#21's fix shipped with `curl -f http://localhost:8181/v1/config` as
`iceberg-rest`'s healthcheck - copied from the same pattern used
elsewhere in this file without checking what's actually inside this
specific image first. It failed immediately in CI: the container's own
logs showed a completely clean startup (Jetty started, listening on
`0.0.0.0:8181`, zero errors) while Docker reported it unhealthy for the
entire retry budget - the exact same signature as the Kafka healthcheck
bug (DEFENSE.md #12).

Checked this time before re-guessing: `iceberg-rest`'s actual source
(`databricks/iceberg-rest-image`'s Dockerfile) is `FROM azul/zulu-
openjdk:17-jre-headless`, a minimal JRE-only image. Its own Dockerfile
installs nothing beyond the compiled jar in the runtime stage - no curl,
no wget, nothing. The healthcheck command didn't exist in the container,
full stop, same as `kafka-broker-api-versions.sh` not being on `PATH`
was a different flavor of the same mistake: assuming a tool is present
in a container instead of checking.

Fixed with bash's `/dev/tcp` (`bash -c 'exec 3<>/dev/tcp/localhost/8181'`)
- no external binary required, just a shell builtin. Confirmed bash
itself is actually present (Debian-based Azul Zulu images ship it) before
relying on it, rather than trading one unverified-tool assumption for
another. Weaker than the curl check it replaces - a successful TCP
connect proves the port is open, not that the HTTP endpoint returns a
real response - but a health check that actually runs and is honest
about what it verifies beats one that fails for a reason unrelated to
the thing it's supposed to be checking.

## 23. AWS SDK v2 always requires a region, even against non-AWS S3

With the healthcheck fixed, `CREATE CATALOG` (rest) started succeeding
cleanly for the first time. The next statement, `CREATE TABLE`, then
failed with an exception that has nothing to do with catalogs, jars, or
anything this session had touched before:

```
Caused by: org.apache.iceberg.exceptions.ServiceFailureException: Server
error: SdkClientException: Unable to load region from any of the
providers in the chain ...: [SystemSettingsRegionProvider: Unable to
load region from system settings. Region must be specified either via
environment variable (AWS_REGION) or system property (aws.region).,
AwsProfileRegionProvider: No region provided in profile: default,
InstanceProfileRegionProvider: Unable to retrieve region information
from EC2 Metadata service ...]
```

AWS SDK v2 (used by both `iceberg-rest`'s own S3 client, server-side, and
Flink's S3FileIO client, writing actual data files) unconditionally
requires a resolvable region before it will do anything, even when
talking to a non-AWS S3-compatible endpoint (SeaweedFS) where the region
concept is functionally meaningless - it's used for request signing and
default endpoint construction in real AWS, neither of which matters once
`s3.endpoint` is already pointing somewhere else entirely. The SDK
doesn't know that; it still runs its full provider chain (env var, system
property, profile, EC2 instance metadata) and fails outright if none of
them resolve.

Fixed by setting `AWS_REGION: us-east-1` - an arbitrary, syntactically
valid placeholder, not a real target region - as a container environment
variable on `iceberg-rest`, `flink-jobmanager`, and `flink-taskmanager`
alike. All three run AWS SDK v2 S3 clients under the hood (the REST
catalog server for its own S3 access; both Flink services for writing
data files directly), so all three needed it, not just the one that
happened to surface the error first.

## 24. SeaweedFS rejects every signed S3 request with no identity configured - and an earlier comment in this project said the opposite

With #23's region fix in place, `CREATE TABLE smoke_iceberg_table` got
past region resolution and failed with a new, unrelated exception:

```
Caused by: org.apache.iceberg.exceptions.ServiceFailureException: Server
error: S3Exception: Signed request requires setting up SeaweedFS S3
authentication (Service: S3, Status Code: 400, Request ID:
18CCC605A040438B2074511D)
```

`docker-compose.yml`'s `seaweedfs` service has always started with `weed
server -s3 ...` and no `-s3.config` flag - no identity file, no
configured access key/secret at all. Both AWS SDK v2 clients that touch
this store (`iceberg-rest`'s own S3 client, and Flink's S3FileIO) always
sign their requests; SeaweedFS's S3 gateway rejects a signed request
outright when it has no identity to validate the signature against.

This directly contradicts a claim already committed in this project, in
three places (`scripts/smoke_test.sh`'s header comment,
`scripts/sql/smoke_step5.sql`'s comment, `.env.example`'s SeaweedFS
section): that SeaweedFS with no identity file is "default-permissive"
and "accepts any access key/secret pair, confirmed in CI." Checked how
that claim could have been written: it dates from the `catalog-type=jdbc`
era (#21), when `CREATE CATALOG` talked to Postgres directly and no
statement ever reached S3 at all - so "CI didn't fail on auth" at the
time was true but never actually exercised a signed S3 request in the
first place. The claim was inferred, not confirmed, and it was wrong. All
three comments are corrected in this same change to state what's now
actually verified: SeaweedFS with no identity configured rejects signed
requests, full stop.

**Alternatives considered:**

1. Bake a static `-s3.config` JSON identity file into the image or a
   config mount, with credentials matching `WEIR_S3_ACCESS_KEY`/
   `WEIR_S3_SECRET_KEY`'s defaults. Rejected: this repeats the exact
   mistake `smoke_step5.sql`'s sentinel-token approach was built to avoid
   (DEFENSE.md #10/#12) - a real credential value (even a local-dev
   default) sitting in a committed file, now duplicated in a second
   place, with no substitution mechanism keeping the two in sync if the
   default ever changes.
2. Generate the identity file at container startup from
   `WEIR_S3_ACCESS_KEY`/`WEIR_S3_SECRET_KEY`, via a shell-wrapped
   `command:` override in `docker-compose.yml` (`sh -c` writing the JSON
   with `printf`, then `exec weed server -s3.config=...`). Drafted, then
   abandoned before committing: getting the quoting right across three
   nested layers - YAML's own `${VAR}` interpolation (needing `$$` to
   suppress it), the outer `sh -c "..."` string, and literal double
   quotes for the JSON payload inside it - was exactly the kind of thing
   this project's own discipline says not to ship un-tested. It couldn't
   be verified without a real Docker run, and getting it wrong would have
   produced a new, confusing parse-time failure instead of the auth
   failure being fixed.
3. **Chosen:** configure the identity at runtime via `weed shell`'s
   `s3.configure -user=... -access_key=... -secret_key=... -actions=...
   -apply` (flags confirmed against SeaweedFS's own
   `weed/shell/command_s3_configure.go` source, not guessed), run from
   `scripts/smoke_test.sh` right after step 2 confirms every service
   healthy. `-apply` is documented in that source as create-or-update,
   so this is idempotent across repeated local runs, not just the first.
   One nested shell level, not three, and it reuses the exact
   `docker compose exec ... sh -c 'echo "..." | weed shell'` pattern this
   project's own Makefile `seed` target already used for bucket creation
   - extended with the same identity, rather than inventing new quoting.

**A second, latent bug found while fixing this one:** CI's "Start stack"
step (`.github/workflows/ci.yml`) has only ever run `docker compose up -d
--build`, never `make seed`. The `weir-warehouse` bucket referenced by
every SQL fixture's `warehouse` option has never actually existed in any
CI run to date. It hadn't surfaced yet because catalog-type=jdbc (#21)
never got far enough to write to S3, and catalog-type=rest didn't get
past region resolution (#23) until now. Fixed in the same change: the new
setup step in `smoke_test.sh` also runs `s3.bucket.create -name
weir-warehouse` (best-effort, `|| true` - no idempotency guarantee found
for this specific command in SeaweedFS's docs or source, unlike
`s3.configure`), so a from-scratch CI run and a repeated local run both
end up with the bucket present. `Makefile`'s `seed` target got the
identical two-line update, kept as a still-useful standalone target for
manual workflows that don't go through `smoke_test.sh` at all.

**Confirmed, not just inferred, by the actual CI run that followed this
change:** `s3.configure -apply` does take effect on an already-running
`weed server -s3` process with no restart involved - the setup step's
own stdout showed the created identity object (echoed back as JSON) and
`created bucket weir-warehouse`, and every subsequent SQL statement
(`CREATE CATALOG`, `CREATE TABLE`, `INSERT`) succeeded with zero
`S3Exception` of any kind. The signed-request-authentication problem
this entry set out to fix is resolved.

**A second, distinct bug surfaced in the same run, after the fix above
was already working:** the row-count check still failed - not on an
auth error this time, but `WEIR_ROW_COUNT=0` instead of `2`. The INSERT
statement's own output read `[INFO] SQL update statement has been
successfully submitted to the cluster: Job ID: ...` - submitted, not
completed. Checked against Flink's own SQL Client docs before treating
this as flaky: by default, SQL Client submits DML as a detached job and
immediately moves on to the next statement; it does not wait for the
job to finish. Running via `-f` with no interactive pause means the
`SELECT COUNT(*)` on the very next line was guaranteed to race the
INSERT's job, not occasionally risk it - this would have failed 100% of
the time, every run, not just this one. Fixed with `SET 'table.dml-sync'
= 'true';`, documented for exactly this: it makes SQL Client block until
a submitted DML statement's job actually completes before returning
control for the next statement. Added to `smoke_step5.sql` only -
`smoke_step4.sql` has no DML statement to race against, only a SELECT.

**Re-verification, not just a single pass:** given this project's own
history of a single green run turning out to mean nothing (#20/#21 - five
identical failures after one earlier, unexplained pass), the identical
commit was re-run five times via `gh run rerun` rather than trusted on
the first green result. All five: pass, `WEIR_ROW_COUNT=2`, no
`S3Exception`, no `table.dml-sync` timeout. Step 5 - and steps 2 through
5 as a whole - is genuinely green, not a lucky single run.

## 25. Step 6's producer: a real delivery-confirmed client, not the console-producer CLI

`scripts/produce_events.py`'s emission log has to be ground truth for
`scripts/verify_exactly_once.py` - every later count (landed, duplicates,
gaps) is only as trustworthy as "the events in this log were genuinely
sent." That requirement, not just convenience, is what the tool choice
turns on.

**Alternative rejected:** reuse `kafka-console-producer.sh` (the tool
smoke test step 3 already uses, zero new dependencies) in a loop,
logging each key/timestamp to the emission log immediately before or
after feeding it a line. Rejected because "logged" and "confirmed
delivered" would be two different things with this tool: console-
producer batches internally and has no way to expose a per-message
delivery callback back to the shell loop driving it. A log entry would
mean "we asked Kafka to send this," not "Kafka's leader acked this" -
weaker than what an exactly-once verification should be built on.

**Chosen, after asking rather than deciding unilaterally (CLAUDE.md hard
rule 6):** `kafka-python` (pinned `3.0.11`, Apache-2.0, confirmed via
PyPI's own package metadata - a pure-Python client, no C extension to
complicate the CI environment). Every send registers a delivery callback
via `.add_callback(...)`/`.add_errback(...)`; the emission log is written
*only* from the success callback, after the broker has acked the write,
never speculatively before sending. A failed delivery is written to
stderr and recorded as a hard failure, not silently dropped - if the
producer can't confirm a send, that has to be visible, not smoothed over
by "the log just doesn't mention it."

Tradeoff accepted: one new Python dependency, for one script, that
nothing else in this project needs. Scoped narrowly on purpose - it's a
dependency of the *validation tooling*, not of any job or service this
project ships, matching the same category as `pytest` already is.

## 26. Detecting orphaned files: SeaweedFS's own tools can't do it; Iceberg's `$all_data_files` plus the Filer's JSON API can

DEFENSE.md #19 defined what an orphaned file is: physically present under
the table's warehouse path, but not reachable from *any* snapshot's
manifest tree, past or present. Two lists have to be built and diffed:
every file Iceberg has ever referenced, and every file actually sitting
in the object store.

**The referenced side.** Checked Iceberg's own Flink SQL metadata-table
docs before assuming `$files` (the one already known from casual
familiarity) was enough: `$files` only covers the *current* snapshot.
`$all_data_files` is the one documented to span every snapshot the table
has ever had, expired or not - confirmed against Iceberg's own Flink
querying docs, not assumed from the Spark-side naming convention.
`SELECT file_path FROM eos_events$all_data_files` is what
`verify_exactly_once.py` actually runs, via the same `sql-client.sh -f`
mechanism every other SQL step in this project already uses.

**The on-disk side - two tools considered, one rejected on inspection.**
`weed shell`'s `fs.tree` is documented as "recursively list all files
under a directory" - looked like the obvious fit. Checked its actual
source (`command_fs_tree.go`) before relying on it: it prints a box-
drawing tree (`├──`, `└──`, Unicode line characters), built specifically
for human eyes, with no flag to emit flat paths instead. Reconstructing
full paths from indentation and tree-drawing characters is exactly the
kind of "parse loosely-structured output and hope" this project has
already been burned by once this session (`grep -qE '\b2\b'`, DEFENSE.md
#14) - rejected before writing a single line depending on it.

**Chosen:** SeaweedFS's Filer HTTP API, which returns real JSON on
request (`Accept: application/json`) for any directory: `{"Path": ...,
"Entries": [{"FullPath": ..., "Mode": ..., "chunks": [...]}, ...]}` -
confirmed against the Filer Server API wiki page directly, not
recalled. `verify_exactly_once.py` walks this recursively from the
warehouse root using Python's stdlib `urllib` and `json` (no new
dependency - unlike #25, this one didn't need one), treating an entry as
a file if its `chunks` key is present (even as an empty list) and a
directory otherwise, per the wiki's own description ("files include a
chunks array; directories do not"). Handles the API's own pagination
(`Limit`/`LastFileName`/`ShouldDisplayLoadMore`) rather than assuming a
single page always covers a table this small - a smoke-scale run
probably never hits the 100-entry default limit, but "probably won't
matter" is exactly the standard this project has been holding itself to
not accepting all session.

**Stated honestly, not swept under the same confidence as the rest of
this entry:** the file-vs-directory heuristic (chunks key present vs.
absent) is inferred from the wiki's prose, not exhaustively verified
against SeaweedFS's own source the way #22's healthcheck fix or #25's
delivery-callback behavior were. If orphan-file counts ever look wrong in
a way nothing else explains, this heuristic - not a re-guess at something
else - is the first thing to re-check.

## 27. Killing the TaskManager for real, and bringing it back without changing docker-compose.yml's failure-handling behavior for everything else

`flink-taskmanager` in `docker-compose.yml` has no `restart:` policy - the
Compose default, `no`. A real container crash (OOM, node failure, a bad
deploy) would just leave it dead until something else notices and acts;
nothing in this stack currently auto-heals that. That's the honest
starting condition this test runs against, not a gap introduced for the
test's convenience.

**Alternative rejected:** add `restart: on-failure` (or
`unless-stopped`) to `flink-taskmanager` in `docker-compose.yml`, so a
killed container comes back on its own during the test. Rejected because
that's a real, permanent change to this project's general failure-
handling posture - affecting every future run, every other kind of
TaskManager failure, not just this one destructive test - decided as a
side effect of writing a test script, not as its own considered choice.
If always-restart TaskManagers is the right call for this project, that
deserves its own DEFENSE.md entry made on its own merits, not smuggled in
here.

**Chosen:** `scripts/verify_recovery.sh` does both halves explicitly and
visibly. `docker compose kill -s SIGKILL flink-taskmanager` for the
failure itself - SIGKILL, not `stop` (SIGTERM, graceful) or `restart`,
because a real crash doesn't wait for the process to clean up, and this
test is specifically about what happens when it doesn't. Then `docker
compose start flink-taskmanager` to bring the container back - standing
in for whatever infra-level mechanism (a orchestrator restarting a pod, a
supervisor process, a human) would do that in a real deployment, which is
explicitly out of scope for what this test is verifying. What's being
tested here is what Flink itself does once task slots exist again, not
whether the surrounding infrastructure heals itself - `docker-compose.yml`
stays exactly as failure-tolerant (or not) as it already was for every
other purpose.

**Stopping the job at the end:** `bin/flink stop <jobID>`, not `cancel` -
a graceful stop triggers a final savepoint before terminating, `cancel`
does not. `config/flink/config.yaml` already sets `state.savepoints.dir:
file:///tmp/flink-savepoints`, and that volume is already mounted in
`docker-compose.yml` for both Flink services - `stop` works with no
extra flag or new infrastructure, entirely because that groundwork
already existed for an unrelated reason.

## 28. Overriding checkpoint interval for real: min-pause has to move too, or "10s" is a lie

The task asks for checkpoint interval overridden to 10s "via the env
var, not hardcoded" for this one validation job, without touching the
cluster-wide 60s default (DEFENSE.md #1) other jobs still run under.

**Session-scoped, not cluster-scoped, confirmed before relying on it:**
checked whether Flink SQL Client's `SET` actually applies arbitrary
`execution.checkpointing.*` keys to a job submitted from that session, or
only a curated subset - confirmed real usage of `SET
'execution.checkpointing.interval' = '5 s';` in this exact form, not
assumed from the `FLINK_PROPERTIES` mechanism already used elsewhere in
this project (that one works at the container/cluster level, a different
mechanism entirely). `scripts/sql/exactly_once_job.sql` sets it via a
sentinel token (`__WEIR_EOS_CHECKPOINT_INTERVAL__`), substituted from a
new `WEIR_EOS_CHECKPOINT_INTERVAL` env var (default `10s`) the same way
`smoke_step5.sql` already substitutes credentials - never hardcoded in
the committed file.

**The part that would have quietly broken this:** `config/flink/
config.yaml` also sets `execution.checkpointing.min-pause: 30s`
cluster-wide - the minimum gap enforced between the *end* of one
checkpoint and the *start* of the next, independent of the interval
setting. Left alone, a 10s interval with a 30s min-pause doesn't produce
checkpoints every 10s; min-pause dominates whenever it's the larger of
the two, so the job would actually checkpoint no more often than every
~30s+ while still claiming a "10s" interval - not wrong, just silently
not doing what it says, exactly the class of mismatch DEFENSE.md #16
already found once (a setting nobody stated explicitly, quietly
determining behavior instead of the one everybody was looking at).
`exactly_once_job.sql` also sets `SET 'execution.checkpointing.min-pause'
= '0s';` for this session, so the 10s interval is the actual cadence, not
a number that only appears in a config key nothing enforces.

**Scope limitation, stated rather than silently assumed:** the topic
(`weir-eos-events`) is created with a single partition, matching smoke
test step 3's own convention. This keeps gap/duplicate accounting exact
(no cross-partition interleaving to reason about) but means this run
doesn't exercise `IcebergStreamWriter`'s parallel-subtask-commit path
described in DEFENSE.md #19 under real multi-partition concurrency -
only its single-partition case. If that path specifically needs
validating later, it needs its own run with a multi-partition topic, not
an assumption that this one already covered it.

## 29. TABLEAU mode's 30-character column truncation would have silently corrupted the orphan-file comparison

Almost shipped `scripts/sql/exactly_once_verify.sql`'s file-path dump
using the exact same sentinel-grep pattern as `smoke_step5.sql`
(`WEIR_FILE=<path>`), the same way `WEIR_ROW=` already works for
event rows. Checked one assumption before trusting it for `file_path`
specifically, since S3 URIs are long and event keys aren't: does
`sql-client.sh`'s TABLEAU result mode ever truncate a column's printed
value? Confirmed, not hypothetical: Flink's own SQL Client
documentation states long string values are truncated to 30 characters
by default. `s3a://weir-warehouse/warehouse/eos_test/eos_events/data/...`
is comfortably past that. A silently truncated path would never match
anything in the Filer's actual on-disk listing - every real data file
would have registered as a false orphan, and the count would have looked
like a specific, plausible finding instead of what it actually would
have been: a display setting nobody looked at.

**Alternative drafted, then abandoned as disproportionate:** route both
the row-dump and the file-list-dump through a Flink filesystem-connector
sink (`INSERT INTO ... WITH ('connector'='filesystem', ...)`), writing
CSV directly to a bind-mounted host directory, bypassing terminal
rendering entirely. Would have worked, but it's a new docker-compose.yml
volume mount, a new host-directory-permissions question (the same
non-root `USER flink` problem DEFENSE.md #10 already solved once, just
in the write direction this time), and an unverified assumption about
exactly how Flink's filesystem connector lays out batch INSERT output
(one file at the given path, or a directory of part-files - not checked,
because it turned out not to be necessary).

**Chosen, once the simpler fix was confirmed real:** raise the limit via
`SET`. `sql-client.display.max-column-width` is the name most search
results surface, but it's deprecated as of FLIP-279/FLINK-30025 (fixed
in Flink 1.18.0, well before this project's pinned 2.1.0) in favor of
`table.display.max-column-width` - used the current key, not the
deprecated one, confirmed against the JIRA ticket's own fix-version
field rather than the first name found. Set to `500` in
`exactly_once_verify.sql`, comfortably past any path this warehouse
layout produces. One `SET` statement, zero new infrastructure, and the
sentinel-grep pattern already proven in `smoke_step5.sql` stays exactly
as it was.

## 30. A latent infrastructure bug in the base image, only reachable once a job actually checkpoints

First real run of `scripts/verify_recovery.sh` failed before the job
even started - not in any step-6-specific code, in job submission
itself:

```
Caused by: java.io.IOException: Failed to create directory for shared state: file:/tmp/flink-checkpoints/<job-id>/shared
	at org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess.initializeBaseLocationsForCheckpoint(...)
	at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.<init>(...)
```

Root cause, once traced back: `docker/flink/Dockerfile` never creates
`/tmp/flink-checkpoints` or `/tmp/flink-savepoints` - those paths have
only ever existed via `docker-compose.yml`'s named volumes
(`flink-checkpoints:`/`flink-savepoints:`), mounted over a path the
image itself has nothing at. A fresh named volume with no matching
content already in the image gets created at first mount owned by
root, not by the non-root `flink` user (uid 9999, confirmed from
`apache/flink-docker`'s own Dockerfile) this image runs as. Same root
cause as DEFENSE.md #10 - a non-root user meeting something it can't
write to - but on a directory Docker itself creates at mount time,
not a file this project copies in.

**Why this was never caught before, honestly:** every SQL run in this
project until now (`smoke_step4.sql`, `smoke_step5.sql`) was bounded/
batch. Batch execution in Flink doesn't enable checkpointing the same
way a continuous streaming job does - `execution.checkpointing.interval:
60s` sits in `config/flink/config.yaml` and applies cluster-wide
regardless, but nothing before `exactly_once_job.sql` (streaming,
by construction - it reads an unbounded Kafka source) had ever actually
asked the JobManager to construct checkpoint storage. The permission
problem was latent in this project's infrastructure since `docker-
compose.yml` first added those volumes, reachable by any streaming job,
just never triggered because none had been submitted yet.

**Fixed** in `docker/flink/Dockerfile`, before `USER flink`: `mkdir -p
/tmp/flink-checkpoints /tmp/flink-savepoints && chown -R flink:flink
...`. Docker's documented behavior for named volumes - content (and
ownership) already present in the image at a mount point gets copied
into a freshly created volume on first mount - means this is enough;
no entrypoint wrapper, no runtime chown, no change to `docker-
compose.yml` at all.

**Scope note:** this is a base-infrastructure fix, not a step-6-only
one - it belongs on `main`, not just `exactly-once-validation`, since
any future streaming job (not only this validation) would have hit the
identical failure the first time it tried to checkpoint. Ported to
`main` in its own commit rather than folded silently into a step-6
commit on this branch.

## 31. An assertion-auditing bug found by auditing this script's own assertions: a diagnostic print swallowed into a variable instead of printed

Explicitly asked to audit every assertion in this script against the
class of bug in #14 - here's a real one, in the *plumbing* around an
assertion rather than the comparison itself.

`wait_for_checkpoints()` was called as `BASELINE_CHECKPOINTS="$(wait_for_
checkpoints 3 180)"` - a command substitution, so the *entire* function's
stdout becomes the captured value, not just the `echo "$completed"` line
intended as its "return value." Its own failure path called `print_raw
"final checkpoints JSON" "$cp_json"` to explain *why* it timed out - and
that diagnostic output went into `$BASELINE_CHECKPOINTS` right along
with everything else, never printed to the actual log at all. The second
real run of this script (see #30 for the first) timed out at this exact
stage, and the log showed the timeout message but not one byte of the
diagnostic that was supposed to explain it - a checkpointing failure
this project still doesn't have a root cause for yet, made harder to
diagnose by the very telemetry meant to help.

This isn't the #14 pattern itself (a check that passes when it
shouldn't) - it's adjacent: a check that fails *correctly* while
destroying the evidence needed to explain the failure, because "return a
value" and "print diagnostics" were sharing the same channel (a
function's stdout) without that being a deliberate choice.

Fixed by giving the function's result its own channel - a global
(`WAIT_RESULT`), set before returning, read by the caller afterward -
freeing stdout entirely for `print_raw`/`echo` diagnostics, which is
what every other stage in this script already uses stdout for. Also
added a per-poll progress line (job state + checkpoint count, every 5s)
rather than only printing at the very end, so a future timeout shows the
whole trajectory - stuck at zero from the start, versus slowly climbing
and running out of budget, are different findings and shouldn't require
a third run just to tell apart.

## 32. The "continuous" job wasn't continuous - it finished on its own in ~2 seconds, reading zero records

With #31's fix in place, the third real run finally showed what stage 5
had been timing out on: `job state=FINISHED, completed checkpoints=1`
from the very first poll, unchanged for the full 180s. The job's own
`/jobs/<id>` detail, pulled before failing:

```
"state":"FINISHED","job-type":"STREAMING","start-time":...,"end-time":...,"duration":3011,
"vertices":[{"name":"Source: eos_kafka_source[1] -> IcebergStreamWriter -> ...",
  "status":"FINISHED",...,"metrics":{"read-records":0,"read-records-complete":true,
  "write-records":0,"write-records-complete":true,...}}]
```

Not a crash, not a hang - `job-type` is genuinely `STREAMING` (confirming
`execution.checkpointing.interval`/`min-pause`/`runtime-mode` overrides
from DEFENSE.md #28 *were* applied), and the job voluntarily completed
in about 3 seconds having read zero records, with `read-records-complete:
true` - a Flink-internal signal specifically meaning the source decided
it had no more data coming, not that it errored or was cancelled. The
single "completed checkpoint" is consistent with a final checkpoint
taken as part of that voluntary shutdown, not a periodic 10s checkpoint
during real execution - the job never lived long enough for a second
periodic one regardless.

Checked before accepting a guess: Flink's own Kafka source documentation
states the default stopping-offsets initializer for an unbounded source
(`NoStoppingOffsetsInitializer`) "does not initialize anything" and the
source "never stops until the Flink job fails or is cancelled" absent an
explicit `scan.bounded.mode` - which this job's DDL never set. That's
the *documented* default behavior, and it's what should have happened
here. It didn't, and stated honestly: the exact internal mechanism that
made *this* source finish anyway - something specific to a single-
partition topic that is still completely empty (0 messages, consumer
position already equal to the high watermark) at the moment the source
starts - was not tracked down to a specific line of Flink or the Kafka
connector's source. This project's own timeline explains why the
condition existed: `scripts/verify_recovery.sh` created the topic (stage
2) and submitted the job (previously stage 3) *before* starting the
producer (previously stage 4) - by design, reasoning at the time that
`scan.startup.mode='earliest-offset'` made the ordering irrelevant to
correctness. It made the topic genuinely empty at the exact moment the
source read from it, which turned out to matter for a different reason
than data correctness.

**Fixed two ways, not one, because the mechanism itself isn't fully
confirmed:**

1. Reordered `scripts/verify_recovery.sh`: the producer now starts
   first (new stage 3), confirmed running for 3 seconds before the job
   is submitted (new stage 4) - the source's very first poll has real
   data waiting, removing the empty-topic condition entirely.
2. Added `'scan.topic-partition-discovery.interval' = '10s'` to
   `eos_kafka_source` in `exactly_once_job.sql` - continuous partition
   discovery instead of the connector's one-time-at-startup default, as
   a second, complementary line of defense in case the actual mechanism
   is related to split/partition enumeration finalizing early rather
   than (or in addition to) the empty-topic condition itself.

Recorded as two fixes rather than confidently claiming one root cause,
because that's the honest state of the investigation: the reorder
directly removes the specific condition observed in the failing run: the
partition-discovery setting is a reasoned hedge, not a confirmed
independent fix. If a future run still finishes early with a non-empty
topic at start, the discovery setting - not a new guess - is the next
thing to test in isolation.

**Addendum - the fourth real run disproved the reorder's assumption
outright.** With the producer confirmed running for 3 seconds before
the job was submitted, the job still finished, in about 3 seconds,
having read zero records - the exact same signature as before, just a
few seconds later. The empty-topic-at-start theory is now disproven by
direct evidence, not just unconfirmed: the topic demonstrably had data
by the time this job read from it, and it still happened.

That run also exposed a second problem in the *investigation* itself:
`dump_logs_and_fail`'s `--tail=200` container-log dump ran only after
the full 180s wait budget expired, by which point 175+ seconds of
routine Kafka heartbeat/consumer-group logging had scrolled whatever the
JobManager logged at actual completion time out of a 200-line window -
the diagnostic evidence was gone before it was ever captured, for the
second run in a row. Fixed by treating `FINISHED`/`FAILED`/`CANCELED` as
terminal inside `wait_for_checkpoints` itself (mirroring what stage 8
already does for job recovery) - failing within one 5-second poll cycle
of the state actually changing, not 180 seconds later, so the next run's
log dump has a real chance of showing what actually happened instead of
three minutes of unrelated noise.

**Second addendum - the fail-fast fix worked, and immediately exposed a
third, different gap in the investigation itself.** The fifth real run
failed within 5 seconds as intended, but `dump_logs_and_fail`'s
container-log dump still showed nothing but container *startup* banners
(`Starting Job Manager`, `Starting standalonesession as a console
application`) - no job-lifecycle or source-completion messages at all,
regardless of timing. `docker compose logs` only surfaces what a
container prints to its own stdout/stderr; Flink's `standalonesession`
entrypoint runs as "a console application" for supervisory output only
- its actual operational logging (job state transitions, source/split
lifecycle) goes to log *files* inside the container
(`/opt/flink/log/*.log`), never touching stdout at all. Three
consecutive real runs have now each surfaced a genuine gap before ever
reaching the underlying question - a broken diagnostic swallowing
output (#31), a timeout dumping logs 175s too late (#32's first
addendum), and now dumping the wrong log source entirely.

**Status, stated plainly rather than pushed further right now:** the
underlying bug - a nominally-continuous streaming Kafka source reaching
`FINISHED` with zero records read, even with confirmed live data in the
topic before the job was ever submitted - is not yet root-caused.
Parked here, not abandoned silently: the concrete next step is reading
`/opt/flink/log/*.log` directly (via `docker compose exec ... cat`, not
`docker compose logs`) at the moment of failure, to see what the
JobManager and TaskManager actually logged about the source/split
lifecycle - not a new guess, the specific gap this addendum identifies.
This is deliberately being set aside now to start Part 2.1 (NYC TLC
downloader + replay producer, which only needs Kafka, already green) in
parallel, per explicit instruction not to block on this.

**Third addendum - re-audited before resuming, per E5/E6.** Explicitly
re-checked whether `scan.bounded.mode` could have leaked in from
`smoke_step4.sql`'s diagnostic (the exact bug class E6 warns about): it
doesn't appear anywhere in `exactly_once_job.sql`, only in the separate
`smoke_step4.sql`, and `sql-client.sh -f` takes no `--init`/shared-
session file - each invocation is a fresh process with no mechanism to
inherit settings from a different file's separate invocation. No leak.
`execution.runtime-mode=streaming` and `scan.startup.mode=earliest-
offset` are both present and correct; the job's own REST API status has
independently confirmed `job-type: STREAMING` on every real run. The
bounded-completion *signature* (E5) is still exactly right as a
diagnosis - it's just not caused by a stray config value sitting in a
file. `dump_logs_and_fail` is fixed (this commit) to read `/opt/flink/
log/*.log` directly instead of `docker compose logs`, which two
consecutive runs proved only shows console startup banners, never
Flink's actual job-lifecycle logging. Next real run gets the actual
evidence needed to diagnose from, not another guess.

**Fourth addendum - the fixed path was itself another unverified guess,
and it was wrong.** The next real run: `tail: cannot open '/opt/flink/
log/*.log' for reading: No such file or directory`, from both
containers. `/opt/flink/log/*.log` was written down as "the" Flink log
location from general knowledge, not confirmed against this specific
image - the exact mistake E6 exists to name, made while writing the fix
for a *different* instance of the same mistake. Four consecutive real
runs have now each cost a full CI cycle to a diagnostic gap rather than
the underlying question: a swallowed print (#31), a 175-second-late
dump (this entry's first addendum), the wrong capture command entirely
(second addendum), and now a wrong path for the right command. Fixed by
not guessing a second path: `find /opt/flink -iname '*.log*'` discovers
whatever's actually there instead of asserting where it should be.

**Fifth addendum - `find /opt/flink` found nothing, not a missing
directory: zero matches.** No log files exist under `/opt/flink` at
all, in either container. Rather than narrow the search to a sixth
guessed directory, widened it two ways at once: a filesystem-wide `find
/ -xdev -iname '*.log*'`, and Flink's own REST API log listing
(`GET /jobmanager/logs`) - confirmed as a real, documented endpoint
against Flink's own REST API reference before use, returning JSON
metadata (name/size/mtime) for whatever Flink itself considers its log
files, independent of where the container happens to route them. Between
the two, this should either surface the actual location or establish
directly that Flink genuinely isn't writing log files in this setup at
all (plausible: many container-oriented Flink configurations route
everything to console specifically for log-aggregation friendliness,
which would mean the missing piece is *log level*, not log location -
`docker compose logs`'s console output may simply be filtered below
whatever level logs source/job completion).

**Sixth addendum - found it, and it's not a script bug at all: the base
infrastructure's logging has been broken since the conf mount was added
(DEFENSE.md #13).** `GET /jobmanager/logs` returned `{"logs":[]}` -
Flink's own accounting confirms zero log files exist, not a wrong
search path. The filesystem-wide `find` turned up only OS package-
manager logs (`/var/log/dpkg.log`, `apt/*.log`, ...), nothing Flink-
related at all. And the *entire* `docker compose logs` output for both
containers, across their whole lifetime (not a 200-line tail truncating
something longer - this was everything either container had ever
printed) was: two `config-parser-utils.sh: ... Permission denied` lines,
a "Starting X Manager"/"Starting X as a console application" pair, and
two `main ERROR Reconfiguration failed: No configuration found for
'<hash>' at 'null' in 'null'` lines. That reconfiguration error is
log4j2's own bootstrap failure message - a strong, checkable lead,
followed up rather than dismissed as more noise.

Root cause: `docker-compose.yml`'s `./config/flink:/opt/flink/conf` bind
mount **replaces** the image's entire `conf/` directory rather than
overlaying it - `config/flink/` has only ever contained `config.yaml`
(added in DEFENSE.md #13), which means the image's own default
`log4j-console.properties` - the file Flink's logging docs specifically
name as the one used "if Job-/TaskManagers are run in the foreground"
(exactly what "Starting standalonesession as a console application"
describes) - has been absent from both containers since that mount was
first added. Log4j2 falls back to essentially no meaningful output at
all rather than the INFO-level console logging (and rolling file
appender) that file configures. This has been true for every run since
DEFENSE.md #13, including every smoke-test run on `main` - it never
surfaced there only because nothing on `main` has ever needed to read
detailed Flink logs to debug a failure; steps 4/5's own assertions
(DEFENSE.md #14) never depended on log content.

Fixed by adding `config/flink/log4j-console.properties` - a verbatim,
unmodified copy of Flink 2.1's own default file (`apache/flink`,
`release-2.1` branch, `flink-dist/src/main/flink-bin/conf/
log4j-console.properties`), not a from-scratch or partial
reconstruction - so the bind mount carries a complete, working conf
directory instead of a partial one. Recorded in PROVENANCE.md/
THIRD_PARTY_NOTICES.md as copied from upstream Apache Flink (Apache-2.0),
a new provenance category distinct from `reference/`.

**This explains why five consecutive diagnostic attempts found
nothing** - not because each one looked in a slightly wrong place, but
because the underlying logging was never actually configured to produce
anything worth finding, in this container, since before step 6 existed.
It does not yet explain the original bug (the job reaching `FINISHED`
with zero records read) - that still needs an actual run with real
logging in place. This fix is a precondition for diagnosing it, not the
diagnosis itself. Base-infrastructure scope, like DEFENSE.md #25/#30:
ported to `main` in its own commit, not left stranded on this branch.

## 33. Root cause: `eos_kafka_source` was never a Kafka table - it was a silently-created, empty Iceberg table

The full diagnostic path, in order - what was ruled out, and how, before
the actual cause was found. Recorded as its own entry because the path
is the more valuable half of this: five real tooling gaps had to be
found and fixed, in sequence, before the underlying bug was even
*visible* to look at.

**What was ruled out, with evidence, before any log was read:**

1. `scan.bounded.mode` leaking in from `smoke_step4.sql`'s diagnostic
   (E6's exact concern) - grepped every relevant file by line number;
   zero occurrences in `exactly_once_job.sql` or `config/flink/
   config.yaml`, and `sql-client.sh -f` has no shared-session mechanism
   between separate invocations to leak through anyway.
2. `execution.runtime-mode`/`scan.startup.mode` misconfigured - both
   confirmed correct (`streaming`/`earliest-offset`), and independently
   corroborated by the job's own REST API status reporting `job-type:
   STREAMING` on every real run, not `BATCH`.
3. Stale committed consumer-group offsets (`properties.group.id` is
   fixed, `weir-eos-verify`, across runs) - ruled out because
   `scan.startup.mode='earliest-offset'` ignores committed offsets
   entirely regardless of group history; this would only matter under
   `group-offsets` mode, which isn't used here.
4. Topic/partition mismatch - checked the actual substituted SQL in two
   real runs' captured output: `'topic' = 'weir-eos-events'` in both,
   exactly matching the producer and the topic-creation stage. No
   sentinel-substitution bug, correct partition count.
5. An empty topic at the moment the source first read from it (#32) -
   plausible, tested directly by reordering the script to start the
   producer first, confirmed running for 3 seconds before job
   submission. The job still finished in ~3s reading zero records. This
   directly disproved the leading hypothesis rather than just failing to
   confirm it.

**Five diagnostic-tooling gaps, found and fixed in sequence, before the
real evidence was ever visible (all under #31/#32's addenda):** a
diagnostic `print_raw` silently swallowed into a variable instead of
printed; a 180s-late log dump that captured three minutes of unrelated
noise instead of the actual failure moment; capturing `docker compose
logs` when the relevant logging was never going to be there regardless
of timing; a guessed log-file path (`/opt/flink/log/*.log`) that didn't
exist; and, once `find` and Flink's own `/jobmanager/logs` REST endpoint
proved *no log files existed anywhere*, the actual root cause of *that*:
`docker-compose.yml`'s conf bind mount had been silently discarding the
image's own `log4j-console.properties` since the mount was first added
(DEFENSE.md #13/#26/#32's sixth addendum) - a base-infrastructure bug
that predates step 6 entirely and had simply never been noticed.

**With real logging finally in place, the actual evidence:**

```
INFO org.apache.flink.runtime.source.coordinator.SourceCoordinator - Starting split enumerator for source Source: eos_kafka_source[1].
INFO org.apache.iceberg.flink.source.enumerator.AbstractIcebergEnumerator - Received request split event from subtask 0
INFO org.apache.iceberg.flink.source.enumerator.AbstractIcebergEnumerator - Assigning splits for 1 awaiting readers
INFO org.apache.iceberg.flink.source.enumerator.AbstractIcebergEnumerator - No more splits available for subtask 0
INFO org.apache.iceberg.flink.sink.IcebergFilesCommitter - Skip commit for checkpoint 1 due to no data files or delete files.
```

The class actually enumerating splits for `eos_kafka_source` is
`org.apache.iceberg.flink.source.enumerator.AbstractIcebergEnumerator` -
Iceberg's own read-side enumerator. Not one single log line from any
real Kafka connector class (`org.apache.flink.connector.kafka.*`,
`org.apache.kafka.*`) appears anywhere in either container's full log,
despite `flink-sql-connector-kafka-4.0.1-2.0.jar` being confirmed on the
classpath. `eos_kafka_source` was never wired to Kafka at all.

Confirmed directly from Iceberg 1.11.0's own source
(`FlinkCatalog.createTable()`, `flink/v2.0` module):

```java
if (Objects.equals(
        table.getOptions().get(FlinkCreateTableOptions.CONNECTOR_PROPS_KEY),
        FlinkDynamicTableFactory.FACTORY_IDENTIFIER)
    && table.getOptions().get(FlinkCreateTableOptions.SRC_CATALOG_PROPS_KEY) == null) {
  throw new IllegalArgumentException(
      "Cannot create the table with 'connector'='iceberg' table property in an iceberg catalog...");
}
Preconditions.checkArgument(table instanceof ResolvedCatalogTable, "table should be resolved");
createIcebergTable(tablePath, (ResolvedCatalogTable) table, ignoreIfExists);
```

This method special-cases exactly one value: `'connector'='iceberg'`
(rejected outright, unless it's a `CREATE TABLE LIKE`). For every other
value - `'kafka'` included - execution falls straight through to
`createIcebergTable(...)`, unconditionally, silently discarding the
connector property and every Kafka-specific option along with it.
`exactly_once_job.sql` created `eos_kafka_source` *after* `USE CATALOG
weir_eos_catalog` - while an Iceberg catalog was active. The table this
created was a real, empty Iceberg table that happened to be named
`eos_kafka_source`, not a Kafka source under any name. Reading it
correctly, deterministically returns zero rows and completes
immediately - not a bug in the read, a correct read of an empty table.

**Why `smoke_step4.sql` never hit this:** it creates `smoke_kafka_source`
under Flink's default catalog - no `CREATE CATALOG`/`USE CATALOG`
statement anywhere in that file at all. The two scripts' Kafka tables
were never created under the same conditions; step 4 was never at risk.

**Fixed** by creating `eos_kafka_source` first, before the Iceberg
catalog is created or switched to, and referencing it by its
fully-qualified name (`default_catalog.default_database.
eos_kafka_source`) in the final `INSERT` once the current catalog has
moved to `weir_eos_catalog` - Flink SQL resolves cross-catalog
references this way natively. `scan.topic-partition-discovery.interval`
from #32 is kept as a real hardening measure, not reverted, even though
it was never the actual mechanism.

**Confirmed working, first real run with the fix:** checkpoints
incremented correctly and continuously (0 -> 1 -> 2 -> 3, not stuck),
the TaskManager kill/recovery cycle worked end to end (job state
RUNNING -> RESTARTING -> RUNNING, checkpoints resuming from 3 through
6), and stages 1 through 9 all passed. This is the actual exactly-once
mechanism (DEFENSE.md #19) working, for the first time in this
project, against a genuinely continuous Kafka source.

**A second, unrelated bug surfaced immediately after, at stage 10:** the
producer crashed with an uncaught `KafkaTimeoutError: Failed to update
metadata after 60.0 secs`, raised synchronously from `producer.send()`
itself (not from a delivery callback - a different failure mode
`produce_events.py` had no handling for at all). This is exactly what
CLAUDE.md's V6 exists for: a failure during the TaskManager-kill window
(which stresses the whole stack under CI's limited resources, not just
Flink) has to be distinguishable and survivable, not a crash that reads
as an unrelated script failure. Fixed by catching `KafkaError` around
the `send()` call specifically (separate from the existing delivery-
callback error handling), logging it as a `send_failures` entry
distinct from `delivery_failures`, and continuing the loop rather than
exiting - the run still reports the failure (and still exits non-zero
overall), but it no longer masks whatever else was happening in the
same window.

## 34. Kafka's advertised listener was never reachable from a host-side client - two producer runs surfaced it two different ways

With #33's root cause fixed, a fresh re-run failed again at stage 10 -
same symptom class (a `KafkaTimeoutError`), but this time from the
producer's very *first* send attempt (`evt-00000000`), not partway
through a long-running window. `attempted=2, delivery_failures=0,
send_failures=2` - the producer never successfully sent a single event
this entire run, despite stages 4 through 9 (the actual Flink job) all
passing again.

Checked before assuming CI flakiness: `docker-compose.yml`'s `kafka`
service has exactly one data listener, `KAFKA_ADVERTISED_LISTENERS:
PLAINTEXT://kafka:9092` - advertised as the container's own hostname,
which only resolves inside the `weir-network` Docker network.
`scripts/produce_events.py` (and `ingestion/replay/replay_producer.py`
on the `nyc-tlc-replay` branch) both run on the *host* (the CI runner
itself, or a developer's machine), connecting via `localhost:
${KAFKA_PORT}`. Kafka's protocol means an initial bootstrap connection
can succeed superficially (the TCP port is genuinely reachable via
Docker's port mapping), but any subsequent metadata-driven reconnect -
which the client needs for real produce traffic, not just the first
handshake - tries to reach `kafka:9092` and fails, since that hostname
doesn't exist outside the Docker network at all.

**Why nothing on `main` had ever hit this:** every existing Kafka
client interaction in this project (`smoke_test.sh`'s `kafka-topics.sh`/
`kafka-console-producer.sh`/`kafka-console-consumer.sh` calls) runs via
`docker compose exec -T kafka ...` - *inside* the container, where
`kafka:9092` (or even `localhost:9092`, since that's the container's own
loopback) resolves fine. `produce_events.py` is the first client in this
project to connect from the host directly, and the first to actually
exercise this path.

**Why it surfaced differently across two runs, rather than failing the
same way every time:** likely dependent on exactly when the client's
Kafka library needs a metadata-refresh-triggered reconnect versus being
able to ride the initial bootstrap connection for a while first - not
fully traced to the exact trigger, but the underlying reachability
problem is the same either way and doesn't depend on timing to be real.

**Fixed** with the standard Kafka Docker pattern: a second listener,
`EXTERNAL`, on its own port (`29092`, mapped via a new
`KAFKA_EXTERNAL_PORT` env var), advertised as `localhost:
${KAFKA_EXTERNAL_PORT}` - reachable from the host, where `kafka:9092`
never was. The existing `PLAINTEXT` listener, its port, and its
`kafka:9092` advertised address are unchanged; every in-network client
(Flink's Kafka SQL connector, `smoke_test.sh`'s `docker compose exec`
calls) keeps using it exactly as before. No single listener can serve
both audiences - a hostname a container-internal client can resolve is
never one a host-side client can, and vice versa - two listeners are the
actual fix, not a workaround around a single one. `produce_events.py`'s
invocation in `scripts/verify_recovery.sh` now points at
`KAFKA_EXTERNAL_PORT` instead of `KAFKA_PORT`.

## 35. Java 21's module system blocks Kryo's checkpoint serialization - and it's the same root cause as the log4j bug (#26/#32)

With #34's listener fix in place, the next real run got dramatically
further: `read-records: 7531` on the source vertex - genuine Kafka data,
finally, confirming #33's catalog fix works end to end - but then failed
repeatedly at checkpointing, restarted three times (`fixed-delay.
attempts: 3`), and reached `FAILED`.

```
java.lang.Exception: Could not perform checkpoint 4 for operator Source: eos_kafka_source[1] -> IcebergStreamWriter (2/2)#3.
Caused by: com.esotericsoftware.kryo.KryoException: java.lang.reflect.InaccessibleObjectException: Unable to make field final byte[] java.nio.ByteBuffer.hb accessible: module java.base does not "opens java.nio" to unnamed module @51931956
```

Kryo (Flink's fallback serializer for state types without a dedicated
native serializer) uses reflection to access private/final JDK fields -
here, `java.nio.ByteBuffer`'s internal `hb` array, needed to serialize
some part of the Kafka source's split/offset state for checkpointing.
Since Java 17, the JDK's module system enforces "strong encapsulation"
of `java.base` internals by default; without an explicit `--add-opens`
JVM flag for the specific package, this kind of reflective access
throws rather than warns. This project's Flink image is pinned to
`flink:2.1.0-java21` - well past that threshold.

Checked before assuming a from-scratch fix was needed: Flink's own
stock `config.yaml` (`apache/flink`, `release-2.1` branch,
`flink-dist/src/main/resources/config.yaml`) ships exactly this,
labelled "required for Java 17 support":

```yaml
env:
  java:
    opts:
      all: --add-exports=... --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
```

`--add-opens=java.base/java.nio=ALL-UNNAMED` is right there in Flink's
own default. This is the exact same root cause as #26/#32's sixth
addendum: `docker-compose.yml`'s `./config/flink:/opt/flink/conf` bind
mount **replaces** the image's entire conf directory, and
`config/flink/config.yaml` - adapted from `reference/`'s pre-2.1,
pre-Java21 `flink-conf.yaml` (PROVENANCE.md) - never carried these
Java-17+-support defaults forward, because the file it was adapted from
predates the Java version where they'd have mattered. The log4j bug and
this one are the same class of mistake, found twice: a committed config
file that unintentionally *replaces* upstream defaults instead of
*extending* them, discovered only once something that needed the
missing piece actually ran.

**Fixed** by adding the identical `env.java.opts.all` block to
`config/flink/config.yaml`, copied verbatim from Flink 2.1's own
default (same source and verbatim-copy discipline as #26's
`log4j-console.properties`) - not reconstructed or abbreviated.

**Confirmed working: the first fully green run of the entire pipeline.**
All 11 stages passed, including the TaskManager kill/recovery cycle.
The actual `verify_exactly_once.py` report:

```
events_emitted: 9070, events_landed_total_rows: 9070, events_landed_distinct_keys: 9070
duplicate_key_count: 0, gap_count: 0, unexpected_keys_not_in_emission_log: []
```

9070 events confirmed-delivered to Kafka, 9070 rows landed in Iceberg,
zero duplicates, zero gaps - across a real SIGKILL of the TaskManager
mid-write and a real recovery. This is genuine exactly-once, not an
assumption: the checkpoint-barrier/commit-protocol explanation written
in DEFENSE.md #19, before any of this code existed, actually held under
a real failure injection.

## 36. The "22 orphaned files" were a scope bug in the check, not a finding

The same run's report also showed `orphaned_file_count: 22` - every one
of them under `.../eos_events/metadata/` (`metadata.json` version
files, manifest `.avro` files, manifest-list `snap-*.avro` files), not
one single actual data file. Checked before reporting 22 orphans as a
real result: `parse_referenced_files` reads `$all_data_files`, which -
true to its name - only ever enumerates Iceberg's *data* layer (the
Parquet files under `data/`). It was never going to list a
`metadata.json` or a manifest file; those aren't data files, they're
the structure Iceberg's own tree-walk passes *through* to reach data
files, tracked and expired by Iceberg's own snapshot-management
mechanisms entirely separately from "was this data file's commit ever
finished."

`verify_exactly_once.py`'s Filer walk (`--warehouse-path`) was scoped to
the whole table directory - `data/` and `metadata/` both - then diffed
against a reference set that only ever covers `data/`. Every metadata
file was therefore guaranteed to look "orphaned," structurally, on every
run, regardless of whether anything was actually wrong. Not a finding
about this run's commit behavior - a scope mismatch in what was being
compared to what.

Fixed by scoping `--warehouse-path` to `eos_events/data` specifically,
matching exactly what `$all_data_files` enumerates - an apples-to-apples
comparison instead of table-directory-vs-data-layer. This is the same
discipline as DEFENSE.md #14 (a check has to be verified to test what it
claims to test) applied to this project's own verification tooling, not
just the system under test - and it was caught by actually reading the
"orphaned" list instead of trusting the count.

## 37. Step 6, re-verified 5/5 - not trusted on the first green run

Same discipline as smoke test step 5 (DEFENSE.md #24): a single pass
means nothing on its own in this project's own history (#20/#21, #32's
entire diagnostic odyssey). With #33 through #36's fixes all in place,
the identical commit was re-run five times via `gh run rerun`, not
trusted on the first green result. All five: pass, all 11 stages,
`events_emitted == events_landed_total_rows` with `duplicate_key_count:
0` and `gap_count: 0` every time, orphan count correctly scoped and
consistent with expectations.

The full path from a parked, unresolved bug to this point: #32's five
diagnostic-tooling gaps (a swallowed print, a too-late log dump, the
wrong capture command, a wrong guessed path, and finally the missing
`log4j-console.properties` root cause), #33's actual root cause (the
Iceberg catalog silently absorbing the Kafka table), #34's Kafka
listener gap, #35's Java 21 module-encapsulation gap, and #36's own
verification-script bug - each one found from real evidence, not
guessed, each one fixed and re-verified before moving to the next. Step
6 is genuinely, reproducibly green. This is the exactly-once mechanism
DEFENSE.md #19 described, before any of this code existed, working
under a real, injected TaskManager failure - not assumed, demonstrated.

**Merged to `main`, and re-verified there specifically, not assumed to
compose.** `exactly-once-validation` touched two pieces of shared base
infrastructure also used by the ordinary smoke test - the Kafka
listener config (#27/#34) and Flink's `env.java.opts.all` (#35) - and
neither had been exercised together with the smoke test path before.
Per CLAUDE.md's V4, ran `scripts/smoke_test.sh` via CI 5 times on the
merged `main`, not just once: 5/5 pass. The two changes compose
correctly; nothing about combining them broke steps 2-5.

## 38. Part 2.1: NYC TLC downloader and time-compressed replay producer

Written before any of this code, per the standing rule reaffirmed this
session: the explanation comes first, or the code doesn't get written.
This work only needs Kafka, already green - deliberately started while
the `exactly-once-validation` branch's own bug (DEFENSE.md #32) stays
parked and unresolved rather than blocking on it.

**Dataset scope.** Yellow Taxi trip records only, not green/FHV/HVFHV -
the most standard, widely-referenced TLC dataset, and one dataset is
enough to validate the download-and-replay mechanism itself. Default
month `2025-01` (real file, confirmed reachable: `curl -I` against
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet`
returned `200 OK`, `Content-Length: 59158238`), overridable via
`--year-month` - not hardcoded as the *only* option, since TLC publishes
monthly and a real benchmark run will eventually want more than one
month's data.

**Download mechanism: Python's stdlib `urllib`, not `requests`.** Not
asked as a dependency question because it doesn't need to be one - a
streamed HTTP GET with a status check is well within what `urllib`
handles directly, and `requests` would be a second HTTP client library
in a project that has had zero need for one until now. `pyarrow` (asked
about, approved) is the only new dependency this component adds.

**Trip identity: synthetic, not a TLC-provided field.** The Yellow Taxi
schema has no natural per-trip unique key - confirmed by reading each
downloaded file's own embedded Parquet schema at runtime rather than
assuming a fixed, hardcoded column list (TLC's schema has changed
slightly release to release; reading the file's actual schema and
failing loudly if an expected column - `tpep_pickup_datetime` - is
missing is more robust than trusting a column list written down once).
The replay producer generates a key from the row's ordinal position in
the sorted-by-pickup-time sequence (`tlc-yellow-2025-01-000001`, ...) -
stable and unique for a single file, sufficient for what this component
does (produce realistic-shaped traffic), not intended as a durable
cross-run trip identifier.

**Time compression.** Sorted by `tpep_pickup_datetime` ascending, then
replayed with the wall-clock gap between consecutive sends equal to the
real inter-arrival delta divided by `--speed-factor` (default `3600`:
one real hour of trip arrivals compressed into one replayed second) -
preserving the *shape* of the arrival pattern (rush-hour bursts, overnight
lulls) rather than a flat, unrealistic constant rate. Capped at
`--max-inter-arrival-sleep` (default `5` seconds): a real gap in the data
(an overnight lull, a data quality hole) divided by 3600 could still be
a multi-minute wait uncompressed further, and this is a replay tool, not
a faithful real-time simulator - the cap is a deliberate, stated
tradeoff, not an unexamined shortcut.

**Zone IDs left unresolved.** `PULocationID`/`DOLocationID` are passed
through as TLC's own integer zone IDs, not joined against TLC's separate
`taxi_zone_lookup.csv` to resolve human-readable borough/zone names.
Resolving them is a real, addable enhancement, not attempted here - it's
a second dataset and a join this component doesn't need to prove out the
download-and-replay mechanism itself.

**Producer library: `kafka-python`, reusing DEFENSE.md #25's decision,
not a fresh dependency question.** Same delivery-confirmed-callback
reasoning applies, though this component doesn't need step 6's emission
log specifically - it's producing realistic seed/benchmark traffic, not
validating exactly-once semantics, so a delivery failure is logged and
raised, not written to a ground-truth file nothing downstream compares
against yet.

**Runtime lineage:** not emitted by this producer. Runtime lineage
emission is deferred project-wide (CLAUDE.md rule 7, docs/FUTURE_WORK.md,
README's "Lineage: declared, not runtime-emitted" section) - this
component doesn't carve out an exception for itself.

**V5 audit, after CLAUDE.md's Verification Standard landed - this
component predated it and wasn't automatically compliant.** The first
version of `replay_producer.py` incremented a `sent` counter immediately
after calling `producer.send()`, registering only an error callback -
"sent" meant "attempted," not "broker-confirmed," even though the
variable name and the final log line ("done, sent N trips") read as if
it meant the latter. `produce_events.py` (step 6, DEFENSE.md #25) never
had this problem - it was built with the delivery-callback discipline
from the start, since exactly-once validation makes ground truth the
entire point. This component's own purpose (realistic seed/benchmark
traffic, not exactly-once verification) doesn't need a persisted
emission log the way step 6 does, but it still shouldn't misreport what
"sent" means. Fixed by tracking `attempted` (send() calls) and
`confirmed` (successful on_success callbacks) as two separate counters,
reporting both, and failing loudly if they don't match after `flush()` -
rather than trusting a count that was never actually checked against the
broker's own acknowledgment.

## 39. Part 2.1 completion: wiring the replay producer to live Kafka and verifying it for real

Written before any of this code, per the standing rule. Four things
need proving, not assuming: timestamps survive the round trip, the
compression ratio is what it claims to be, resuming from an event-time
offset is gap-free and duplicate-free at the seam, and (re-audited,
this time against the actual current file rather than assumed) whether
V5 still holds after this session's earlier fix.

**V5 re-audit, this time - result: compliant, not a violation.**
Re-read `replay_producer.py` line by line against V5 rather than
trusting the earlier fix (DEFENSE.md #38's V5 audit) blindly.
`confirmed` increments only inside `on_success`, which only fires from
a delivery callback; the `attempted % 1000` progress line and the final
summary both say "attempted", never "sent", for the send-call count
specifically; and `main()` still hard-fails if `confirmed != attempted`
after `flush()`. No send-call-as-truth logging found anywhere in the
current file. Confirmed compliant, not assumed.

**Emission log: reversing #38's own "doesn't need one" call.** #38
reasoned this component didn't need step 6's persisted emission log
since it isn't validating exactly-once semantics. That reasoning held
for its original scope; it doesn't hold for verifying timestamp
integrity or a resume seam, both of which need a trustworthy record of
*which* rows were confirmed-sent and *what timestamp* each one carried,
across two separate process invocations for the resume case
specifically. Adding `--emission-log` (optional, off by default),
writing exactly what `produce_events.py` already writes on confirmed
delivery - not new design, reuse of an already-proven pattern once the
actual need for it showed up.

**Resume seam: verified the sort's determinism before designing around
it, not assumed.** A tie-safe resume (`--resume-after-timestamp T
--resume-skip-ties K`: skip every row with `pickup < T`, then skip the
first `K` rows with `pickup == T`) is only gap-free and duplicate-free
across two separate runs if the *same* input file sorted the *same* way
produces the *same* row order for tied timestamps every time - taxi
pickup times are recorded to the second, and dense periods genuinely
have multiple trips per second, so ties at an arbitrary resume boundary
are a real scenario, not a hypothetical. Checked before relying on it:
`pyarrow.Table.sort_by()` is documented as a stable sort (ties keep
their original relative order), and `pq.read_table()` on the same file
always reads rows in the same on-disk physical order - stable sort of a
deterministic input order is deterministic output order, ties included,
across separate process invocations. No compound tie-breaking sort key
needed; the existing `sort_by(PICKUP_COLUMN)` is already safe for this,
confirmed rather than assumed.

**Time-compression ratio test: isolated from the cap on purpose, not
by accident.** `--max-inter-arrival-sleep` caps any single gap's
replayed delay (DEFENSE.md #38) - correct production behavior, but it
would corrupt a naive `event_time_span / wall_clock_elapsed ≈
speed_factor` check if any gap in the tested sample actually hit the
cap, since a capped gap contributes less wall-clock time than the ratio
predicts. Rather than reimplementing the capped-sum formula a second
time inside the test (a redundant parallel implementation that could
itself be wrong, testing itself more than the system), the verification
run uses a deliberately large `--max-inter-arrival-sleep` (effectively
disabling the cap for this one run) so the simple ratio check is
exactly correct for what it's checking, with the cap's own behavior
untested here on purpose - that's a different property, already
covered by #38's own reasoning for why the cap exists at all.

**Wall-clock ground truth: the broker's own message timestamps, not the
producer's.** Same V5 principle applied to a new measurement: elapsed
wall-clock time for the ratio check is computed from the *consumed*
messages' broker-assigned timestamps (`ConsumerRecord.timestamp`, when
Kafka actually received each message), not from timers inside
`replay_producer.py` itself. The producer's own clock is exactly what's
under test here - measuring it with itself would be circular.

**Live-Kafka wiring: Kafka only, no Flink/Iceberg.** This work has only
ever needed Kafka (DEFENSE.md #38's own framing) - the verification
workflow brings up `docker compose up -d kafka` alone, not the full
stack, keeping this test fast and free of dependencies step 6 already
covers elsewhere.

**Caught before shipping, not in a real CI run:** the workflow's first
draft waited for Kafka to become healthy via `docker compose ps kafka
--format '{{.Health}}'` - checked against `docker compose ps`'s own
documentation before trusting it, which only supports `pretty` (the
default) or `json` as `--format` values, not an arbitrary Go-template
field. Fixed by reusing `smoke_test.sh`'s already-proven pattern (grep
the plain-text output for this service's line and the literal
`(healthy)` substring) instead of inventing a second, unverified one.

**Verified 5/5, not trusted on one pass - per CLAUDE.md V4.** All three
checks passed clean on the first real run, then re-run four more times
via `gh run rerun`: 5/5. Real measured numbers, not illustrative: 500/500
landed timestamps exactly matched source data on every run; the
compression-ratio check measured `actual_ratio=59.96` against a
configured `speed_factor=60.0` (0.1% relative error, well inside the
25% tolerance) on the first run, similarly tight on the others; the
resume-seam check landed exactly 500/500 in each half with zero overlap
and zero gaps every time. Part 2.1 is genuinely done, not just built.

## 40. Part 2.2: measuring real lateness, and why the naive replay can't produce any

Written before `scripts/measure_lateness.py` or the shuffle feature it
depends on, per D1. The question this has to answer isn't "does the
code run" - it's "is the number it produces actually a lateness
distribution, or an artifact of how the test was built."

**Why naive replay measures nothing.** The source Parquet is sorted by
`tpep_pickup_datetime`, and `replay_producer.py` sends in that exact
order - every event's arrival relative order matches its event-time
order perfectly, by construction. Measuring "lateness" against that
replay would report ~0 for everything, correctly reflecting the test
setup and telling us nothing about how a watermark should actually be
tuned for disorder that hasn't been reproduced yet.

**Out-of-order model: bounded window shuffle (your choice from the
three options presented).** Partition the chronologically-sorted rows
into fixed, non-overlapping windows of size `--shuffle-window W`, and
shuffle *only the send order* within each window - `event_ts` itself is
never touched, only which row goes out at which position. Deterministic
worst case by construction: an event can never actually be more than
`W-1` positions out of true order, so the resulting lateness
distribution has a hard ceiling implied directly by `W`, not an
open-ended tail. That's the tradeoff already named when this model was
chosen over the other two: clean and parameterized, at the cost of a
uniform-ish shape within the bound rather than a realistic long tail.
Reproducible by default via a fixed `--shuffle-seed` (documented, not
hidden) - a measurement meant to justify a real config change has to be
re-derivable, not a one-off random result nobody can reproduce.

**Pacing stays tied to true chronological order, not shuffled order.**
Each row's own wall-clock sleep (the existing `min(delta/speed_factor,
max_sleep)` formula, DEFENSE.md #38) is computed from its real
chronological predecessor's timestamp, *before* the window shuffle is
applied to decide send order. Windows are non-overlapping and shuffling
only permutes within one, so the sum of sleeps per window - and
therefore total replay duration and the compression ratio verified in
#39 - is unchanged by turning shuffling on. Only the local arrival
order moves; the overall pacing envelope doesn't.

**Lateness definition: running-max-based, not wall-clock/broker-
timestamp-based.** For each event, in the order it actually arrives:
`lateness = (running_max_event_time_seen_so_far - this_event's_event_ts)`,
then update the running max. This is the standard operational
definition streaming watermarks are actually tuned against - "how far
behind the newest timestamp seen so far is this one" - not how it
compares to some external wall clock. Broker/consume timestamps are
deliberately not used here (unlike DEFENSE.md #39's ratio test, where
they were the right ground truth for a different question): the
disorder under test is the *synthetic shuffle*, and measuring against
real infra timing would conflate that with unrelated network/consumer
jitter, muddying the one thing this measurement exists to characterize.

**Percentiles: Python's stdlib, no new dependency.**
`statistics.quantiles()` (3.8+) computes p50/p95/p99/p999 and the
histogram bucketing directly - `numpy`/`scipy` were not needed and
weren't asked about as a result.

**Watermark bound proposal: p99 as the default recommendation, stated
with its actual drop rate, not treated as a settled choice.** The
script reports the full distribution (all percentiles, max) and
recommends bounding at p99 specifically because it's the standard
starting point for "cover the overwhelming majority, treat the
remainder as a real, acknowledged tradeoff" - not p999 (see the
correction below for why, written after this paragraph was found
wrong) and not p50/p95 (cheap in latency, but a materially larger drop
rate on a distribution this bounded). The *reported* drop-rate-at-p99
is the actual number to make the call from, not the recommendation by
itself - this project's own rule (CLAUDE.md hard rule 1) is that no
fabricated number substitutes for a measured one, and that applies
exactly as much to this recommendation as to anything benchmarks/
produces.

**Correction, after running the CI-canonical measurement.** The
paragraph above originally justified skipping p999 by claiming "the
tail between p99 and p999 is compressed anyway" - written
speculatively, before any measurement existed, since per D1 the design
has to be written before the code. That claim was never re-checked
against real output before being left in place, which is exactly the
kind of unearned, unverified number CLAUDE.md hard rule 1 exists to
catch - a design rationale is not exempt from it just because it isn't
a benchmark metric. Running `scripts/measure_lateness.py` through live
Kafka (N=2000, `--shuffle-window 50`, `--shuffle-seed 42`, the "Measure
lateness distribution (Part 2.2)" step of `replay-verify.yml`, run
32672957098) against the real January 2025 dataset produced:

```
p50=12.0s  p95=41.0s  p99=270.0s  p999=11309.0s  max=11601.0s
```

The p99-p999 gap is ~42x, not compressed - the original claim was
wrong. What's actually happening is a property of the window-shuffle
model itself, not the pacing formula: `--shuffle-window` bounds a fixed
*row count* per window, not a fixed *event-time span*. NYC taxi pickup
density varies enormously by hour (the rush-hour-bursts-vs-overnight-
lulls shape `replay_producer.py`'s own docstring names, DEFENSE.md
#38) - a 50-row window during a dense period spans seconds of event
time, while the same 50-row window overnight can span many minutes.
The three events landing in the 10440.9-11601.0s histogram bucket are
real sparse-period windows, not a modeling artifact.

This doesn't change the p99 recommendation, but it changes *why*: p99
is preferred not because the tail past it is cheap or compressed, but
because that tail is density-driven and effectively open-ended -
chasing p999 would mean accepting an operationally unacceptable
watermark bound (11309s, ~3.1 hours) for 0.1% of events, a cost that
has nothing to do with the shuffle bound `W` and everything to do with
how unevenly real trips arrive over a day. At the measured p99 bound
(270s, ~4.5 min), 20/2000 events (1.0%) would be dropped as too-late.

## 41. Part 3.1: the metrics store - design, before any code

Written before the Flink metrics job or the Postgres schema, per D1.
Designed against what `yellow_tripdata_2025-01.parquet` actually
contains, not against an assumed-clean dataset - inspected directly
(`pyarrow`, not TLC's docs) before writing anything below:

- **~15.5% nulls**, concentrated in exactly five columns
  (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`,
  `congestion_surcharge`, `Airport_fee`) - correlated with `VendorID`
  but not perfectly (VendorIDs 1, 2, and 6 all contribute rows to it,
  not one single vendor that "doesn't report these fields").
- **Negative dollar amounts** on `fare_amount`, `tip_amount`,
  `tolls_amount`, `total_amount` (down to -$901) - these are real
  refund/correction rows in TLC data, not a parse error, so a metrics
  store that flags them has to count them, not reject them.
- **Extreme outliers**: `trip_distance` max = 276,423.57 (miles,
  presumably - clearly wrong either way), `fare_amount` max =
  $863,372.12.
- **124 negative-duration trips** (`tpep_dropoff_datetime` before
  `tpep_pickup_datetime`) and **1,927 zero-duration trips** - neither
  column is null-checked against the other by TLC before publishing.
- **22 rows with `tpep_pickup_datetime` outside the file's nominal
  month** - TLC's monthly files routinely leak a handful of rows from
  adjacent months; this has no equivalent concept in a live Kafka
  stream (there's no file-month boundary), so it's reframed below as
  a watermark-lateness metric instead of a standalone one.

**Decision 1: metrics computed inline in the Flink job, not by a
separate batch reader over Iceberg (your call).** The alternative -
a standalone job periodically querying Iceberg after ingestion - was
real and stated before this was decided: it would decouple metrics
logic from the streaming pipeline (change what's measured without
redeploying the ingestion job) at the cost of latency (metrics lag
ingestion by however often that job runs) and a second read pass over
the same data. Inline wins because CLAUDE.md already commits to
instrumenting detection latency starting this sprint - a metrics path
with a built-in lag before it even reaches the detector would
undercut the number this project exists to publish.

**Decision 2: fixed event-time tumbling windows, not fixed row-count
batches (your call).** The alternative was real and already
demonstrated to have a real cost: DEFENSE.md #40 measured that a
fixed *row-count* window (the shuffle window) spans wildly different
event-time durations depending on local pickup density - the same
problem would recur here if metrics windows were row-count-based,
making "what happened at 3am" unanswerable without a lookup. Fixed
time windows keep the window key directly meaningful. Window size:
**1 minute of event time**, a parameter (not hardcoded), chosen as
fine-grained enough to localize an incident to roughly the minute
(the "localizes" half of this project's own mission statement) while
still holding enough rows per window to make a rate-based metric
(null %, negative %) statistically meaningful outside of the sparsest
overnight windows. Not claimed as tuned or final - like the p99
bound in #40, it's a stated default subject to revision once the
detection engine is actually run against it.

**Decision 3: narrow (long) schema in Postgres, not one wide row per
window.** Real alternative: a wide table (one column per metric per
source column) would need a migration every time a new metric or
source column is added, and would already need on the order of 20
columns x 3-4 metrics = 60-80 columns for this dataset alone. A
narrow table - one row per `(window_start, window_end, column_name,
metric_name, metric_value)` - trades that off against needing a
`GROUP BY`/join to reconstruct a window's full picture, in exchange
for adding a new metric or column being a data-insert, not a schema
migration. Two tables, not one:
  - `window_metrics` (one row per window): `window_start`,
    `window_end`, `row_count`, `late_event_count`,
    `lateness_p50_seconds`, `lateness_p99_seconds`,
    `lateness_max_seconds` - the watermark-lateness numbers absorb
    the "22 rows outside the nominal month" finding above: in a live
    stream that's not a month-boundary concept, it's just lateness
    beyond whatever bound #40 recommends, and it's already being
    measured for exactly that reason.
  - `column_metrics` (one row per window per source column per
    metric): `window_start`, `window_end`, `column_name`,
    `metric_name`, `metric_value`. Indexed on `(column_name,
    metric_name, window_start)` - the access pattern a detector
    needs is "give me column X's null-rate time series," not "give
    me everything about window Y."

**What gets computed per column, and why each one is grounded in a
real finding above, not a guess:**
  - `null_count` - every column, all five null-heavy columns found
    above are covered by this alone.
  - `distinct_count` - categorical columns only (`VendorID`,
    `RatecodeID`, `payment_type`, `PULocationID`, `DOLocationID`,
    `store_and_fwd_flag`) - a cardinality jump (e.g. a new
    `PULocationID` appearing) is itself a detectable incident
    signature, independent of any rate.
  - `negative_count` - every numeric column where negative is
    possible in the schema but domain-nonsensical on its own
    (`fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`,
    `trip_distance`, `passenger_count`) - a pure count against the
    fixed threshold zero, not a judgment call about what's an
    outlier, so it needs no invented bound (CLAUDE.md hard rule 1).
  - `min`, `max`, `mean` - every numeric column. Deliberately NOT
    accompanied by a stored "is this an outlier" flag or bound in
    this table - classifying $863,372.12 as anomalous requires a
    baseline to compare against (a prior window, or a
    calibration pass), which is the detection engine's job (not yet
    built), not something for the metrics store to hardcode a
    magic threshold for. The metrics store's job is descriptive
    statistics; anomaly classification is a separate, later
    component that reads this table, deliberately kept out of it so
    this component doesn't quietly become both.
  - `negative_duration_count`, `zero_duration_count` - derived from
    `tpep_dropoff_datetime - tpep_pickup_datetime`, not a raw column;
    stored as `column_name = 'trip_duration'` rows in the same
    `column_metrics` table rather than a separate table, since
    they're the same shape of fact (a count against a fixed
    threshold) as `negative_count` above.

Rates (null %, negative %, etc.) are deliberately NOT stored
pre-divided - `column_metrics.metric_value` holds the raw count, and
`window_metrics.row_count` holds the denominator; a rate is computed
at query time via a join. Storing a pre-divided rate would be a
second, derived copy of the same fact with its own rounding, and
CLAUDE.md's discipline throughout this project has been to keep
exactly one stored ground truth per fact (the broker's delivery
callback, not "we called send()"; the running-max lateness
definition, not a second wall-clock one) - this is the same principle
applied to the metrics store.

## 42. Part 3.1: the Flink job - wide staging table + Postgres trigger,
not a Flink-side pivot

Written before `scripts/sql/metrics_job.sql`, per D1. #41 decided
*what* gets computed and *where* it's stored; this entry decides the
mechanism that gets a windowed aggregation with ~70 metric values
(19 null counts, 6 distinct counts, 6 negative counts, 12 x 3
min/max/mean, 2 duration-derived counts, plus `row_count`) from one
Flink SQL query into `column_metrics`'s narrow (one-row-per-metric)
shape.

**Real alternative considered and rejected: pivot inside Flink SQL,
via `CROSS JOIN UNNEST(ARRAY[ROW(...), ROW(...), ...])`.** Confirmed
this syntax is real (Flink's array-of-ROW literals plus
`UNNEST`/`CROSS JOIN` to explode them into rows is documented,
verified via the Ververica SQL cookbook and Flink's own docs, not
guessed) - so it would work. Rejected anyway: this session's actual
track record is that every genuinely novel piece of Flink SQL syntax
tried so far (the Iceberg-catalog connector-property absorption in
DEFENSE.md #33, the JSON timestamp format needed below) has surfaced
a real bug on first contact, and a single ~70-entry array-of-ROW
literal, hand-written and never run until CI, is exactly the shape of
thing likely to hide a transcription error in the one place (a
70-way melt) that's hardest to spot-check by eye.

**Decision: Flink writes one wide row per window; Postgres reshapes
it.** The Flink SQL job does nothing but the parts of standard SQL
this project already has real, working experience with this
session - a Kafka source, a watermark, a `TUMBLE` window, a
`GROUP BY` with aggregate functions - and sinks the wide result via
the JDBC connector's upsert mode (real, documented: a JDBC sink table
with a `PRIMARY KEY` in its DDL performs `INSERT ... ON CONFLICT DO
UPDATE`, not append-only) into a new staging table,
`weir_metrics.window_metrics_wide`, keyed on `(window_start,
window_end)`. A Postgres trigger (`AFTER INSERT OR UPDATE`) on that
table fans each wide row out into `window_metrics` (the fixed
columns) and `column_metrics` (via Postgres's own
`UNNEST(ARRAY[...])`, which - unlike the Flink-side version above -
this project can rely on without a first-contact-bug tax; it's
mature, ubiquitous Postgres syntax). This also means the melt logic
lives in a schema migration, not a Flink job redeploy, if a metric's
definition ever needs to change.

**Watermark bound: 270 seconds - #40's measured p99, not a new
number.** `WATERMARK FOR tpep_pickup_datetime AS
tpep_pickup_datetime - INTERVAL '270' SECOND`. Reusing the bound
DEFENSE.md #40 already measured and justified (rather than picking a
fresh one here) means the two design phases agree with each other by
construction, and the "20/2000 events (1.0%) dropped as too-late"
cost #40 already quantified is the real, known cost of this exact
value - not a second guess that would need its own justification.

**Timestamp parsing: `json.timestamp-format.standard = 'ISO-8601'`,
verified against Flink's own docs, not assumed.** `replay_producer.py`
serializes `tpep_pickup_datetime`/`tpep_dropoff_datetime` via Python's
`datetime.isoformat()` (DEFENSE.md #38's `_json_default`), producing
`T`-separated strings like `"2024-12-31T20:47:55"` - confirmed by
actually loading a real row and printing its serialized JSON, not
assumed from reading the code alone. Flink's JSON format connector
defaults to the SQL style (space-separated) and requires this option
set explicitly to parse the `T`-separated form Flink calls
`'ISO-8601'`. Also confirmed (via `pyarrow.compute.microsecond`
against the full real January 2025 file) that all 3,475,226 pickup
timestamps carry zero microseconds, so there's no sub-second
formatting inconsistency to worry about across rows.

**Lateness columns left NULL by this job.** `window_metrics.
late_event_count`/`lateness_p50_seconds`/`lateness_p99_seconds`/
`lateness_max_seconds` are populated by the trigger as NULL for every
window this job writes - already flagged as a separate, not-yet-built
step in #41's schema design (computing a true per-window late-arrival
count needs Flink's side-output/DataStream-level late-data handling,
not plain SQL), not a new gap introduced here.

**Scope match against #41: no metric added or dropped.** The wide
table's column list is exactly #41's metric list - this entry is
about the mechanism that gets that list into the narrow schema, not a
revision of what the list contains.

**Found via CI, not caught beforehand: `INTERVAL '270' SECOND` needs
`SECOND(3)`.** The first real submission of this job (after #43's jar
gap was already fixed and verified separately) failed at parse time:
`org.apache.calcite.sql.validate.SqlValidatorException: Interval
field value 270 exceeds precision of SECOND(2) field`. Calcite's
interval-literal grammar - which Flink SQL uses as-is, including
inside a `WATERMARK FOR` clause - defaults an unqualified `SECOND` to
2-digit precision; 270 needs three. Confirmed against Calcite's own
`SqlIntervalQualifier` javadoc before applying the fix
(`INTERVAL '270' SECOND(3)`), not guessed from the error text alone.
Caught specifically because `verify_metrics_job.py`'s own stdout
buffering (a separate, real gap - CPython fully block-buffers a
piped, non-tty stdout by default) was fixed first: the initial CI
failure showed zero diagnostic output, and only reran with `python
-u` did the actual Calcite exception become visible at all.

**Found next via CI: an idle parallel source subtask blocked every
window from ever firing.** With the interval fix in place, the job
submitted and ran cleanly - checkpoints completed every ~10s for the
full 180s wait, no errors - but no window ever landed in Postgres.
`verify_metrics_job.py`'s test topic has 1 partition; this job's
parallelism was left at its default (2, visible in the JobManager log
as subtasks "(1/2)"/"(2/2)"). Only one subtask ever gets the single
partition assigned; the other is idle from the start, and Flink's
merged watermark is the *minimum* across all parallel source
subtasks - an idle subtask that never advances its own watermark
holds every window open indefinitely, regardless of how much real
data flows through the active one. This is a documented Flink
behavior (confirmed against the Kafka connector docs' own "Source
Per-Partition Watermarks" section and cross-checked against
`table.exec.source.idle-timeout`'s entry in Flink's table-config docs:
type Duration, default `0 ms` - disabled unless set), not something
guessed from the symptom alone. Fix: `SET
'table.exec.source.idle-timeout' = '5s';` added alongside the other
session-scoped `SET`s already in this file. A steadily-completing
checkpoint count was not, on its own, enough to conclude the job was
producing correct output - it only proves the job is alive, not that
data is flowing through to the sink.

**Constraint on any future change to this job: the fix is idleness
detection, not parallelism pinned to 1, and not a partition-count
change.** Two other "fixes" would have made this specific CI run pass
just as well: setting this job's parallelism to 1 (matching the test
topic's single partition), or giving the test topic more partitions.
Neither was chosen, and neither is an acceptable substitute for
`table.exec.source.idle-timeout` going forward - both only remove the
*symptom* for this one topic's current shape, and would silently
reintroduce the exact same bug the moment a real deployment's Kafka
topic has more partitions than this job's parallelism, or an uneven
partition-to-subtask assignment (both routine in production Kafka,
not edge cases). `table.exec.source.idle-timeout` is the only one of
the three that stays correct regardless of how partitions and
parallelism relate to each other. Any future change to this job's
parallelism, or to the source topic's partition count, must keep this
setting - removing it would not fail loudly, it would silently
reintroduce this exact bug (every window blocked forever) with no
error in the logs, exactly as it did before this was diagnosed.

**Found next via CI: the UNNEST(ARRAY[ROW(...), ...]) mechanism this
entry originally chose was itself wrong.** With the idle-timeout fix
in place, the job ran, checkpointed, and the wide row landed in
`window_metrics_wide` - but Postgres's own log showed the trigger
failing on every invocation: `ERROR: function return row and
query-specified return row do not match`. This is a genuine Postgres
composite-type inference limitation, not a typo: building an array
from many separate anonymous `ROW(...)` constructors doesn't reliably
resolve to one uniform composite type Postgres can then `UNNEST(...)
AS m(column_name text, metric_name text, metric_value double
precision)` against, even when every individual field looks
correctly typed. The real irony: this entry rejected pivoting inside
*Flink* SQL specifically because novel syntax there kept finding
bugs, then hit the equivalent problem on the Postgres side instead -
"battle-tested" was true of `UNNEST(ARRAY[...])` on simple arrays, not
of this specific many-anonymous-ROW-literals usage of it. Fixed by
switching to a literal `VALUES (...), (...), ...) AS m(cols)` list -
each column's type unifies independently down its own column instead
of needing one shared row type across all 69 entries, which is
exactly what a fixed, known-shape tuple list needs and has no
equivalent ambiguity.

**Verified 5/5, not trusted on one pass.** All four bugs above were
found and fixed across several individually-failing CI runs; once the
trigger fix landed, `.github/workflows/metrics-job-verify.yml` was run
5 consecutive times (run IDs 32844933111, 32845312440, 32845730285,
32846100518, 32846483857) - 5/5 green, each one independently
rebuilding the Flink image from scratch, replaying real TLC data, and
running both the deep check (all 69 `column_metrics` values plus
`window_metrics.row_count` for one specific window, exact match
against ground truth computed independently from the source parquet)
and the broad check (`SUM(row_count)` across every closed window
exactly matching an independently-computed expected count from the
emission log). Neither check has ever passed on a fluke - every
value compared is either an exact integer/float match or the run
fails loudly.

## 43. Flink's JDBC connector jars target Flink 2.0.0, not this
project's 2.1.0 - stated explicitly, not left implicit

Added to `docker/flink/Dockerfile` for #42's mechanism (a JDBC sink
into `weir_metrics.window_metrics_wide`), approved before being added
per CLAUDE.md hard rule 6.

**Two jars needed, not one - checked, not assumed.** Kafka's connector
is a single self-contained uber jar
(`flink-sql-connector-kafka`, bundling its own transitive deps -
already in this Dockerfile). Searched Maven Central's own search API
for a `flink-sql-connector-jdbc*` equivalent - none exists for any
version. Flink 4.x's JDBC connector is split into a thin core jar
(`flink-connector-jdbc-core`, the generic `JdbcDynamicTableFactory`)
plus a separate per-database dialect jar
(`flink-connector-jdbc-postgres`), neither of which bundles the
other or the driver - the driver jar this Dockerfile already had
(`postgresql-42.7.13.jar`) turns out to have been necessary but not
sufficient; both new jars are needed alongside it.

**Version caveat, stated explicitly rather than left implicit.**
`org.apache.flink:flink-connector-jdbc-core`/`-postgres` exist on
Maven Central at exactly three versions total (confirmed via Maven
Central's search API, not browsed-and-guessed): `3.3.0-1.19`,
`3.3.0-1.20`, `4.0.0-2.0`. The `-2.0` suffix reads as "built for
Flink 2.0.x" by the same convention this Dockerfile's Kafka connector
already uses (`4.0.1-2.0`) - confirmed for certain by fetching the
actual `pom.xml` at both the `v4.0.0` git tag AND the newer `v4.1.0`
tag (not yet published to Maven Central under this artifact) from
`apache/flink-connector-jdbc`: both declare
`<flink.version>2.0.0</flink.version>` verbatim. This image runs
Flink **2.1.0** (`docker/flink/Dockerfile`'s own `FROM` line) - `4.0.0-2.0`
is the only JDBC connector build that exists at all, and it was built
and tested against 2.0.0, not 2.1.0. Same-minor-line (2.x)
compatibility is a reasonable bet - Flink's own connector API doesn't
typically break within a major version - but it is a bet, not a
confirmed match, and this entry exists so that bet is on the record
rather than silently assumed. If a JDBC-specific incompatibility ever
surfaces, this is the first place to look.

**Verification, not just a successful build.** Per E7 (a bind mount
has silently discarded image defaults twice already), jar presence
was checked inside the actual container as `docker compose` runs it -
mounts and all - not just confirmed as present in a bare `docker
build` layer. See the same commit's CI run for the real output of
`docker compose exec flink-jobmanager ls /opt/flink/lib`.

## 44. The volume detector's design, before any detector code

Written before `reliability/volume/`'s implementation, per D1. Studied
(not copied) `reference/streaming-lakehouse-lab/pyflink_jobs/src/jobs/
ewma_ad.py` first - PROVENANCE.md's "Studied, not copied" section
records that read. That job's actual algorithm: a single continuously-
updating EWMA mean and EWMA-variance per key (device_id), no
time-of-day/day-of-week structure at all, anomaly flagged at
`|x - EWMA| > K * sqrt(Var)`, K=3.0. Weir's volume detector diverges
from it in three real ways, each with a reason:

**1. Bucketed by (weekday, hour-of-day), not one global running
baseline.** A single EWMA over `window_metrics.row_count` would treat
every rush-hour burst and overnight lull as deviation from "normal,"
constantly firing on ordinary daily/weekly seasonality already
documented in this project's own data findings (#38, #41). Rejected
alternatives, both real and considered: (a) STL/seasonal decomposition
- explicitly separates trend from seasonality, but is a batch fit
requiring periodic re-fitting over a full history window, not a
natural per-window streaming update; wrong shape for a job that
processes one window at a time. (b) A flat rolling average over the
last N same-bucket occurrences - simpler to explain, but needs to
store N raw historical values per bucket instead of one compact
running statistic, and weights an 8-week-old occurrence the same as
last week's, reacting slower to genuine regime shifts.

**2. Bucket granularity is (weekday, HOUR), not (weekday, hour,
minute).** A (weekday, hour, minute) bucket only recurs once every 7
days - at most 4-5 occurrences in this project's entire dataset,
nowhere near enough to build any baseline, in this project's lifetime
or a real deployment's early months. (weekday, hour) gives ~60
observations/week/bucket (one per 1-minute window within that hour),
and - because every weekday-hour combination recurs exactly once every
7 days - all 168 buckets warm up in lockstep with calendar time, not on
a staggered per-bucket schedule.

**3. Modified (robust) z-score, not the reference's variance-based
one - and the exact Iglewicz-Hoaglin formula, not an invented one.**
`z = 0.6745 * (x - ewma_median) / scale`, flagged at `|z| > 3.5`
(Iglewicz, B. and Hoaglin, D.C. (1993), "How to Detect and Handle
Outliers", ASQC Quality Press - the standard modified-z-score
convention; 0.6745 and 3.5 are its established constants, not
independently chosen). A MAD-based scale is far less sensitive to a
single unusual historical week than the reference's variance-based
one, which is what this project needs given the baseline itself will
contain real seasonal spikes and holidays (below) that a variance
estimate would let distort the threshold for a long time afterward.

**Warmup: `min_baseline_weeks = 8` (-> `min_observations = 480`,
derived, not a separate magic number), and honest about what that
buys.** Below threshold, the detector emits a distinct
`status = "insufficient_baseline"` record - `score = None`, no
incident ever written - not a suppressed score or a score of 0. The
real sample size that matters for MAD stability is *distinct weekly
occurrences* (8), not raw per-window updates (480, autocorrelated
within each hour) - and 8 is genuinely on the low side of robust-stats
guidance, which generally wants n>=20-30 for a well-behaved MAD. This
is stated as a floor against the worst cold-start noise, not a claim
of full maturity at exactly 8 weeks. Consequence, stated explicitly
rather than left implicit: **this project's measured false-positive
rate for the volume detector should be read as an upper bound, not a
settled number** - a longer baseline than this dataset's available
history would likely lower it further, and CLAUDE.md hard rule 1 (no
fabricated/inflated-precision metrics) applies as much to *how a real
number is characterized* as to whether it's real at all.

**MAD = 0 floor: Poisson-justified, not an arbitrary constant.**
`scale = max(mad, sqrt(ewma_mean), 1.0)`. Row count is a count of
arrivals; under a simple homogeneous-arrivals assumption, variance ~=
mean, so `sqrt(mean)` is a principled lower bound on plausible noise
for a bucket of that magnitude - self-scaling (a quiet overnight
bucket gets a small floor, rush hour a larger one), with an absolute
floor of `1.0` underneath for the zero-mean edge case, since row
counts are integers and a sub-1 difference is meaningless. Unit-tested
explicitly against identical historical values (MAD would otherwise be
exactly 0).

**DST: drop the ambiguous hour, don't guess at UTC normalization.**
Confirmed empirically, not assumed, that `tpep_pickup_datetime` is
naive local (America/New_York) civil time: Nov 3 2024's hour=1 had
9,869 trips against 5,606 on the equivalent Saturday (Nov 2) - a ~76%
jump isolated to exactly the fall-back hour, consistent with that
wall-clock hour occurring twice that night; UTC storage would show no
such anomaly. A pure fall-back (which Nov 3 2024 is) doubles one
bucket's data for that week; it does not zero one out - that's
spring-forward's effect, not present in the Oct 2024-Jan 2025 window.
"Normalize to UTC" was considered and rejected: doing so would require
disambiguating which of the two "1:00-1:59am" batches is pre- or
post-transition, which is NOT recoverable from a naive timestamp alone
without an unverifiable assumption - exactly the kind of fabricated
certainty this project's discipline forbids. Dropping the ambiguous
hour (Nov 3 2024, 01:00:00-01:59:59 local, that week only) from both
warmup counting and scoring is a small, explicit, honest exclusion
instead.

**Holidays: stay in the baseline, on purpose.** Thanksgiving,
Christmas, and New Year's fall inside the Oct 2024-Jan 2025 window and
are genuinely anomalous ridership. Excluding them would be curating
the baseline to avoid an inconvenient result - the same shape of
problem hard rule 3 forbids for benchmark scenarios ("never tune a
detector to pass a benchmark scenario... if it misses, it misses").
Real production baselines contain real holidays; a detector that's
never seen one in its baseline isn't more correct, just untested
against a real, recurring case. Consequence, stated explicitly: the
clean (no injected failure) replay may report real false positives on
these specific days, and that gets reported as a real, disclosed
number - not suppressed, not excluded after the fact to make the
published rate look better.

**Multi-month loader reads only `tpep_pickup_datetime`, not full
rows - and that's what makes the schema mismatch a non-issue.**
Confirmed (not assumed) that Oct/Nov/Dec 2024's TLC files have 19
columns, missing `cbd_congestion_fee` entirely (Jan 2025 has 20) -
NYC's Manhattan congestion pricing fee started January 2025, so this
is real and expected, not a data error. The volume detector needs
nothing but pickup timestamps to compute per-window row counts, so the
loader built for it (`ingestion/replay/load_pickup_timestamps.py`)
projects only that one column, which is schema-identical across all
four months - sidestepping the mismatch entirely rather than needing
to reconcile it (e.g. via a null-filled column). This loader is for
ground-truth/baseline construction only (V7 - independent of the
system under test); the production detector reads its baseline from
`weir_metrics` in Postgres, never from raw parquet.

**Data range: Oct 2024-Jan 2025 (~17.4 weeks), not the existing single
month.** One month (~4.3 weeks) is under the 8-week warmup threshold -
confirmed insufficient before writing any detector code, per this
project's own instruction to fix data sufficiency first rather than
shrink the threshold to fit what happened to already be downloaded.
~17.4 weeks gives 8 for warmup and ~9 remaining where the integration
test can demonstrate real scoring against a matured baseline, not a
detector that barely crosses the warmup line at the last window in the
dataset.

**Addendum, found building the loader: 10 rows across these four
files have clock-error pickup timestamps years off (2002, 2008,
2009) or into March 2025 - not legitimate month-boundary spillover.**
Confirmed via an independent pyarrow.compute filter in
`tests/integration/test_load_pickup_timestamps.py`, not eyeballed.
`load_pickup_timestamps.py` drops anything outside each requested
month's own span +/-7 days (generous relative to the ~22-row spillover
`#41` already found for a single month; nowhere near enough to admit
a timestamp that's years off) and reports the exact count and values
dropped - not silently. First version of the verifying test asserted
"9" from a manual check that used a looser bound (through Feb 15)
than the loader's actual margin_days=7 policy (through Feb 8); the
test's own independent check caught the mismatch and the expectation
was corrected to the real, verified number (10) - the test doing
exactly the job V7/V8 exist for.

**Consequence for the published false-positive rate: read it as an
upper bound, not a settled number.** With `min_baseline_weeks = 8`,
the MAD's effective independent sample size (8 distinct weekly
occurrences per bucket) is below the n>=20-30 robust-stats guidance
generally wants for a stable estimate - stated earlier in this entry,
repeated here because it has a direct, honest consequence for any
number this detector eventually publishes. A noisier-than-ideal MAD
means more scored windows land near the flagging threshold by chance,
which pushes a measured false-positive rate up, not down - so
whatever number the benchmark eventually reports for this detector is
more likely to overstate the true rate than understate it. A longer
baseline than this dataset's ~17 weeks would likely lower it further.
This same caveat is stated in README.md, next to where that number
will eventually appear, not buried here alone - an honest upper bound
is more credible than a clean number with no stated uncertainty.

## 45. The volume detector runs as its own process, state in Postgres
- not Flink keyed state, and why that's not just safer but necessary

Written before `reliability/volume/`'s schema or code, per D1.

**Decision: a separate Python process, not part of `metrics_job.sql`.
State (the warmed EWMA/MAD baseline) lives in Postgres, not Flink
keyed state.** Real alternative, stated before being rejected: Flink
keyed state is faster (no network round-trip per update) and is
exactly what the reference `ewma_ad.py` already does, just re-keyed by
(weekday, hour) instead of `device_id`. Rejected because:

- It dies with anything beyond a checkpoint-recoverable TaskManager
  failure - a deliberate redeploy, a savepoint mistake, a state-schema
  change - and losing it means losing 8+ weeks of accumulated history
  that Kafka retention almost certainly can't replay from scratch.
- It's not inspectable without Flink's own state-processor tooling;
  Postgres state answers "what does this bucket's baseline currently
  look like" with a plain `SELECT`.
- Every genuinely novel piece of Flink SQL/connector syntax tried this
  session has found a real bug on first contact (#33, #42 x2, #43) -
  putting a stateful anomaly detector's own logic inside another Flink
  job adds a fifth surface for that same tax, for no compensating
  benefit at a 1-window-per-minute update cadence where a few ms of
  Postgres round-trip is irrelevant.
- **The argument that actually settles it, not just favors it: the
  sensitivity sweep this project has already committed to (measuring
  how detection/false-positive rate vary with threshold and other
  config) needs to re-score the same historical data at many parameter
  settings.** That's only tractable if the baseline is warmed once and
  stored, then re-scored cheaply against different thresholds. Flink
  keyed state would mean a full 8-week rewarm per sweep point - with,
  say, 20 sweep points, that's 20 full baseline rebuilds instead of
  one. Postgres state isn't just the safer choice here; it's the only
  one that makes a deliverable already on this project's roadmap
  actually tractable.

**Dependency: `psycopg[binary]==3.3.4` (v3, not psycopg2) - approved,
per hard rule 6.** psycopg2 is in extended maintenance mode; psycopg 3
is the actively developed line, with better type support and native
async, and this project's other Python code already targets a modern
interpreter. `[binary]` pulls prebuilt wheels rather than requiring
libpq-dev/a C compiler on every machine this runs on (CI runners, this
project's Windows dev machine) - the tradeoff psycopg's own docs
describe as convenience-over-leanness, chosen deliberately given this
project has no existing build-toolchain requirement for Python code
today. Pinned to 3.3.4, the latest stable release at the time of this
decision (confirmed via PyPI's own JSON API, not assumed).
`reliability/volume/requirements.txt` is a new, directory-scoped
manifest - this project has no root-level Python requirements file yet
(every other Python dependency so far was installed ad hoc per CI
step); scoping it to the directory that needs it avoids implying a
project-wide dependency policy that doesn't exist yet.

**Idempotent scoring: `scored_windows`'s own UNIQUE constraint is the
correctness backstop; `detector_progress` is the fast resume-point.**
Every window that gets past `insufficient_baseline` is logged to
`weir_incidents.scored_windows`, keyed uniquely on `(detector_name,
window_start, window_end)`. Processing a window is one transaction:
update `baseline_state`'s EWMA for its bucket, insert the
`scored_windows` row, insert an `incidents` row if the score crosses
threshold, and advance `detector_progress.last_processed_window_end` -
all four writes commit together or none do. A crash mid-transaction
rolls back cleanly (that window simply gets reprocessed next run,
exactly once, since nothing partial was ever committed); a restart
resumes from `detector_progress` (an O(1) lookup, not a scan of
`scored_windows` for its max) but even if that value were somehow
wrong, `scored_windows`'s UNIQUE constraint would reject a genuine
double-processing attempt outright rather than silently double-
applying a window to the EWMA. Windows must be processed in
chronological order for the EWMA recursion to mean anything, so one
global watermark per `detector_name` is sufficient - a bucket's own
occurrences are a subset of the globally chronological sequence, so
processing globally in order processes each bucket's occurrences in
order too.

**Baseline state and scoring results are separable, on purpose - the
same fact that makes the sweep tractable, reflected in the schema.**
Three tables, not one: `baseline_state` (the warmed per-bucket EWMA/
MAD, updated once as live data streams in - the ONLY table a
sensitivity sweep never touches), `scored_windows` (an append-only log
of every scored window's inputs - `observed_value`,
`baseline_mean_at_time`, `baseline_scale_at_time` - decoupled from any
particular threshold decision, so a sweep recomputes `score`/flag at a
different threshold straight from these stored numbers, never
re-reading `weir_metrics` or re-running the EWMA recursion), and
`incidents` (only the rows that crossed the *currently configured*
threshold - the smaller, actionable table something like an alerting
system would consume). If baseline and scoring shared one table and
lifecycle, a sweep would force a rebuild every time; separating them
is what makes "warm once, re-score cheaply many times" real rather
than aspirational.

**Correction, made honestly before it became a false claim in a
docstring: this is an EWMA-recursive *approximation* of Iglewicz-
Hoaglin, not a literal implementation of it.** The true modified
z-score is defined against a sample's actual median and MAD (median
absolute deviation), computed from a static, sorted collection of
historical values - not something an O(1)-state exponential recursion
can produce, since neither median nor MAD is a linear function of its
inputs the way a mean is. What this detector actually tracks per
bucket is an exponentially weighted MEAN (`ewma_mean`) and an
exponentially weighted MEAN ABSOLUTE DEVIATION from that mean
(`ewma_mad`) - a standard, legitimate robust-ish streaming
approximation (used under names like "MEWMA" in some statistical-
process-control literature), but not the same statistic Iglewicz and
Hoaglin (1993) defined. Caught while designing the schema (deciding
what `ewma_mad` actually means precisely enough to write a column
comment), not after shipping a docstring that overclaimed.

**Follow-on correction: Iglewicz-Hoaglin's 0.6745 doesn't transfer to
a mean-absolute-deviation scale - reusing it anyway would make the
score tighter than its own label implies.** Confirmed independently
(not just asserted) that for a normal distribution, MAD (median-
based) ~= 0.6745*sigma while MeanAD (mean-based) ~= 0.7979*sigma =
sigma*sqrt(2/pi) - two different constants because MAD and MeanAD are
different statistics of the same distribution, not interchangeable
approximations of each other. `0.6745 / 0.7979 ~= 0.8455`, meaning
reusing 0.6745 as this detector's scale divisor, with `ewma_mad` (a
MeanAD, not a MAD) as the input, silently produces an *effective*
threshold around `3.5 * 0.8455 ~= 2.96` sigma-equivalents while the
code would still read "3.5" - tighter than the label states, which
means more false positives than the stated threshold implies, not
fewer.

**Decision: rescale by `1/0.7979 ~= 1.2533` (equivalently
`sqrt(pi/2)`) - the MeanAD-to-sigma constant, not MAD's - keeping the
threshold at 3.5.** Real alternative, stated before rejecting it: keep
0.6745/3.5 as empirically-tuned constants with no sigma-equivalent
claim at all, and let the sensitivity sweep (DEFENSE.md #44's honest-
upper-bound framing already anticipates this project publishing a
false-positive rate against this exact score) find a working threshold
empirically instead of asserting one. Rejected because this project is
about to publish a false-positive rate measured against this score -
an interpretable unit (a real number of sigma-equivalents) is worth
more here than bit-for-bit continuity with a reference algorithm this
detector already deliberately diverges from in three other ways (this
entry, above). The final formula: `z = (x - ewma_mean) /
max(1.2533 * ewma_mad, sqrt(ewma_mean), 1.0)`, flagged at `|z| > 3.5`
- the Poisson-noise floor (`sqrt(ewma_mean)`) is applied to the
already-rescaled, sigma-equivalent quantity, not to raw `ewma_mad`,
so the floor and the scale live in the same units throughout.

**Docstring requirement, stated plainly, not implied away:** this
detector computes an EWMA-of-mean-absolute-deviation, rescaled by the
MeanAD-to-sigma constant (`sqrt(pi/2) ~= 1.2533`) - not a true
median/MAD, and not Iglewicz-Hoaglin's own formula, even though the
3.5 threshold convention is reused from it. An exact-MAD variant would
require retaining a sorted historical sample per bucket instead of an
O(1) recursive update, which is precisely the state-size tradeoff this
detector's streaming design exists to avoid.

**Five schema corrections from review, before any migration runs:**

1. `incidents.baseline_median` -> `baseline_mean`. Leftover from the
   original MAD framing; this detector tracks an EWMA mean, never a
   median (the correction two sections above).

2. **Ordering: strict `window_end` ASC, with an explicit lag buffer
   and an explicit skip-and-report path - not just an intention.** The
   EWMA recursion is order-dependent - applying two windows' updates
   in the wrong relative order produces a different, wrong `ewma_mean`
   /`ewma_mad` than applying them correctly, unlike a commutative
   aggregate (a sum) where arrival order wouldn't matter. Two distinct
   lateness concerns, not one: (a) Kafka-to-window event-time
   lateness, already resolved by `metrics_job.sql`'s own watermark
   (270s bound, DEFENSE.md #40/#42) before a row ever reaches
   `weir_metrics.window_metrics` - a window that closes has, by
   construction, already absorbed everything the watermark was willing
   to wait for; (b) **write-order lateness at the Postgres-ingestion
   boundary** - nothing guarantees the JDBC sink's flush/commit order
   across different windows matches strict `window_end` order (sink
   batching, checkpoint timing, a restart re-emitting buffered writes).
   It's (b) this detector's own read loop has to defend against, not
   (a) - #40's p999 (11,309s) describes Kafka-arrival disorder that's
   already resolved by the time a row exists in `window_metrics`.
   Mechanism: each read cycle queries `window_metrics` rows not yet in
   `scored_windows`, computes `max_window_end_seen`, and only processes
   rows with `window_end <= max_window_end_seen - max_lag_seconds`
   (a new config value; default reasoned, not measured against #40's
   number, below), strictly in ascending `window_end` order, one
   transaction per window (per #45's existing idempotency design). A
   row that's still older than `detector_progress.last_processed_
   window_end` when it's finally read - i.e., it arrived so late even
   the lag buffer didn't hold long enough - gets a `scored_windows` row
   with `status = 'skipped_late'`, `score = NULL`, never applied to
   `baseline_state`'s EWMA. Counted and visible (V8), never silently
   dropped. `max_lag_seconds` default: a few minutes, reasoned from
   typical JDBC-sink-flush/checkpoint cadences (the write-order
   concern this buffer actually exists for), not from #40's Kafka-
   level p999 - that number belongs to a layer of the pipeline this
   detector never sees directly.

3. **`scored_windows.status IN ('scored', 'insufficient_baseline',
   'skipped_late')`, `score`/`baseline_mean_at_time`/
   `baseline_scale_at_time` all nullable, one row written in every
   case.** Without this, the 8-week warmup period (DEFENSE.md #44) is
   an accounting hole - windows that were seen but never scored would
   leave no trace at all, and a benchmark computed only from `scored`
   rows would silently undercount the denominator. Every window that
   clears the ordering/lag check gets exactly one `scored_windows` row,
   whichever of the three outcomes applies.

4. **`baseline_state.alpha` and `.config_hash`, checked on every
   update, failing loudly on mismatch.** `alpha` is the actual
   numeric EWMA decay derived from `half_life_weeks` (DEFENSE.md #44);
   `config_hash` is a hash of the full baseline-relevant config.
   Stored per-bucket-row (redundant across all 168 rows for one
   `detector_name`, not centralized) so any single row is self-
   describing in isolation and a partially-corrupted state is still
   individually checkable. If a config change (e.g. a different
   `half_life_weeks`) is deployed against an already-warmed baseline,
   the mismatch is detected and raised explicitly, rather than
   silently continuing to update a baseline that's now a blend of two
   different decay rates matching neither config.

5. **`TIMESTAMPTZ`, not naive `TIMESTAMP`, in every `weir_incidents`
   table - scoped to this new schema, not a retroactive fix to
   `weir_metrics`.** `weir_metrics.window_metrics` (Part 3.1, already
   5/5-verified) stores naive `TIMESTAMP` throughout and isn't being
   touched here - re-opening an already-shipped, verified component
   is a bigger, separate decision, not bundled into this one. Instead,
   the volume detector's own reading adapter is the one place that
   explicitly states and applies a timezone: `window_metrics.
   window_start` is interpreted as America/New_York local civil time
   (`AT TIME ZONE 'America/New_York'`, converting to a true UTC
   instant) *before* being stored as `TIMESTAMPTZ` in `weir_incidents`
   and before `bucket_weekday`/`bucket_hour` are derived from it.
   Bucketing in UTC instead was considered and rejected: the entire
   reason for (weekday, hour) bucketing is to capture genuine
   behavioral seasonality (rush hour, overnight lulls) that people
   observe in *local* civil time, not UTC - a UTC hour bucket would
   mix two different local hours across the EST/EDT boundary,
   blurring exactly the seasonality this detector exists to isolate.
   **The DST fall-back's ambiguous hour (DEFENSE.md #44) recurs here,
   at a second place in the pipeline**: `replay_producer.py` and
   `metrics_job.sql` don't exclude it the way `load_pickup_
   timestamps.py` does, so `window_metrics` still contains that hour's
   data with no recoverable true UTC offset. Rather than lean on
   Postgres's own (unverified, implementation-specific) default
   disambiguation for an ambiguous `AT TIME ZONE` conversion, the
   reading adapter applies #44's same policy a second time: those
   specific windows are excluded from `baseline_state` updates,
   explicitly, not guessed at. The inconsistency this exposes -
   `load_pickup_timestamps.py` excludes the ambiguous hour,
   `replay_producer.py`/`metrics_job.sql` don't - is a real gap,
   logged in docs/FUTURE_WORK.md rather than silently left
   unaddressed, and not fixed here since it touches an already-
   verified upstream component out of this task's scope.

**Four more corrections from a second review pass, applied to
`reliability/store/incidents_schema.sql` before any migration ran:**

1. `scored_windows_status_valid` CHECK, enforcing the three-value enum
   at the database level. Free-text `status` means a typo creates a
   silent fourth category and corrupts the V8 accounting it exists to
   support - documenting the three valid values in a comment isn't
   the same as making a fourth one impossible to write.

2. `scored_windows_null_contract` CHECK, made a real biconditional.
   The first draft only checked one direction (`status <> 'scored'`
   implies all three NULL) and would have silently allowed a `'scored'`
   row with a NULL `score` straight through - the exact gap this
   constraint exists to close. Fixed to require both directions:
   `'scored'` implies all three NOT NULL, anything else implies all
   three NULL.

3. Range CHECKs (`bucket_weekday BETWEEN 0 AND 6`,
   `bucket_hour BETWEEN 0 AND 23`) on every table that has these
   columns - `incidents`' version is NULL-aware since a future non-
   bucketed detector type may not populate them at all. Catches a
   timezone-conversion off-by-one (America/New_York vs UTC, DEFENSE.md
   #45) at write time, as a rejected INSERT, instead of as an
   inexplicable baseline discovered much later.

4. `detector_progress.last_processed_window_end` (and
   `baseline_state.last_window_end`) changed from nullable to a
   `NOT NULL DEFAULT`. Nullable was a real bug waiting to happen: SQL's
   three-valued logic makes `window_end <= NULL` evaluate to NULL, not
   a real answer either way, and the equivalent comparison in
   application code (against a Python `None`) would raise rather than
   silently misbehave - but either way, "what happens on a detector's
   very first window" would have been an unstated edge case discovered
   by accident, not a decision made on purpose. A real, ordinary
   comparable sentinel removes the ambiguity structurally: every
   genuine `window_end` is trivially greater than it, so "is this
   window behind the frontier" has exactly one correct, structural
   answer (no) on a brand-new detector - no separate NULL-handling
   branch needed anywhere in the comparison logic, and the default
   lives on the column itself, not in application code that could be
   called in an unexpected order.

   **First choice, `'-infinity'::timestamptz`, was wrong - caught by
   checking psycopg 3's own documentation before shipping it, not
   after.** Confirmed (not assumed): psycopg 3, unlike psycopg2,
   raises `DataError` by default when reading Postgres's
   `'infinity'`/`'-infinity'` timestamp special values back into
   Python, rather than mapping them to `datetime.min`/`max` the way
   psycopg2 did. Given #45 already approved psycopg 3 as this
   detector's Postgres client, the very first `SELECT` of this column
   would have crashed - a sentinel that breaks the one thing it exists
   to make safe. Switched to the Unix epoch
   (`'1970-01-01T00:00:00+00'::timestamptz`) instead: an ordinary,
   fully representable instant with no special-casing in any driver,
   and - since every real TLC `window_end` starts in Oct 2024 - just
   as reliably "less than any real window this system will ever see"
   as `-infinity` was meant to be.

**Two more real bugs, both caught while designing the detector core -
before either reached actual detector code:**

- **`scored_windows`'s own comment claimed `'insufficient_baseline'`
  windows don't update `baseline_state` - wrong, and self-
  contradictory.** If an under-warmed bucket's own windows never
  updated `baseline_state`, `observation_count` could never reach
  `min_observations` and the bucket could never warm up at all - a
  chicken-and-egg bug in the schema's own documentation, not the
  constraints. Corrected: every status except `'skipped_late'` updates
  `baseline_state` (the warmup check happens against the bucket's
  observation count *before* this window's own update is applied, so
  `'scored'` means "was already warm," not "became warm just now").
  `'skipped_late'` is the one status that never touches
  `baseline_state`.

- **The CI run that verified `scored_windows_status_valid` was
  actually testing the wrong constraint.** Confirmed via 5/5-style
  discipline applied to the CI output itself, not just its overall
  conclusion: the test's own success message named
  `scored_windows_null_contract` as the constraint that actually
  fired, not `scored_windows_status_valid` as claimed - the test row
  (`status='bogus_status'` with non-NULL `baseline_mean_at_time`/
  `baseline_scale_at_time`/`score`) violated both constraints at once,
  and Postgres reported whichever it evaluated first. Fixed two ways:
  the row now sets all three nullable columns to NULL (satisfying
  `null_contract`'s "not scored" branch on its own, isolating
  `status_valid` as the only constraint left to violate), and
  `expect_check_violation()` now takes an `expected_constraint`
  argument and fails if the wrong one fires - so a test that passes
  for the wrong reason can't happen silently a second time.

**A third bug, caught writing `adapter.py`: the DST fall-back's
ambiguous hour (DEFENSE.md #44/#45) had a documented exclusion policy
but no actual status to record it under.** `scored_windows.status`
only had three values, none of which fit "this window was seen but
deliberately excluded because its true UTC offset can't be
recovered" - it would have had to be misreported as some other status
or silently skipped outright, either of which is exactly the kind of
unaccounted-for record V8 exists to catch. Added a fourth status,
`'dst_ambiguous_excluded'`, checked in `adapter.process_window`
*before* the frontier lock is even taken (this exclusion doesn't
depend on arrival order, so it doesn't need the same serialization
the frontier check does) - real, if rare: `1/(7*24*60) ~= 0.01%` of
all windows this detector will ever see fall in this one hour per
year.

## 46. Part 4.2: `reliability/volume/run.py` - the read cycle, and where the naive-local-time boundary actually gets crossed

Written before `run.py`'s code, per D1.

**Confirmed, not assumed: `weir_metrics.window_metrics.window_start`/
`.window_end` are naive America/New_York civil time, not UTC.**
Checked directly against `ingestion/replay/load_pickup_timestamps.py`
and `ingestion/replay/replay_producer.py` - neither performs any
UTC conversion anywhere; NYC TLC's own `tpep_pickup_datetime` is
recorded in local wall-clock time by the source data and flows through
Kafka and `metrics_job.sql`'s `TUMBLE` unmodified. This matters because
`adapter.py`'s `process_window`/`to_local_bucket`/`is_dst_ambiguous`
all require a genuinely UTC-aware `window_end` (DEFENSE.md #45) -
`run.py` is the one place in this detector that performs the
naive-local -> aware-UTC conversion #45 point 5 already named but
didn't itself implement. Getting the direction of this conversion
backwards (treating the naive value as if it were already UTC) would
silently shift every bucket by the local UTC offset (4 or 5 hours)
without raising anything - exactly the kind of off-by-one #45's own
range CHECKs exist to catch at the *schema* level, but a wrong offset
that still lands in 0-23 wouldn't trip a CHECK at all, only a
comparison against real seasonality would ever surface it, and only
by accident.

**Mechanism: `naive_local.replace(tzinfo=ZoneInfo(timezone_name))`,
then `.astimezone(datetime.timezone.utc)` - not `.astimezone()`
alone.** `replace()` attaches a zone to the naive value without
shifting the clock reading (correct: the naive value already *is*
America/New_York wall-clock time); `astimezone()` alone on a naive
value would instead assume the *system* zone, making the result
depend on whatever machine happens to run this script. For the DST
fall-back's ambiguous hour, `replace()` + `astimezone()` still
produces *some* well-defined UTC instant (Python's default `fold=0`:
the earlier of the two valid offsets, i.e. still-EDT) even though
neither offset is verifiably the real one. Deliberately not resolved
more carefully here: `adapter.is_dst_ambiguous` re-derives the
NY-local naive value from whatever UTC instant it's handed and checks
it against the exact same `DST_FALLBACK_AMBIGUOUS_HOURS` table, so the
window is caught and excluded regardless of which of the two folds
this conversion happened to pick - the correctness of the exclusion
doesn't depend on `run.py` picking the "right" fold, only on the
round trip landing back in the same one-hour range, which it always
does for either fold.

**Alternative considered and rejected: push the conversion into the
SQL query itself** (`window_end AT TIME ZONE 'America/New_York' AT
TIME ZONE 'UTC'`), letting Postgres do it instead of Python. Rejected
because it would duplicate the same timezone semantics in two
different languages/libraries (Postgres's `AT TIME ZONE` vs Python's
`zoneinfo`) for no benefit at this detector's 1-window-per-minute
cadence (DEFENSE.md #45's own reasoning for why a network round trip
here is irrelevant) - one reviewable place for this logic
(`adapter.py`'s pure helpers, now joined by `run.py`'s
`to_utc_instant`) is worth more than a marginal query-side
optimization, especially given how easy this exact boundary is to get
backwards (previous paragraph).

**Read cycle: implements #45 point 2's already-specified design, not
a new one.** `detector_progress.last_processed_window_end` (a true
UTC `TIMESTAMPTZ`) is converted to NY-local-naive terms *once* per
run, then used directly against `window_metrics.window_end` (also
naive, also NY-local) as a same-type, same-semantics SQL comparison -
deliberately not a cross-timezone comparison in SQL, consistent with
the previous paragraph's decision to keep all timezone logic in
Python. Of the rows returned, `max_window_end_seen` is computed
(still in naive-local terms - a fixed-duration subtraction for the lag
buffer is insensitive to which naive representation it's done in,
except within the one DST hour this detector already excludes
separately), and only rows with `window_end <= max_window_end_seen -
max_lag_seconds` are processed, strictly ascending. This query-level
filter is a pure efficiency measure, not the correctness mechanism -
correctness is `process_window`'s own transactional frontier check
plus `scored_windows`' UNIQUE constraint (#45); a coarse or even
slightly-wrong filter here just means a row gets picked up on a later
run instead of this one, never processed incorrectly or twice.

**Not a daemon.** `run.py` is a single pass: process everything
currently eligible, then exit. Scheduling it repeatedly (cron, a
systemd timer, a long-running loop) is a deployment concern, deferred
along with the rest of Sprint 1's deployment scope - there's no
deployment target yet to schedule it against (docs/FUTURE_WORK.md).

## 47. `incidents/dev/volume_drop.py` - a direct-write dev trigger, not a pipeline-level fault injector

Written before the script's code, per D1.

**Decision: writes one synthetic row straight into
`weir_metrics.window_metrics`, never touches Kafka/Flink/the replay
producer.** Real alternative, stated before rejecting it: suppress a
fraction of `replay_producer.py`'s real emissions for a target window
so the drop flows through actual Flink aggregation. Rejected for this
tool specifically:

- Hard rule 2 requires `benchmarks/` never import from `incidents/dev/`
  - the two are structurally separate on purpose, and a dev trigger
  that requires the full Kafka/Flink stack blurs that boundary from
  the other side: it would need the same live infrastructure a real
  benchmark scenario needs, for a tool whose entire point is a fast
  local dev loop while building/debugging the detector itself.
- This project already has a live-pipeline-based real-data check on
  the roadmap (the real-data integration test, next). Building a
  second, separate mechanism to inject a fault *into* the live
  pipeline would duplicate that surface for no distinct benefit - the
  dev trigger's job is "give the detector one deliberately anomalous
  row to react to right now," not "prove the whole pipeline reacts to
  a fault correctly."
- Direct SQL is exactly this project's own established pattern for
  exercising Postgres-side logic in isolation:
  `scripts/verify_incidents_schema.py` inserts synthetic rows
  directly for the same reason - it's testing/exercising the
  consumer, not re-proving the producer.

**Deliberately reuses `run.py`'s normal read path unchanged - no
special-casing.** The injected row is picked up by the same
`fetch_eligible_windows` query as any real window; the detector has
no way to distinguish a dev-injected row from a real one, which is
the point - exercising the dev trigger also exercises the runner and
the adapter's transaction, not a separate code path that could drift
from what real data actually goes through.

**Target window defaults to the next minute after
`MAX(window_end)` currently in `window_metrics`** (or the current
UTC time, floored to the minute and converted to naive local, if the
table is empty) - not a fixed hardcoded timestamp. `run.py`'s
frontier only advances forward, so a fixed default would work exactly
once per fresh database and then silently fail every following
invocation (`window_end <= frontier` -> `skipped_late`, no incident
possible). Advancing off the real high-water mark means repeated
invocations during a dev session keep working without the caller
tracking state by hand. `--window-end` is available to override this
for a specific bucket.

**Prints the target bucket's current `baseline_state` (mean, MAD,
derived scale, warmup status) before writing, per V3** - not just
"row inserted." Without this, picking a `--row-count` that will
actually cross the detector's threshold (or deliberately won't) is
guesswork; the whole reason to run this tool by hand is to see the
detector react to a value you chose knowingly; against a MAD=0 or
still-warming bucket, the printed scale-floor components frequently
make the current row-count choice's likely outcome (or lack of one)
obvious *before* running `run.py` at all, matching this project's
"never invent a plausible-looking number" instinct at the tooling
level, not just in benchmark results (hard rule 1).

**`ON CONFLICT (window_start, window_end) DO UPDATE`, not a plain
`INSERT`.** Matches `adapter.py`'s own `baseline_state` upsert
pattern. A dev iterating on threshold/`row_count` choices for the
same target window (the common case - "try again with a smaller
drop") re-runs this script against the same window rather than being
forced to compute a fresh one every time; nothing about this table's
own correctness depends on inserts being append-only the way
`scored_windows` deliberately is (#45).

## 48. `scripts/verify_volume_detector.py` - the real-data integration check, and what it deliberately doesn't claim

Written before the script's code, per D1.

**Decision: a standalone `verify_*.py` script run by its own
dispatch-only workflow, not a `tests/integration/` pytest file.**
Real alternative, stated before rejecting it:
`tests/integration/test_load_pickup_timestamps.py`'s own pattern -
`pytest.mark.skipif` when the real infrastructure it needs isn't
present, so it's harmless if `pytest -q tests/` (`ci.yml`'s automatic
gate) picks it up. Rejected here specifically because that test's
skip condition is cheap to satisfy accidentally (a few real parquet
files sitting in `data/tlc/`) while this check's real dependency is
the *entire* Kafka+Postgres+Flink stack actually up and the metrics
job actually run against real data - `replay-verify.yml`'s own header
comment already names this exact tradeoff for a different check
("too slow for the normal per-push gate"). A standalone script that
only exists as a step inside its own `workflow_dispatch`-only
workflow has no path to accidentally executing on every push; a
pytest file with a skip condition does, if that condition is ever
accidentally satisfied in the shared CI environment (services left
running from an earlier job, a persisted volume). See docs/CI.md and
CLAUDE.md C6 for the two-tier model this keeps intact.

**Decision: reuses `scripts/verify_metrics_job.py` to populate real
data, rather than re-implementing replay+Flink orchestration a second
time.** That script already does the real work this check needs as a
prerequisite - applies `schema.sql` fresh, runs `replay_producer.py`
against a real downloaded month, runs `metrics_job.sql` via the SQL
client, confirms rows landed. Real alternative rejected: have this
new script drive Kafka/Flink itself. Rejected because it would
duplicate `verify_metrics_job.py`'s own orchestration for no benefit -
the same "one reviewable place" reasoning as #46's SQL-vs-Python
timezone decision. The new workflow simply runs `verify_metrics_job.py`
first, then this script second, against the same live stack.

**Decision: the real month is 2024-11, not 2025-01 (`metrics-job-
verify.yml`'s existing month).** Real alternative: reuse 2025-01,
already downloaded/exercised elsewhere, to avoid a second ~59MB
download. Rejected: 2024-11 is the one month containing the real DST
fall-back (Nov 3, 2024) - the only way to verify `dst_ambiguous_
excluded` (#47) against genuine data instead of only the synthetic
unit tests in `test_volume_adapter.py`/`test_volume_run.py`. A month
without that transition would let this check pass while a real
timezone-boundary regression (#46) went completely unexercised by
anything touching real data.

**Honest scope limit, stated plainly rather than implied away: one
real month cannot warm any bucket's baseline, and this check does not
claim it does.** `min_observations` = 480 (8 weeks x 60
windows/bucket-occurrence, DEFENSE.md #44); one month gives each
(weekday, hour) bucket roughly 4-5 occurrences x 60 = ~240-300 raw
updates - computed and asserted exactly from the real data actually
read, not estimated here. Every row this check processes is
therefore expected to be `insufficient_baseline`, `skipped_late`, or
`dst_ambiguous_excluded` - **never** `scored`, and the check asserts
this exact absence rather than silently not checking for it (hard
rule 3 is about not tuning a detector to pass a scenario; this is the
adjacent honesty requirement - not implying a check exercised
something it structurally cannot). Whether this detector actually
flags a real incident against real, fully-warmed data is
`benchmarks/`'s job once it exists (still `.gitkeep` only - not built
this session), not this integration check's - logged as a real,
unaddressed gap in docs/FUTURE_WORK.md rather than left implicit.

**What's actually verified, each computed independently of the
detector under test (V7), never unaccounted-for (V8):**

1. Every real `window_metrics` row for 2024-11 has exactly one
   `scored_windows` row - counted by a direct `COUNT(*)` comparison
   between the two tables for this detector, not by trusting `run.py`'s
   own printed count.
2. Every real row whose naive `window_end` falls in `[2024-11-03
   01:00, 02:00)` - independently selected straight from
   `window_metrics`, not via `DST_FALLBACK_AMBIGUOUS_HOURS` or
   `is_dst_ambiguous` - has `status = 'dst_ambiguous_excluded'` in
   `scored_windows`, and that count is greater than zero (a sanity
   check that real trips actually exist in that hour, matching
   `test_dst_ambiguous_hour_dropped_matches_independent_count`'s own
   discipline in `test_load_pickup_timestamps.py`).
3. Zero `scored_windows` rows for this detector have `status =
   'scored'` (the honest-scope-limit assertion above, made concrete).
4. Each touched bucket's `baseline_state.observation_count` equals the
   real count of non-`dst_ambiguous_excluded`,
   non-`skipped_late` `window_metrics` rows that fall in that bucket -
   independently aggregated straight from `window_metrics` by
   `(EXTRACT(DOW ...), EXTRACT(HOUR ...))` in local time, not by
   trusting `baseline_state`'s own running count.

## 49. `verify_metrics_job.py`'s ground truth assumed a fixed NYC TLC column set - `cbd_congestion_fee` doesn't exist before 2025-01

First real bug from this session's first actual dispatch against a live
stack (`volume-detector-verify.yml`, targeting 2024-11 for #48's real
DST coverage) - in already-shipped, previously 5/5-verified Part 3.1
code, not anything built this session. Confirmed, not assumed: `pq.
read_table(...).to_pylist()` raised `KeyError: 'cbd_congestion_fee'`
computing ground truth for a 2024-11 window - that file's own parquet
schema genuinely has no such column (NYC's congestion-pricing fee
started 2025-01-05), not merely null-valued. `NULL_COLS`/`STAT_COLS`
hardcoded it, along with every other column, as always-present -
correct for every month this had ever actually run against (only
2025-01, via `metrics-job-verify.yml`) but never exercised against an
earlier month until now.

**Decision: schema-aware - skip a column for every list it appears in
(`NULL_COLS`/`DISTINCT_COLS`/`NEGATIVE_COLS`/`STAT_COLS`) when it's
absent from `table.column_names`, not just the one list that happened
to crash first.** Two real alternatives, stated before rejecting them:

- Special-case `cbd_congestion_fee` only. Rejected: the same crash
  recurs for `Airport_fee` (added later than the earliest TLC months)
  or any future column NYC adds or removes - a narrow fix teaches
  nothing about the general shape of the problem, which is "this
  project's assumed column set isn't actually fixed across real
  months," not "this one column is special."
- Abandon 2024-11, reuse 2025-01 (already exercised, already has every
  column). Rejected: 2025-01 has no DST fall-back to exercise -
  reusing it would mean #48's whole reason for choosing 2024-11 (real
  DST coverage, not just the synthetic unit tests) goes unmet, trading
  away the actual point of this check to avoid fixing a bug the check
  itself exists to surface.

**The expected-count assertion (`69`) is now derived, not
hand-adjusted for this one case.** A first draft computed a corrected
constant by hand (`69 - 4` for this specific column); rejected before
committing it - that number would silently be wrong for a future
month missing a *different* column with a different list membership
(e.g. one only in `NULL_COLS`, contributing 1 fewer, not 4). Fixed to
accumulate `skipped_metric_count` inline, in the same loops that build
`expected` - the count-check can no longer drift from the computation
it's checking, because it's produced by the same code path, not a
parallel formula that has to be kept in sync by hand.

**A second, related bug caught reviewing this fix before committing
it, not after: the mismatch-reporting block was dropped entirely
mid-edit** (a stray replacement left `PASS: stage 8` printing
unconditionally, even with real mismatches in the list) - caught by
re-reading the diff immediately after making it, restored before any
commit or dispatch. Recorded per this project's own standard: an
error caught by re-checking one's own work is still worth writing
down, not just silently fixed.

**Suspected, not yet confirmed: `weir_metrics.fan_out_window_metrics_
wide()`'s trigger (`reliability/store/schema.sql`) may have the same
class of bug one layer deeper, in already-shipped production code this
fix does not touch.** The trigger's `INSERT ... SELECT ... FROM
(VALUES (...))` has no `WHERE metric_value IS NOT NULL` filter, and
`column_metrics.metric_value` is `NOT NULL`. `MIN`/`MAX`/`AVG` over an
entirely-null column (exactly what `cbd_congestion_fee` would be for
every row in a month where it's absent) return SQL `NULL`, which -
reasoned through here, not yet observed - would make the whole
multi-row `INSERT` fail the `NOT NULL` constraint and abort, taking
the JDBC sink's own write down with it for every window that month.
If real, this would mean `metrics_job.sql`'s Postgres sink cannot
process ANY month lacking any one of its hardcoded columns, not just
2024-11 - a real gap in already-verified Part 3.1 code, if confirmed.
Deliberately not fixed here without seeing it actually fail first
(this project's "confirmed, not assumed" discipline applied to my own
hypothesis, not just the code) - and because it touches a different,
more consequential piece of already-shipped code than this entry's own
scope.

**Addendum, caught by the very next real dispatch (after #50's trigger
fix landed): this entry's own first fix skipped `null_count` too
eagerly for an absent column, computing 65 expected metrics against a
real 66.** `null_count` (`COUNT(*) - COUNT(col)`), `distinct_count`
(`COUNT(DISTINCT col)`), and `negative_count` are COUNT-shaped and
always produce a real, non-NULL value - even `n` nulls out of `n` rows
is a valid count, never SQL `NULL` the way `MIN`/`MAX`/`AVG` over zero
non-null values is. `metrics_job.sql`'s Kafka source table declares
`cbd_congestion_fee` (nullable) regardless of whether any given
month's real JSON messages happen to contain that key, so a
genuinely-missing field and an explicitly-null one are indistinguishable
once Flink has typed the row - `null_count` gets written as a real
row (value `n`) either way, and only the three `STAT_COLS` entries
(`min`/`max`/`mean`) are ever actually absent. Fixed: `NULL_COLS`/
`DISTINCT_COLS`/`NEGATIVE_COLS` now always compute (via `dict.get`,
never `continue`-skipped for an absent column) - only `STAT_COLS`
skips, and only when its own `vals` list ends up empty, which now
correctly covers both "column absent" and "column present but
all-null this window" as the same case, matching `#50`'s trigger fix
exactly rather than a second, independently-derived rule.

## 50. Confirmed: `fan_out_window_metrics_wide()`'s trigger crashes the whole JDBC write when any hardcoded aggregate column is entirely null

#49's hypothesis, confirmed by dispatching `volume-detector-verify.yml`
a second time against real 2024-11 data, not left as reasoning alone.
Real error, not inferred:

```
weir-postgres | ERROR: null value in column "metric_value" of
  relation "column_metrics" violates not-null constraint
weir-flink-jobmanager | PL/pgSQL function weir_metrics.
  fan_out_window_metrics_wide() line 7 at SQL statement
  Caused by: java.lang.RuntimeException: Writing records to JDBC failed.
```

`MIN`/`MAX`/`AVG` over `cbd_congestion_fee` (entirely absent from
2024-11's real schema, #49) return SQL `NULL` for every window that
month. The trigger's `INSERT ... SELECT ... FROM (VALUES (...))` had
no filter excluding a `NULL` `metric_value` against a `NOT NULL`
target column - Postgres rejected the whole multi-row `INSERT`, which
took the JDBC sink's checkpoint down with it, repeatedly, for every
window in the month. This is why `verify_metrics_job.py`'s Stage 7
timed out waiting for the target window's row - it never landed at
all, not just with fewer columns than expected.

**Decision: `WHERE m.metric_value IS NOT NULL` on the trigger's
`INSERT ... SELECT`, mirroring #49's fix rather than introducing a
second convention.** A genuinely-uncomputable aggregate (no real
values in this window, for this column) is now simply not written -
absence of a `(window_start, window_end, column_name, metric_name)`
row becomes the documented signal "nothing to compute here," matching
exactly what `verify_metrics_job.py`'s ground truth now also expects
(no entry, not a placeholder value) for the same case.

**Alternative considered and rejected: make `column_metrics.
metric_value` nullable and insert the `NULL` as-is.** Rejected as a
bigger, farther-reaching change than this bug needs: every downstream
reader of `column_metrics` (the volume detector doesn't read this
table at all, but a future column-level detector might) would then
have to handle a NULL metric_value explicitly, everywhere, forever -
versus "the row for this column/window/metric might not exist," which
is a narrower, already-familiar shape (every reader of a SQL table
already has to handle "no matching row").

**This bug predates this session's own changes by definition - it is
in `reliability/store/schema.sql`'s trigger, Part 3.1, already
"5/5-verified" (DEFENSE.md #37) months ago, but only ever verified
against 2025-01, the one month with every hardcoded column present.**
Any earlier NYC TLC month - not just 2024-11 - would trigger the same
crash for whichever columns didn't exist yet in that month's real
schema (`Airport_fee`, `congestion_surcharge`, and `cbd_congestion_
fee` were each added to the real dataset at different points in NYC
TLC's own history, not all at once). This wasn't caught by the
original 5/5 verification because that discipline verifies "5 clean
runs of the same scenario," not "5 runs across different real
months" - a real gap in what "verified" meant for this component,
worth carrying forward: a fixed-schema assumption tested against only
one month's real data is a narrower guarantee than it may read as.

## 51. `run.py`'s lag buffer can never fully drain a finite, already-complete dataset - added `assume_no_more_arrivals`

Confirmed by real dispatch, not reasoned in advance this time: the
first real run of `verify_volume_detector.py` against 2024-11 (once
#49's import fix let it actually execute) reported `window_metrics has
76 real rows but scored_windows has 71` - 5 short, exactly
`max_lag_seconds` (300s = 5 one-minute windows).

**Root cause: `fetch_eligible_windows` re-derives "the max" from
whatever's currently in its own query result, which is correct for a
live stream but never converges for a finite, already-complete
batch.** Pass 1 (76 rows): `max_window_end_seen` = row 76's end;
filters to `window_end <= max - 300s`, processing rows 1-71 and
excluding 72-76 as "too recent to trust." Pass 2 (the remaining 5
rows, 72-76): `max_window_end_seen` is now row 76's end again (the max
of just this smaller batch) - all 5 remaining rows are, by
construction, within 300s of *that*, so all 5 get excluded again.
This repeats forever; no number of passes ever processes the true
tail of a static dataset, because each pass's own notion of "the
current tail" is relative to whatever's left, not to a real "no more
data is coming" fact the buffer has no way to know on its own.

This is not a bug against a live stream, where the buffer's whole job
is exactly this: never trust the most recent few minutes until more
time has actually passed and nothing arrived behind them. It's a real
gap for a bounded, already-loaded dataset, which is exactly what a
real-data integration check needs.

**Decision: `assume_no_more_arrivals=False` (default) on
`fetch_eligible_windows`/`run_once` - `True` skips the lag-buffer
filter entirely.** Real alternative, stated before rejecting it:
leave `run.py` untouched and adjust `verify_volume_detector.py`'s V7/V8
assertion to expect exactly `max_lag_seconds` worth of permanently-
unprocessed rows at the end of any finite batch, as a documented
consequence rather than a gap to close. Rejected: that would mean this
integration check can never actually confirm the real DST fall-back
hour's exclusion or the honest-scope-limit assertion for whichever
windows happen to fall in that permanently-stuck tail - a real
verification hole that just moves depending on the file's own row
count, not a stable, understood boundary. A caller-supplied flag,
default `False`, keeps live-polling's write-order protection exactly
as designed (#45) while giving a backfill/verification caller - who
genuinely knows no more writes are coming for this range - a correct
way to say so, rather than working around an accounting gap the
buffer itself can't detect.

`verify_volume_detector.py` now calls `run_once(..., assume_no_more_
arrivals=True)` once, not `run_once` twice hoping a second pass
catches the tail - the two-pass workaround never actually worked (the
math above), it just silently looked like it might.

**`assume_no_more_arrivals=True` must NEVER be used by the benchmark
runner, once it exists.** The benchmark measures detection latency
(README's own differentiator) - skipping the lag buffer means every
window gets scored the instant it appears, reporting batch-processing
speed, not streaming detection latency, which would make that number
meaningless without anyone having tuned or faked anything, just by
calling this flag from the wrong caller. This flag is for
verification/backfill against an already-complete file only. If the
benchmark ever hits this same non-convergent-tail symptom, the fix is
to append a synthetic trailing window past the frontier (giving the
buffer something real to clear against), never to disable the buffer
to make the symptom go away.

## 52. Freshness detector: arrival gap, not lateness - the lateness columns aren't populated

Decision: observed_value is the gap in seconds between a real window's
window_end and the previous real window's - not lateness_p50/p99/max_seconds.
Real alternative, rejected: those columns exist in window_metrics and are
named for exactly this, but metrics_job.sql never actually populates them
(schema.sql's own comment already says so) - every real row has them NULL.
Arrival gap is a different, better-fitting signal anyway: "is data still
arriving on schedule" is directly what window_end already answers, no
upstream work needed, and it's the more literal reading of "freshness" -
lateness is event-delay distribution, a different (also real, still
blocked) detector. Logged as a scoped-out future item in FUTURE_WORK.md
rather than left implicit.

## 53. Null-rate detector: one detector_name per monitored column, no schema change

Decision: detector_name = f"null_rate_{column_name}" (e.g. "null_rate_passenger_count"),
one instance per column, rather than adding a column_name dimension to
baseline_state/scored_windows' bucketing key. weir_incidents' tables are
already multi-tenant by detector_name; reusing that dimension for
"which column" needs zero schema changes and keeps every existing
constraint/query pattern working unchanged. Rejected: a real
column_name column on every table - more general in the abstract, but
no other detector needs it, and CLAUDE.md's own C2 argues against
carrying a dimension nothing uses yet.
