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
