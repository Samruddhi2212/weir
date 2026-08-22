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

## 25. A latent infrastructure bug in the base image, only reachable once a job actually checkpoints

Found on the `exactly-once-validation` branch, while building step 6's
continuous Kafka -> Iceberg validation job - but the bug itself lives
here, in `docker/flink/Dockerfile`, and would hit any streaming job on
`main` too, not just that branch's work. Ported here in its own commit
rather than left stranded on a feature branch.

First submission of a genuinely continuous streaming job (unlike
`smoke_step4.sql`/`smoke_step5.sql`, both bounded/batch) failed before
the job even started running:

```
Caused by: java.io.IOException: Failed to create directory for shared state: file:/tmp/flink-checkpoints/<job-id>/shared
	at org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess.initializeBaseLocationsForCheckpoint(...)
	at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.<init>(...)
```

Root cause: `docker/flink/Dockerfile` never creates `/tmp/flink-
checkpoints` or `/tmp/flink-savepoints` - those paths have only ever
existed via `docker-compose.yml`'s named volumes, mounted over a path
the image itself has nothing at. A fresh named volume with no matching
content already in the image gets created at first mount owned by
root, not by the non-root `flink` user (uid 9999, confirmed from
`apache/flink-docker`'s own Dockerfile) this image runs as. Same root
cause as DEFENSE.md #10 - a non-root user meeting something it can't
write to - but on a directory Docker creates at mount time, not a file
this project copies in.

**Why this was never caught before, honestly:** `execution.checkpointing.
interval: 60s` (DEFENSE.md #1) applies cluster-wide regardless of job
type, but batch execution doesn't enable checkpointing the way a
continuous streaming job does - nothing submitted on `main` before this
had ever actually asked the JobManager to construct checkpoint storage.
The permission problem was latent since `docker-compose.yml` first added
those volumes, reachable by any streaming job, just never triggered
because none had been submitted yet.

**Fixed** in `docker/flink/Dockerfile`, before `USER flink`: `mkdir -p
/tmp/flink-checkpoints /tmp/flink-savepoints && chown -R flink:flink
...`. Docker's documented behavior for named volumes - content (and
ownership) already present in the image at a mount point gets copied
into a freshly created volume on first mount - means this is enough; no
entrypoint wrapper, no runtime chown, no change to `docker-compose.yml`
at all.

## 26. Flink's console logging has been silently broken since the conf mount was added

Found on `exactly-once-validation` while trying (for the fifth time) to
capture useful JobManager/TaskManager logs to diagnose an unrelated bug
(a job reaching `FINISHED` with zero records read) - but the root cause
lives here, on `main`, since before that branch existed.

Every prior diagnostic attempt assumed detailed Flink logging existed
*somewhere* and just hadn't been found yet. It didn't exist at all:
`GET /jobmanager/logs` (Flink's own REST API) returned `{"logs":[]}`; a
filesystem-wide `find` turned up nothing Flink-related, only OS
package-manager logs; and the *entire* `docker compose logs` output for
both containers across their whole lifetime - not a truncated tail,
everything either container had ever printed - was two `Permission
denied` lines (already known, DEFENSE.md #13), a "Starting X
Manager"/"console application" pair, and two `main ERROR
Reconfiguration failed: No configuration found for '<hash>' at 'null'
in 'null'` lines. That last one is log4j2's own bootstrap-failure
message - checked rather than dismissed as more noise.

Root cause: `docker-compose.yml`'s `./config/flink:/opt/flink/conf`
bind mount **replaces** the image's entire `conf/` directory rather
than overlaying it. `config/flink/` has only ever contained
`config.yaml` (added in DEFENSE.md #13) - meaning the image's own
default `log4j-console.properties`, the file Flink's own logging docs
name specifically for "Job-/TaskManagers run in the foreground" (exactly
what "Starting standalonesession as a console application" describes),
has been absent from both containers since that mount was first added.
Log4j2 falls back to near-zero output instead of the INFO-level console
logging (and rolling file appender) that file actually configures.

**Why this was never caught on `main`:** nothing here has ever needed
to read detailed Flink logs to debug a failure - `smoke_test.sh` steps
4/5's own assertions (DEFENSE.md #14) check exit codes and sentinel
markers in SQL output, never Flink's own log content. It took a bug
that specifically required reading JobManager logs to notice logging
itself was broken.

Fixed with `config/flink/log4j-console.properties` - a verbatim,
unmodified copy of Flink 2.1's own default file (`apache/flink`,
`release-2.1` branch), not a from-scratch or partial reconstruction -
so the bind mount carries a complete conf directory instead of a
partial one. Recorded in PROVENANCE.md/THIRD_PARTY_NOTICES.md as
copied from upstream Apache Flink (Apache-2.0), a new provenance
category distinct from `reference/`.
