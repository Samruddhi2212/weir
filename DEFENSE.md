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
