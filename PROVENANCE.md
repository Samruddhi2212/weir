# Provenance

## Adapted from streaming-lakehouse-lab (MIT, 770189b05beca6f8a14e5fb6e45940ddc321847f, 2026-08-15)

- config/flink/config.yaml — checkpointing and state backend block, adapted
  from infra/flink/conf/flink-conf.yaml with modified checkpoint interval
  (see DEFENSE.md #1). Renamed from flink-conf.yaml to config.yaml after a
  runtime crash confirmed Flink 2.0+ no longer reads the legacy filename at
  all (FLIP-366) - not just a naming preference.
- docker/flink/Dockerfile — Flink core version (2.1.0) and Iceberg version
  choice informed by build.gradle.kts's `flinkVersion`/`icebergVersion`
  pins, but re-verified against Maven Central rather than copied as-is:
  Iceberg moved 1.10.0 -> 1.11.0 (currently latest, confirmed to exist).
  The actual mechanism - baking connector jars into a custom image so the
  cluster's classpath has Iceberg/Kafka-SQL/Postgres-JDBC available for
  ad-hoc SQL Client/Gateway use - does not exist in reference at all;
  reference only solves job-level Gradle compile dependencies (DataStream
  API), a different problem. The Kafka SQL connector and Postgres JDBC
  driver versions are independently sourced, not from reference.
  Part 3.1 (DEFENSE.md #42/#43) added two more independently-sourced
  jars for `'connector'='jdbc'`: `flink-connector-jdbc-core` and
  `flink-connector-jdbc-postgres`, both `4.0.0-2.0` - two thin jars
  instead of one uber jar like Kafka's, because no
  `flink-sql-connector-jdbc` uber-jar equivalent exists on Maven
  Central for any version (checked, not assumed) - Flink 4.x split the
  JDBC connector into a generic core module and a per-database dialect
  module instead. `4.0.0-2.0` is also the *only* JDBC connector build
  that exists at all (three versions total on Maven Central), and its
  own `pom.xml` (checked at both the `v4.0.0` and `v4.1.0` git tags)
  declares `<flink.version>2.0.0</flink.version>` - built against Flink
  2.0.0, not this image's 2.1.0. Same-minor-line compatibility, not a
  confirmed exact match; see DEFENSE.md #43.

## Copied from upstream Apache Flink (Apache-2.0)

- config/flink/log4j-console.properties — verbatim copy of Flink 2.1's
  own default `flink-dist/src/main/flink-bin/conf/log4j-console.properties`
  (fetched from `apache/flink`'s `release-2.1` branch, not modified).
  Needed because `docker-compose.yml`'s `./config/flink:/opt/flink/conf`
  bind mount replaces the image's *entire* conf directory, including
  this file - without it, log4j falls back to essentially no console
  output at all (`main ERROR Reconfiguration failed: No configuration
  found for '<hash>' at 'null' in 'null'` on every container start,
  confirmed the hard way across several real CI runs - see DEFENSE.md
  #32 (also ported to main as its own entry, #26). License header
  preserved as-is in the copied file.
- config/flink/config.yaml's `env.java.opts.all` line — verbatim copy of
  the same key/value from Flink 2.1's own default `flink-dist/src/main/
  resources/config.yaml` (`apache/flink`, `release-2.1` branch), not
  reconstructed or abbreviated. Same root cause as the entry above:
  the conf bind mount replaces Flink's own config.yaml too, and this
  project's file (adapted from a pre-Java21 reference) never carried
  these Java 17+ `--add-opens`/`--add-exports` defaults forward, which
  surfaced as a real checkpoint failure (Kryo reflecting into
  `java.nio.ByteBuffer` under Java 21's module system) - see DEFENSE.md
  #35.

## Read as reference, written independently

- StatefulDedupJob.java — stateful pattern reference
- ewma_ad.py — EWMA algorithm reference; Weir's implementation written from
  the statistical definition, not adapted

## Design references (read, not copied)

- OpenLineage event spec — Weir's lineage event shape is modeled on it, not
  adapted from its code
- AWS Deequ anomaly detection module — read as a baseline strategy
  comparison (see DEFENSE.md #4)

## Written from scratch

- Everything else

## Note

- Upstream README states Apache-2.0; upstream LICENSE file is MIT. Relying on
  the LICENSE file. See THIRD_PARTY_NOTICES.md for the reproduced MIT notice
  covering the adapted file above.
