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
  #26). License header preserved as-is in the copied file.

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
