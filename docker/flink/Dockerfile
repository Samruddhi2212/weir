# ============================================================
# Flink + Iceberg + Kafka SQL + Postgres-JDBC connector jars, baked in
# at build time.
# ============================================================
# This solves the classpath problem reference/ never actually solved:
# reference's build.gradle.kts pins Flink/Iceberg versions for compiling
# a job's own fat jar (DataStream API), but nothing in reference gets
# Iceberg/Kafka-SQL/Postgres-JDBC connector jars onto the Flink cluster's
# own classpath for ad-hoc SQL Client / SQL Gateway use. This Dockerfile
# does that. See PROVENANCE.md.
#
# Versions re-verified against Maven Central at write time (2026-08-17),
# not copied blindly from reference - Iceberg moved 1.10.0 -> 1.11.0 in
# that process.
#
# flink-sql-connector-kafka's "-2.0" build (vs. an unconfirmed "-2.1")
# was flagged here as a known risk. It turned out fine - smoke test step
# 4 (Flink reads Kafka, no Iceberg) passes clean in CI, no classpath
# errors. The dependency that actually broke was one I'd assumed away
# entirely: see DEFENSE.md #11.
#
# Iceberg's FlinkCatalogFactory.createCatalog() calls clusterHadoopConf()
# unconditionally (confirmed from Iceberg 1.11.0's own source), regardless
# of catalog-type or io-impl - S3FileIO does not avoid this. hadoop-
# client-api/-runtime are Hadoop 3.x's shaded client artifacts, added
# purely to supply org.apache.hadoop.conf.Configuration without pulling
# in unshaded hadoop-common's full transitive dependency chain. Pinned to
# 3.4.3, not Maven Central's newest (3.5.0) - 3.4.3 is what Iceberg
# 1.11.0 itself is built against (gradle/libs.versions.toml: hadoop3 =
# "3.4.3"), verified rather than assumed.
#
# Base image pinned to tag@digest, not just the tag - a tag is mutable
# (the same tag can be repushed with different content later), which
# left open a real, unresolved hypothesis for a step-5 inconsistency
# between two otherwise-identical CI runs. Digest resolved from Docker
# Hub's registry API on 2026-08-17 (see DEFENSE.md #15).
#
# /tmp/flink-checkpoints and /tmp/flink-savepoints are pre-created here,
# owned by flink:flink, before docker-compose.yml's named volumes ever
# mount over them - Docker copies whatever a mount point already
# contains (including ownership) into a fresh named volume on first
# mount. Without this, the mount point gets created at container start
# owned by root, and the non-root `flink` user this image runs as
# (see DEFENSE.md #10 - same class of bug, this time on a directory
# instead of a copied-in file) can't write into it, which only ever
# surfaces once a job actually tries to checkpoint - see DEFENSE.md #30
# (also ported to main as its own entry, #25).

FROM flink:2.1.0-java21@sha256:11d31310a00763e0927325c6fcfe65961c9170c05ce421b9458b923004e95895

ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG HADOOP_VERSION=3.4.3
# Part 3.1's metrics job needs 'connector'='jdbc'. Unlike Kafka, the JDBC
# connector has no flink-sql-connector-jdbc uber jar (confirmed via Maven
# Central's search API - no such artifact exists); Flink 4.x split it
# into a thin core jar plus a per-database dialect jar, so both are
# needed alongside the postgresql driver jar above. See DEFENSE.md #43
# for the version-compatibility caveat (this "-2.0" build targets Flink
# 2.0.0 specifically, confirmed from its own pom.xml - not 2.1.0, which
# is the line this image is actually on) and PROVENANCE.md for why two
# jars instead of one.
ARG JDBC_CONNECTOR_VERSION=4.0.0-2.0

USER root

RUN mkdir -p /tmp/flink-checkpoints /tmp/flink-savepoints && \
    chown -R flink:flink /tmp/flink-checkpoints /tmp/flink-savepoints

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -o "iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/${ICEBERG_VERSION}/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o "postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    curl -fSL -o "flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o "flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o "hadoop-client-api-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    curl -fSL -o "hadoop-client-runtime-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"

USER flink
