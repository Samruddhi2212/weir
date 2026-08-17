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
# that process. NOT runtime-verified: no Docker available in this
# environment to actually build and boot this image.
#
# Known open risk: flink-sql-connector-kafka is pinned to its "-2.0"
# build (the newest confirmed to exist on Maven Central at write time).
# Whether Flink's Kafka connector requires an exact "-2.1" build to load
# cleanly against a 2.1.0 cluster, or whether "-2.0" is cross-compatible
# within the 2.x line, could not be confirmed via available search tools.
# Verify this actually loads before relying on it.

FROM flink:2.1.0-java21

ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0
ARG POSTGRES_JDBC_VERSION=42.7.13

USER root

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -o "iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/${ICEBERG_VERSION}/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o "postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

USER flink
