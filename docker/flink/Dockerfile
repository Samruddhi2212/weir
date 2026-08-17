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

FROM flink:2.1.0-java21

ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG HADOOP_VERSION=3.4.3

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
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    curl -fSL -o "hadoop-client-api-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    curl -fSL -o "hadoop-client-runtime-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"

USER flink
