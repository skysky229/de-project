# bitnami/spark tags were retired from Docker Hub in 2025; bitnamilegacy/spark
# is Bitnami's free-tier successor. If this also stops working, switch to
# apache/spark:3.5.x and adapt the master/worker entrypoint accordingly.
# Pinned to exactly the pyspark version Airflow 2.9.3's own constraints file
# pins (see docker/airflow/requirements.txt) - driver/executor Java
# serialization breaks (serialVersionUID mismatch) if these ever drift apart.
FROM bitnamilegacy/spark:3.5.1

# Hadoop-AWS + AWS SDK so Spark can talk to MinIO (S3-compatible) via the s3a:// connector.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ClickHouse JDBC driver (the "all" classifier bundles its dependencies into
# one jar) so Spark can write to ClickHouse via .format("jdbc").
RUN curl -fsSL -o /opt/bitnami/spark/jars/clickhouse-jdbc-0.10.0-all.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.10.0/clickhouse-jdbc-0.10.0-all.jar
USER 1001
