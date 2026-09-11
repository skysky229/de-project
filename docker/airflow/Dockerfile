FROM apache/airflow:2.9.3-python3.11

# spark-submit is a JVM process even when just submitting to a remote master,
# so the Airflow image needs a local JDK (it has none by default).
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt /requirements.txt
# Install against Airflow's own constraints file so extra packages can't drag
# in an incompatible Airflow version transitively (providers-amazon >=9 pulls
# in Airflow 3.x otherwise, which breaks this image's Airflow 2.9.3 CLI).
RUN pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# spark-submit in client mode runs the driver here, so this needs the same
# hadoop-aws / aws-sdk jars as the Spark image (docker/spark/Dockerfile) for
# s3a:// to work. Versions must match pyspark's bundled hadoop-client-api jar.
RUN PYSPARK_JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -fsSL -o "$PYSPARK_JARS/hadoop-aws-3.3.4.jar" \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o "$PYSPARK_JARS/aws-java-sdk-bundle-1.12.262.jar" \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o "$PYSPARK_JARS/clickhouse-jdbc-0.10.0-all.jar" \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.10.0/clickhouse-jdbc-0.10.0-all.jar
