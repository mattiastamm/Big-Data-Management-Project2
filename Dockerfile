FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN pip install --no-cache-dir pyspark==4.1.0 kafka-python-ng pandas pyarrow

# Spark config applied to all sessions (packages, Iceberg catalog, S3, etc.)
ENV SPARK_CONF_DIR=/opt/spark-conf
COPY conf/spark-defaults.conf /opt/spark-conf/spark-defaults.conf

WORKDIR /opt/app
