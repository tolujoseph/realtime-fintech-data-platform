FROM apache/airflow:3.0.6

USER root

RUN apt-get update \
    && apt-get install -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir pyspark==4.0.1