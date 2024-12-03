# Use the Astronomer base image
FROM quay.io/astronomer/astro-runtime:12.3.0

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
# ENV SPARK_HOME=/usr/local
# ENV PATH=$SPARK_HOME/bin:$PATH
COPY data-mock /data
USER airflow
