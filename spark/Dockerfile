FROM quay.io/curl/curl-base:latest as DOWNLOADER
ARG DELTA_CORE_VERSION=2.12-2.4.0
RUN curl https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_${DELTA_CORE_VERSION}.jar --output /tmp/delta-core_${DELTA_CORE_VERSION}.jar

# build spark and copy some installed library from DOWNLOADER , ARG is unreachable from DOWNLOADER
FROM bitnami/spark:3.4.0
ARG DELTA_CORE_VERSION=2.12-2.4.0 
USER 1001
COPY --from=DOWNLOADER /tmp/delta-core_${DELTA_CORE_VERSION}.jar /opt/bitnami/spark/jars/delta-core_${DELTA_CORE_VERSION}.jar