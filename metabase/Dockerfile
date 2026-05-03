FROM eclipse-temurin:21-jre-jammy

ARG METABASE_VERSION=0.48.0
ARG METABASE_DUCKDB_DRIVER_VERSION=0.2.4

RUN groupadd -r metabase && useradd -r -g metabase metabase

RUN apt-get update && apt-get install -y \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app && \
    chown -R metabase:metabase /app

WORKDIR /app

ADD https://downloads.metabase.com/v${METABASE_VERSION}/metabase.jar /app/metabase.jar

RUN chown metabase:metabase /app/metabase.jar && \
    chmod 755 /app/metabase.jar

ENV MB_PLUGINS_DIR=/plugins

EXPOSE 3000

USER metabase

CMD ["java", "-jar", "/app/metabase.jar"]
