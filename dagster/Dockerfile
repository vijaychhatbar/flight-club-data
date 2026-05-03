FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

RUN mkdir -p /opt/dagster/dagster_home

COPY config/dagster.yaml /opt/dagster/dagster_home/dagster.yaml
COPY workspace.yaml /opt/dagster/dagster_home/workspace.yaml
COPY aviation_pipeline/ /opt/dagster/app/aviation_pipeline/
COPY dbt/ /opt/dagster/app/dbt/
RUN cd /opt/dagster/app/dbt && dbt parse --profiles-dir .

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1

EXPOSE 3000 4000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
