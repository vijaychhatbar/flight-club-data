FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

COPY consumer.py .

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

RUN mkdir -p /data && chown -R appuser:appuser /data

USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD pgrep -f consumer.py || exit 1

CMD ["python", "-u", "consumer.py"]
