FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY README.md ./
RUN pip install --no-cache-dir ".[stream]" \
    && useradd --create-home --uid 10001 app
USER app

# Default: run the consumer (override the command to `produce`).
ENTRYPOINT ["rta"]
CMD ["consume", "--bootstrap", "redpanda:9092", "--topic", "events", "--clickhouse", "clickhouse"]
