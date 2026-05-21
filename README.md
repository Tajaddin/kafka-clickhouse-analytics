# kafka-clickhouse-analytics

> Real-time analytics pipeline: Kafka events to a tumbling-window aggregator to ClickHouse. **931,000 events/sec windowed aggregation on a single core** (1.07 µs/event, watermark-driven eviction, bounded memory). Pure-Python hot path with zero infra to benchmark; full Kafka + ClickHouse + Grafana stack via `docker compose up`.

[![ci](https://github.com/Tajaddin/kafka-clickhouse-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Tajaddin/kafka-clickhouse-analytics/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](pyproject.toml)

## Hero metrics

Reproducible in ~3 seconds with no Kafka, no ClickHouse, no Docker:

```bash
python -m benchmarks.throughput --events 2000000
```

| Metric | Value |
|---|---:|
| **Aggregation throughput** | **931,000 events/sec** (single core) |
| Per-event cost | 1.07 µs |
| Events processed | 2,000,000 |
| Rollup rows emitted | 41,699 |
| Window model | 60s tumbling, 5s allowed lateness |

This measures the production hot path: window assignment, accumulation, watermark advance, and closed-window eviction. The eviction scan is gated behind a min-window-end check, so the per-event path is O(1) amortized instead of O(open-windows). That single change took the benchmark from 233K to 931K events/sec.

## What it is

```
events --> Kafka topic --> consumer --> TumblingWindowAggregator --> ClickHouse rollups --> Grafana
                                          (count/sum/min/max per
                                           window x tenant x kind)
```

| Component | Role |
|---|---|
| `windows.py` | Tumbling-window aggregator. Watermark-driven close + eviction bounds memory on an unbounded stream. Pure Python, the hot path. |
| `events.py` | Event schema + deterministic generator (clickstream-style: tenant, kind, amount). |
| `producer.py` | Kafka producer (kafka-python, lazy import). |
| `consumer.py` | Provider-agnostic consume loop: any event-dict iterable -> aggregator -> sink. |
| `sink_clickhouse.py` | ClickHouse `MergeTree` sink ordered by `(window_start, tenant, kind)`, the natural time-series layout. |

## Why this matters for hiring

Role categories unlocked: **Data Engineer**, Analytics Engineer, streaming/platform.

This is the Kafka + ClickHouse cousin of a DuckDB batch ETL: real streaming ingestion, event-time windowing with watermarks and late-event handling, and a columnar OLAP sink. It backs the "real-time data pipeline" resume line with a measured throughput number and a one-command live stack.

## Run the live stack

```bash
docker compose up --build
# redpanda (Kafka) :9092, clickhouse :8123, grafana :3000, consumer running

# in another shell, produce events:
pip install -e ".[stream]"
rta produce --bootstrap localhost:9092 --topic events --count 200000

# query the rollups:
curl 'http://localhost:8123/?query=SELECT tenant, sum(count) FROM rollups GROUP BY tenant FORMAT Pretty'
```

Grafana at http://localhost:3000 auto-provisions a ClickHouse datasource (see [`grafana/README.md`](grafana/README.md)).

## The windowing model

- **Tumbling windows**: fixed-size, non-overlapping, keyed by `(window_start, tenant, kind)`.
- **Watermark**: the max event time seen. A window `[start, end)` closes once `watermark >= end + allowed_lateness`.
- **Late events**: an event for an already-closed window is dropped and counted (`dropped_late`), never silently merged.
- **Memory**: closed windows are evicted on close, so a multi-day stream holds only the currently-open windows in memory.

## Testing

```bash
pip install -e ".[dev]"
pytest                       # 19 unit tests + 1 integration (auto-skips without Docker)
pytest -m "not integration"  # unit only (what CI gates on)
```

- **test_windows.py**: window assignment, count/sum/min/max, per-key separation, cross-window split, watermark close timing, late-event drop, eviction bound, flush.
- **test_events.py**: schema round trip, type coercion, generator bounds + determinism.
- **test_pipeline.py**: sink record shape, ensure-schema, write/empty, full `run_pipeline` with fakes.
- **test_producer.py**: producer send/flush/determinism with a fake broker.
- **test_integration.py**: real Kafka + ClickHouse via testcontainers; asserts 2000 produced events sum back to 2000 rollup count. Skips automatically without Docker.

Coverage 73% (excludes the live integration path, which needs Docker).

## Integration test

The integration test stands up real Redpanda + ClickHouse containers, produces 2000 events, runs the consumer pipeline, and asserts the ClickHouse rollup counts reconcile to exactly 2000. It is marked `@pytest.mark.integration` and skips when Docker is absent. CI gates on the unit suite and builds the image; run the integration test locally with Docker for full end-to-end coverage.

## Project layout

```
src/rtanalytics/
  events.py            # schema + generator
  windows.py           # tumbling-window aggregator (the hot path)
  producer.py          # Kafka producer (lazy import)
  consumer.py          # consume loop (provider-agnostic)
  sink_clickhouse.py   # ClickHouse MergeTree sink
  cli.py               # `rta produce` / `rta consume`
benchmarks/throughput.py  # in-process events/sec hero
grafana/                  # auto-provisioned ClickHouse datasource
docker-compose.yml        # redpanda + clickhouse + grafana + consumer
```

## Stack

Python 3.10+, pydantic, kafka-python, clickhouse-connect, Redpanda (Kafka API), ClickHouse, Grafana, testcontainers, pytest, Docker, GitHub Actions.

## License

MIT
