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
| `consumer.py` | Consume loop: Kafka messages -> aggregator -> sink, committing offsets only once the events they cover are in ClickHouse. `run_pipeline` is the provider-agnostic variant for any event-dict iterable. |
| `sink_clickhouse.py` | ClickHouse `ReplacingMergeTree` sink ordered by `(window_start, tenant, kind)`, the natural time-series layout; one row per window after merge. |

## Why this matters for hiring

Role categories unlocked: **Data Engineer**, Analytics Engineer, streaming/platform.

This is the Kafka + ClickHouse cousin of a DuckDB batch ETL: real streaming ingestion, event-time windowing with watermarks and late-event handling, and a columnar OLAP sink. It backs the "real-time data pipeline" resume line with a measured throughput number and a one-command live stack.

## How to run

Prerequisites: Python 3.10+; Docker (only for the live Kafka/ClickHouse/Grafana stack).

```bash
# unit tests (pure, no infra)
pip install -e ".[dev]"
pytest -q

# in-process aggregation benchmark (the 931K events/sec hero)
python -m benchmarks.throughput --events 2000000
```

Live stack (Redpanda + ClickHouse + Grafana + consumer):

```bash
export GRAFANA_ADMIN_PASSWORD='<choose one>'   # required; or set it in a .env file
docker compose up --build
# redpanda (Kafka) :9092, clickhouse :8123, grafana :3000, all on 127.0.0.1; consumer running

# in another shell, produce events:
pip install -e ".[stream]"
rta produce --bootstrap localhost:9092 --topic events --count 200000

# query the rollups:
curl 'http://localhost:8123/?query=SELECT tenant, sum(count) FROM rollups FINAL GROUP BY tenant FORMAT Pretty'
```

A window is written once the consumer has read events at least 5 s past its end (event time), so the newest windows of a batch appear after the next batch.

Grafana at http://localhost:3000 (log in as `admin` with that password; no anonymous access) auto-provisions a ClickHouse datasource (see [`grafana/README.md`](grafana/README.md)).

## The windowing model

- **Tumbling windows**: fixed-size, non-overlapping, keyed by `(window_start, tenant, kind)`.
- **Watermark**: the max event time seen. A window `[start, end)` closes once `watermark >= end + allowed_lateness`.
- **Late events**: an event for an already-closed window is dropped and counted (`dropped_late`), never silently merged.
- **Memory**: closed windows are evicted on close, so a multi-day stream holds only the currently-open windows in memory.
- **Delivery**: auto-commit is off, and a window is written only once the watermark closes it. After each ClickHouse write, and when the consumer stops, it commits offsets up to the earliest event still in an open window, so a crash or a restart re-reads everything ClickHouse does not have yet. `rta consume` stops after 10 s without messages and leaves the windows still open to its next run. On start it resumes after the latest window already in ClickHouse, so re-read events of written windows are dropped rather than re-emitted as partial rows. Net effect: each window is written once, with every event that was not late (`rta consume` prints how many were dropped as late). The watermark is global, so which events are late depends on how the partitions interleave, and a restart can change that interleaving. Assumes one consumer per topic.
- **Querying**: `rollups` is a `ReplacingMergeTree`, so a window that is ever written twice collapses to one row on merge; use `FINAL` to collapse at read time.

## Testing

```bash
pip install -e ".[dev]"
pytest                       # 29 unit tests + 1 integration (auto-skips without Docker)
pytest -m "not integration"  # unit only (what CI gates on)
```

- **test_windows.py**: window assignment, count/sum/min/max, per-key separation, cross-window split, watermark close timing, late-event drop, eviction bound, flush.
- **test_events.py**: schema round trip, type coercion, generator bounds + determinism.
- **test_pipeline.py**: sink record shape, ensure-schema, write/empty, full `run_pipeline` with fakes.
- **test_producer.py**: producer send/flush/determinism with a fake broker.
- **test_delivery.py**: `rta consume` against in-memory Kafka and ClickHouse fakes. Kills the consumer mid-stream and between a ClickHouse write and its offset commit, restarts it, and asserts the rows and committed offsets match an uninterrupted run exactly: nothing lost, nothing written twice. Runs on one partition with out-of-order events, and on two partitions with in-order events: a restart reads the two partitions in a different interleaving, which with out-of-order events changes which events are late. Also checks that a window still open when the consumer stops is written on a later run with every event, and that the committed offsets stop at the earliest event in an open window.
- **test_integration.py**: real Kafka + ClickHouse via testcontainers; asserts 2000 produced events sum back to 2000 rollup count. Skips automatically without Docker.

Coverage 93% (excludes the live integration path, which needs Docker).

## Integration test

The integration test stands up real Redpanda + ClickHouse containers, produces 2000 events, runs the consumer pipeline, and asserts the ClickHouse rollup counts reconcile to exactly 2000. It is marked `@pytest.mark.integration` and skips when Docker is absent. CI gates on the unit suite and builds the image; run the integration test locally with Docker for full end-to-end coverage.

## Project layout

```
src/rtanalytics/
  events.py            # schema + generator
  windows.py           # tumbling-window aggregator (the hot path)
  producer.py          # Kafka producer (lazy import)
  consumer.py          # consume loop + offset commits
  sink_clickhouse.py   # ClickHouse ReplacingMergeTree sink
  cli.py               # `rta produce` / `rta consume`
benchmarks/throughput.py  # in-process events/sec hero
grafana/                  # auto-provisioned ClickHouse datasource
docker-compose.yml        # redpanda + clickhouse + grafana + consumer
```

## Stack

Python 3.10+, pydantic, kafka-python, clickhouse-connect, Redpanda (Kafka API), ClickHouse, Grafana, testcontainers, pytest, Docker, GitHub Actions.

## License

MIT
