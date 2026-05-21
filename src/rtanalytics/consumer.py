"""Kafka consumer: reads events, feeds the aggregator, sinks rollups.

The consume loop is provider-agnostic: it takes any iterable of event dicts
and any object with a .write(rows) method, so it can be driven by a real
Kafka consumer in production or a plain list in tests.
"""
from __future__ import annotations

from collections.abc import Iterable

from rtanalytics.events import Event
from rtanalytics.sink_clickhouse import ClickHouseSink
from rtanalytics.windows import TumblingWindowAggregator


def run_pipeline(
    event_dicts: Iterable[dict],
    aggregator: TumblingWindowAggregator,
    sink: ClickHouseSink,
    *,
    flush_every: int = 1000,
) -> int:
    """Drive the aggregator from an event-dict stream, writing closed windows
    to the sink. Returns total rollup rows written.
    """
    written = 0
    pending = []
    seen = 0
    for d in event_dicts:
        event = Event.from_dict(d)
        closed = aggregator.add(event)
        if closed:
            pending.extend(closed)
        seen += 1
        if seen % flush_every == 0 and pending:
            written += sink.write(pending)
            pending.clear()

    pending.extend(aggregator.flush())
    if pending:
        written += sink.write(pending)
    return written


def make_consumer(bootstrap_servers: str, topic: str, group_id: str = "rta"):
    try:
        import json

        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError("kafka-python not installed. pip install .[stream]") from exc
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=10_000,
    )
