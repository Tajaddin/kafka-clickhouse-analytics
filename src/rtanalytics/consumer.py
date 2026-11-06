"""Kafka consumer: reads events, feeds the aggregator, sinks rollups.

`run_pipeline` is provider-agnostic: it takes any iterable of event dicts
and any object with a .write(rows) method, so it can be driven by a plain list
in tests. `consume` is the live loop: it also tracks Kafka offsets and commits
them only once the events they cover are in the sink.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable

from rtanalytics.events import Event
from rtanalytics.sink_clickhouse import ClickHouseSink
from rtanalytics.windows import RollupRow, TumblingWindowAggregator


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


def consume(
    messages: Iterable,
    aggregator: TumblingWindowAggregator,
    sink: ClickHouseSink,
    commit: Callable[[dict[int, int]], None],
    *,
    flush_every: int = 1000,
) -> int:
    """At-least-once consume loop that never writes a window twice.

    ``messages`` yields records with ``.partition``, ``.offset`` and ``.value``
    (an event dict). After each sink write, ``commit`` receives
    ``{partition: next_offset}`` that stops at the earliest event still held in
    an open window, so after a crash every event the sink lacks is read again.
    On start the aggregator resumes after the last window the sink holds, so
    re-read events of windows already written are dropped as late instead of
    being re-emitted as partial rows. Assumes one consumer per topic.
    Returns total rollup rows written.
    """
    resume = sink.max_window_end()
    if resume is not None:
        aggregator.resume_after(resume)

    # Per open window: the first offset it took from each partition.
    first_offsets: dict[tuple[int, str, str], dict[int, int]] = {}
    next_offsets: dict[int, int] = {}
    pending: list[RollupRow] = []
    written = 0

    def write_and_commit() -> None:
        nonlocal written
        written += sink.write(pending)
        for row in pending:
            del first_offsets[(row.window_start_ms, row.tenant, row.kind)]
        pending.clear()
        safe = dict(next_offsets)
        for offsets in first_offsets.values():
            for partition, offset in offsets.items():
                safe[partition] = min(safe[partition], offset)
        commit(safe)

    for seen, msg in enumerate(messages, 1):
        event = Event.from_dict(msg.value)
        dropped = aggregator.dropped_late
        pending.extend(aggregator.add(event))
        if aggregator.dropped_late == dropped:
            key = (aggregator.window_start(event.event_time_ms), event.tenant, event.kind)
            first_offsets.setdefault(key, {}).setdefault(msg.partition, msg.offset)
        next_offsets[msg.partition] = msg.offset + 1
        if seen % flush_every == 0 and pending:
            write_and_commit()

    pending.extend(aggregator.flush())
    if next_offsets:
        write_and_commit()
    return written


def kafka_commit(consumer, topic: str) -> Callable[[dict[int, int]], None]:
    """`consume` commit callback for a kafka-python consumer."""
    from kafka.structs import OffsetAndMetadata, TopicPartition

    def commit(offsets: dict[int, int]) -> None:
        consumer.commit(offsets={
            TopicPartition(topic, partition): OffsetAndMetadata(offset, "", -1)
            for partition, offset in offsets.items()
        })

    return commit


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
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=10_000,
    )
