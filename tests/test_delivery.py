"""Delivery guarantees of `rta consume`: a crash neither loses nor duplicates rollups.

Kafka and ClickHouse are replaced by in-memory fakes at the module level, so
the real CLI wiring (consumer config, commits, sink) runs without a broker.
"""
from __future__ import annotations

import json
import random
import sys
from collections import namedtuple
from types import ModuleType, SimpleNamespace

import pytest

from rtanalytics.cli import main
from rtanalytics.consumer import consume
from rtanalytics.events import random_event
from rtanalytics.windows import TumblingWindowAggregator

TopicPartition = namedtuple("TopicPartition", ["topic", "partition"])
OffsetAndMetadata = namedtuple("OffsetAndMetadata", ["offset", "metadata", "leader_epoch"])
Message = namedtuple("Message", ["topic", "partition", "offset", "value"])


class SimulatedCrash(Exception):
    pass


class FakeBroker:
    """One single-partition topic and the consumer group's committed offset.

    With auto-commit on, the consumed position is committed as soon as the
    application asks for the next record, as kafka-python does on poll.
    """

    def __init__(self, events: list[dict]):
        self.log = [json.dumps(e).encode() for e in events]
        self.committed = 0
        self.commit_calls = 0
        self.crash_at: int | None = None
        self.crash_on_commit: int | None = None

    def append(self, events: list[dict]) -> None:
        self.log.extend(json.dumps(e).encode() for e in events)

    def kafka_module(self) -> ModuleType:
        broker = self

        class KafkaConsumer:
            def __init__(self, topic, *, enable_auto_commit=True, value_deserializer, **_):
                self.topic = topic
                self.auto_commit = enable_auto_commit
                self.deserialize = value_deserializer

            def __iter__(self):
                pos = broker.committed
                while pos < len(broker.log):
                    if pos == broker.crash_at:
                        raise SimulatedCrash(f"killed before offset {pos}")
                    yield Message(self.topic, 0, pos, self.deserialize(broker.log[pos]))
                    pos += 1
                    if self.auto_commit:
                        broker.committed = pos

            def commit(self, offsets):
                broker.commit_calls += 1
                if broker.commit_calls == broker.crash_on_commit:
                    raise SimulatedCrash("killed before the commit reached the broker")
                ((tp, meta),) = offsets.items()
                assert tp == TopicPartition(self.topic, 0)
                broker.committed = meta.offset

        mod = ModuleType("kafka")
        mod.KafkaConsumer = KafkaConsumer
        return mod


class FakeClickHouse:
    """Keeps every inserted row, the way a table reads before any merge."""

    def __init__(self):
        self.rows: list[tuple] = []

    def command(self, sql: str) -> None:
        pass

    def insert(self, table: str, records: list, column_names: list) -> None:
        self.rows.extend(tuple(r) for r in records)

    def query(self, sql: str):
        assert "maxOrNull(window_end_ms)" in sql, sql
        latest = max((r[1] for r in self.rows), default=None)
        return SimpleNamespace(result_rows=[(latest,)])


def _events(n: int = 3000) -> list[dict]:
    rng = random.Random(11)
    base = 1_700_000_000_000
    # Up to 10s of jitter against 1s windows and 5s lateness: plenty of
    # out-of-order events, windows closing mid-stream, and some late drops.
    return [random_event(base + i * 50, rng).to_dict() for i in range(n)]


def _consume(monkeypatch, broker: FakeBroker, clickhouse: FakeClickHouse) -> None:
    structs = ModuleType("kafka.structs")
    structs.TopicPartition = TopicPartition
    structs.OffsetAndMetadata = OffsetAndMetadata
    monkeypatch.setitem(sys.modules, "kafka", broker.kafka_module())
    monkeypatch.setitem(sys.modules, "kafka.structs", structs)
    monkeypatch.setitem(
        sys.modules, "clickhouse_connect", SimpleNamespace(get_client=lambda **_: clickhouse)
    )
    assert main(["consume", "--window-ms", "1000"]) == 0


def _uninterrupted(monkeypatch) -> list[tuple]:
    clickhouse = FakeClickHouse()
    _consume(monkeypatch, FakeBroker(_events()), clickhouse)
    return sorted(clickhouse.rows)


@pytest.mark.parametrize("crash_at", [700, 1500, 2500])
def test_crash_mid_stream_loses_and_duplicates_nothing(monkeypatch, crash_at):
    expected = _uninterrupted(monkeypatch)
    broker, clickhouse = FakeBroker(_events()), FakeClickHouse()
    broker.crash_at = crash_at
    with pytest.raises(SimulatedCrash):
        _consume(monkeypatch, broker, clickhouse)

    broker.crash_at = None
    _consume(monkeypatch, broker, clickhouse)
    assert sorted(clickhouse.rows) == expected


def test_crash_between_sink_write_and_commit_duplicates_nothing(monkeypatch):
    expected = _uninterrupted(monkeypatch)
    broker, clickhouse = FakeBroker(_events()), FakeClickHouse()
    broker.crash_on_commit = 2
    with pytest.raises(SimulatedCrash):
        _consume(monkeypatch, broker, clickhouse)

    _consume(monkeypatch, broker, clickhouse)
    assert sorted(clickhouse.rows) == expected


def test_commits_stop_at_the_earliest_event_in_an_open_window():
    def msg(partition, offset, t):
        return Message("events", partition, offset,
                       {"event_time_ms": t, "tenant": "acme", "kind": "view", "amount": 1.0})

    stream = [msg(0, 0, 100), msg(1, 0, 1100), msg(0, 1, 1200)]
    commits: list[dict[int, int]] = []
    sink = SimpleNamespace(write=lambda rows: len(list(rows)), max_window_end=lambda: None)
    agg = TumblingWindowAggregator(window_ms=1000, allowed_lateness_ms=0)

    consume(stream, agg, sink, commits.append, flush_every=3)

    # After the third event only [0, 1000) is in the sink; [1000, 2000) still
    # holds offset 0 of partition 1 and offset 1 of partition 0, and stays
    # open when the stream ends.
    assert commits == [{0: 1, 1: 0}, {0: 1, 1: 0}]


def test_restart_after_the_stream_goes_idle_keeps_every_event(monkeypatch):
    def event(t: int) -> dict:
        return {"event_time_ms": t, "tenant": "acme", "kind": "view", "amount": 1.0}

    base = 1_700_000_000_000
    broker, clickhouse = FakeBroker([event(base + 100)] * 50), FakeClickHouse()
    _consume(monkeypatch, broker, clickhouse)  # exits idle while [base, base+1s) is open

    # More events for that window, then one 7s later that closes it.
    broker.append([event(base + 200)] * 50 + [event(base + 7000)])
    _consume(monkeypatch, broker, clickhouse)
    assert [row[:5] for row in clickhouse.rows] == [(base, base + 1000, "acme", "view", 100)]
    assert broker.committed == 100  # the last event's window is still open
