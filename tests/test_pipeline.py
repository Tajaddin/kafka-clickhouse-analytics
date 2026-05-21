"""Pipeline + sink tests using a fake ClickHouse client (no infra)."""
from __future__ import annotations

import random

from rtanalytics.consumer import run_pipeline
from rtanalytics.events import random_event
from rtanalytics.sink_clickhouse import ClickHouseSink, rows_to_records
from rtanalytics.windows import RollupRow, TumblingWindowAggregator


class FakeClient:
    """Records inserts so tests can assert what the sink wrote."""

    def __init__(self):
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list, list]] = []

    def command(self, sql: str):
        self.commands.append(sql)

    def insert(self, table: str, records: list, column_names: list):
        self.inserts.append((table, records, column_names))


def test_rows_to_records_shape():
    row = RollupRow(0, 1000, "acme", "view", 3, 30.0, 5.0, 15.0)
    records = rows_to_records([row])
    assert records == [[0, 1000, "acme", "view", 3, 30.0, 5.0, 15.0]]


def test_sink_ensure_schema_and_write():
    fake = FakeClient()
    sink = ClickHouseSink(client=fake)
    sink.ensure_schema()
    assert any("CREATE TABLE" in c for c in fake.commands)

    rows = [RollupRow(0, 1000, "acme", "view", 1, 1.0, 1.0, 1.0)]
    n = sink.write(rows)
    assert n == 1
    assert fake.inserts[0][0] == "rollups"


def test_sink_write_empty_is_noop():
    fake = FakeClient()
    sink = ClickHouseSink(client=fake)
    assert sink.write([]) == 0
    assert fake.inserts == []


def test_run_pipeline_end_to_end_with_fakes():
    rng = random.Random(7)
    base = 1_000_000
    events = [random_event(base, rng).to_dict() for _ in range(5000)]
    # Add a few far-future events so earlier windows close mid-stream.
    for i in range(5):
        events.append({"event_time_ms": base + 600_000 + i, "tenant": "acme", "kind": "view", "amount": 1.0})

    agg = TumblingWindowAggregator(window_ms=60_000, allowed_lateness_ms=5_000)
    sink = ClickHouseSink(client=FakeClient())
    written = run_pipeline(iter(events), agg, sink, flush_every=500)

    # Every event lands in some window; total rollup rows must be >= 1 and
    # the aggregator must end empty after the final flush.
    assert written >= 1
    assert agg.open_windows == 0
