"""Producer logic test using a fake Kafka producer (no broker)."""
from __future__ import annotations

from rtanalytics.events import EVENT_KINDS, TENANTS
from rtanalytics.producer import produce_events


class FakeProducer:
    """Captures sends so we can assert what would hit Kafka."""

    def __init__(self):
        self.sent: list[tuple[str, dict]] = []
        self.flushed = False

    def send(self, topic: str, value: dict):
        self.sent.append((topic, value))

    def flush(self):
        self.flushed = True


def test_produce_events_sends_count_and_flushes():
    p = FakeProducer()
    n = produce_events(p, "events", count=250, base_time_ms=1_000_000, seed=7)
    assert n == 250
    assert len(p.sent) == 250
    assert p.flushed is True


def test_produced_events_are_well_formed():
    p = FakeProducer()
    produce_events(p, "events", count=100, base_time_ms=1_000_000, seed=3)
    for topic, value in p.sent:
        assert topic == "events"
        assert value["tenant"] in TENANTS
        assert value["kind"] in EVENT_KINDS
        assert 1_000_000 <= value["event_time_ms"] <= 1_010_000
        assert 1.0 <= value["amount"] <= 500.0


def test_produce_is_deterministic_with_seed():
    a, b = FakeProducer(), FakeProducer()
    produce_events(a, "events", count=50, base_time_ms=0, seed=99)
    produce_events(b, "events", count=50, base_time_ms=0, seed=99)
    assert a.sent == b.sent
