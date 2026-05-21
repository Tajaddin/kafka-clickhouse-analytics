"""Event schema tests."""
from __future__ import annotations

import random

from rtanalytics.events import EVENT_KINDS, TENANTS, Event, random_event


def test_roundtrip_dict():
    e = Event(event_time_ms=1234, tenant="acme", kind="purchase", amount=99.5)
    back = Event.from_dict(e.to_dict())
    assert back == e


def test_from_dict_coerces_types():
    e = Event.from_dict({"event_time_ms": "1000", "tenant": "x", "kind": "view", "amount": "12.5"})
    assert e.event_time_ms == 1000
    assert isinstance(e.event_time_ms, int)
    assert e.amount == 12.5
    assert isinstance(e.amount, float)


def test_random_event_within_bounds():
    rng = random.Random(1)
    base = 1_000_000
    for _ in range(200):
        e = random_event(base, rng)
        assert base <= e.event_time_ms <= base + 10_000
        assert e.tenant in TENANTS
        assert e.kind in EVENT_KINDS
        assert 1.0 <= e.amount <= 500.0


def test_random_event_is_deterministic_with_seed():
    a = random_event(0, random.Random(42))
    b = random_event(0, random.Random(42))
    assert a == b
