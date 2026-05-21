"""Kafka producer: publishes JSON events to a topic.

kafka-python is imported lazily (only when you actually produce), so the rest
of the package stays import-light.
"""
from __future__ import annotations

import json
import random

from rtanalytics.events import random_event


def make_producer(bootstrap_servers: str):
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("kafka-python not installed. pip install .[stream]") from exc
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        linger_ms=5,
    )


def produce_events(producer, topic: str, count: int, base_time_ms: int, seed: int = 7) -> int:
    """Publish `count` random events to `topic`. Returns the number sent."""
    rng = random.Random(seed)
    for _ in range(count):
        ev = random_event(base_time_ms, rng)
        producer.send(topic, ev.to_dict())
    producer.flush()
    return count
