"""Real-time analytics pipeline: Kafka -> tumbling-window aggregator -> ClickHouse."""

from rtanalytics.events import Event, random_event
from rtanalytics.windows import RollupRow, TumblingWindowAggregator

__version__ = "0.1.0"
__all__ = ["Event", "RollupRow", "TumblingWindowAggregator", "random_event"]
