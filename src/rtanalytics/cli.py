"""CLI for the live pipeline. Needs the .[stream] extra and running services.

    rta produce --bootstrap localhost:9092 --topic events --count 100000
    rta consume --bootstrap localhost:9092 --topic events --clickhouse localhost
"""
from __future__ import annotations

import argparse
import sys
import time


def _cmd_produce(args: argparse.Namespace) -> int:
    from rtanalytics.producer import make_producer, produce_events

    producer = make_producer(args.bootstrap)
    sent = produce_events(producer, args.topic, args.count, int(time.time() * 1000))
    print(f"produced {sent} events to {args.topic}")
    return 0


def _cmd_consume(args: argparse.Namespace) -> int:
    from rtanalytics.consumer import consume, kafka_commit, make_consumer
    from rtanalytics.sink_clickhouse import ClickHouseSink
    from rtanalytics.windows import TumblingWindowAggregator

    sink = ClickHouseSink(host=args.clickhouse, port=args.clickhouse_port)
    sink.ensure_schema()
    consumer = make_consumer(args.bootstrap, args.topic)
    agg = TumblingWindowAggregator(window_ms=args.window_ms)

    written = consume(consumer, agg, sink, kafka_commit(consumer, args.topic))
    print(f"wrote {written} rollup rows to ClickHouse; dropped {agg.dropped_late} late events")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Real-time analytics pipeline CLI.")
    sub = p.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("produce", help="Publish random events to Kafka.")
    pp.add_argument("--bootstrap", default="localhost:9092")
    pp.add_argument("--topic", default="events")
    pp.add_argument("--count", type=int, default=100_000)
    pp.set_defaults(func=_cmd_produce)

    pc = sub.add_parser("consume", help="Consume, aggregate, sink to ClickHouse.")
    pc.add_argument("--bootstrap", default="localhost:9092")
    pc.add_argument("--topic", default="events")
    pc.add_argument("--clickhouse", default="localhost")
    pc.add_argument("--clickhouse-port", type=int, default=8123)
    pc.add_argument("--window-ms", type=int, default=60_000)
    pc.set_defaults(func=_cmd_consume)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
