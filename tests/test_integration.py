"""End-to-end integration test: real Kafka + ClickHouse via testcontainers.

Skipped automatically when Docker is unavailable (local dev on Windows
without a running daemon) or when the optional deps are missing. Runs in CI,
which has Docker.
"""
from __future__ import annotations

import time

import pytest


def _docker_available() -> bool:
    import shutil
    import subprocess

    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.integration

testcontainers = pytest.importorskip("testcontainers")
if not _docker_available():
    pytest.skip("Docker not available", allow_module_level=True)

from testcontainers.clickhouse import ClickHouseContainer  # noqa: E402
from testcontainers.kafka import KafkaContainer  # noqa: E402

from rtanalytics.consumer import make_consumer, run_pipeline  # noqa: E402
from rtanalytics.producer import make_producer, produce_events  # noqa: E402
from rtanalytics.sink_clickhouse import ClickHouseSink  # noqa: E402
from rtanalytics.windows import TumblingWindowAggregator  # noqa: E402


def test_kafka_to_clickhouse_roundtrip():
    with KafkaContainer() as kafka, ClickHouseContainer() as ch:
        bootstrap = kafka.get_bootstrap_server()

        # Produce.
        producer = make_producer(bootstrap)
        sent = produce_events(producer, "events", count=2000, base_time_ms=int(time.time() * 1000))
        assert sent == 2000

        # Consume -> aggregate -> sink.
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=ch.get_container_host_ip(),
            port=int(ch.get_exposed_port(8123)),
            username=ch.username,
            password=ch.password,
        )
        sink = ClickHouseSink(client=client)
        sink.ensure_schema()

        consumer = make_consumer(bootstrap, "events", group_id="it")
        agg = TumblingWindowAggregator(window_ms=1000, allowed_lateness_ms=0)
        written = run_pipeline((m.value for m in consumer), agg, sink)
        assert written > 0

        total = client.query("SELECT sum(count) FROM rollups").result_rows[0][0]
        assert total == 2000
