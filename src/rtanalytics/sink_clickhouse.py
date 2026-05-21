"""ClickHouse sink for rollup rows.

clickhouse-connect is imported lazily so the unit tests and benchmark do not
require it. Install the package via the .[stream] extra to use this.

The target table is a MergeTree keyed by (window_start_ms, tenant, kind),
which is the natural sort order for time-series rollups and makes range scans
over a tenant's recent windows cheap.
"""
from __future__ import annotations

from collections.abc import Iterable

from rtanalytics.windows import RollupRow

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rollups (
    window_start_ms Int64,
    window_end_ms   Int64,
    tenant          LowCardinality(String),
    kind            LowCardinality(String),
    count           UInt64,
    sum             Float64,
    min             Float64,
    max             Float64
) ENGINE = MergeTree
ORDER BY (window_start_ms, tenant, kind)
""".strip()

_COLUMNS = ["window_start_ms", "window_end_ms", "tenant", "kind", "count", "sum", "min", "max"]


def rows_to_records(rows: Iterable[RollupRow]) -> list[list]:
    """Project RollupRows into ClickHouse insert records (list-of-lists)."""
    return [
        [r.window_start_ms, r.window_end_ms, r.tenant, r.kind, r.count, r.sum, r.min, r.max]
        for r in rows
    ]


class ClickHouseSink:
    """Thin wrapper around a clickhouse-connect client.

    Pass an existing client (e.g. in tests) or host/port to construct one.
    """

    def __init__(self, client=None, *, host: str = "localhost", port: int = 8123,
                 username: str = "default", password: str = "", database: str = "default"):
        if client is None:
            try:
                import clickhouse_connect
            except ImportError as exc:
                raise RuntimeError(
                    "clickhouse-connect not installed. pip install .[stream]"
                ) from exc
            client = clickhouse_connect.get_client(
                host=host, port=port, username=username, password=password, database=database
            )
        self._client = client

    def ensure_schema(self) -> None:
        self._client.command(CREATE_TABLE_SQL)

    def write(self, rows: Iterable[RollupRow]) -> int:
        records = rows_to_records(rows)
        if not records:
            return 0
        self._client.insert("rollups", records, column_names=_COLUMNS)
        return len(records)
