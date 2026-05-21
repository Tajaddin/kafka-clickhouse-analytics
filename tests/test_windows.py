"""Tumbling-window aggregator tests (pure, no infra)."""
from __future__ import annotations

from rtanalytics.events import Event
from rtanalytics.windows import TumblingWindowAggregator


def ev(t_ms: int, tenant: str = "acme", kind: str = "view", amount: float = 10.0) -> Event:
    return Event(event_time_ms=t_ms, tenant=tenant, kind=kind, amount=amount)


class TestWindowAssignment:
    def test_window_start_aligns_to_window_size(self):
        agg = TumblingWindowAggregator(window_ms=1000)
        assert agg.window_start(0) == 0
        assert agg.window_start(999) == 0
        assert agg.window_start(1000) == 1000
        assert agg.window_start(2500) == 2000


class TestAggregation:
    def test_aggregates_count_sum_min_max(self):
        agg = TumblingWindowAggregator(window_ms=1000, allowed_lateness_ms=0)
        for amt in (5.0, 15.0, 10.0):
            agg.add(ev(100, amount=amt))
        rows = agg.flush()
        assert len(rows) == 1
        r = rows[0]
        assert r.count == 3
        assert r.sum == 30.0
        assert r.min == 5.0
        assert r.max == 15.0
        assert r.avg == 10.0

    def test_separate_keys_per_tenant_and_kind(self):
        agg = TumblingWindowAggregator(window_ms=1000)
        agg.add(ev(100, tenant="acme", kind="view"))
        agg.add(ev(100, tenant="acme", kind="click"))
        agg.add(ev(100, tenant="globex", kind="view"))
        rows = agg.flush()
        keys = {(r.tenant, r.kind) for r in rows}
        assert keys == {("acme", "view"), ("acme", "click"), ("globex", "view")}

    def test_events_split_across_windows(self):
        agg = TumblingWindowAggregator(window_ms=1000)
        agg.add(ev(500))   # window [0,1000)
        agg.add(ev(1500))  # window [1000,2000)
        rows = sorted(agg.flush(), key=lambda r: r.window_start_ms)
        assert [r.window_start_ms for r in rows] == [0, 1000]


class TestWatermarkAndEviction:
    def test_window_closes_once_watermark_passes_end_plus_lateness(self):
        agg = TumblingWindowAggregator(window_ms=1000, allowed_lateness_ms=500)
        # Window [0,1000). Closes when watermark >= 1000 + 500 = 1500.
        agg.add(ev(100))
        assert agg.add(ev(1400)) == []        # watermark 1400, not yet
        closed = agg.add(ev(1600))            # watermark 1600 -> closes [0,1000)
        assert any(r.window_start_ms == 0 for r in closed)

    def test_late_event_for_closed_window_is_dropped(self):
        agg = TumblingWindowAggregator(window_ms=1000, allowed_lateness_ms=0)
        agg.add(ev(100))     # window [0,1000)
        agg.add(ev(5000))    # advances watermark, closes [0,1000)
        before = agg.dropped_late
        agg.add(ev(200))     # very late, belongs to closed [0,1000)
        assert agg.dropped_late == before + 1

    def test_open_windows_bounded_after_eviction(self):
        agg = TumblingWindowAggregator(window_ms=1000, allowed_lateness_ms=0)
        for i in range(100):
            agg.add(ev(i * 1000))  # each in its own window, monotonic time
        # Once watermark advances, old windows evict; only recent stay open.
        assert agg.open_windows < 100


class TestFlush:
    def test_flush_emits_all_remaining_and_clears(self):
        agg = TumblingWindowAggregator(window_ms=1000)
        agg.add(ev(100))
        agg.add(ev(200))
        rows = agg.flush()
        assert len(rows) == 1
        assert agg.open_windows == 0
        assert agg.flush() == []
