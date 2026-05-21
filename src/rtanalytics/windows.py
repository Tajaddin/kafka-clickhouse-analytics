"""Tumbling-window aggregator.

Groups events into fixed-size, non-overlapping time windows keyed by
(window_start, tenant, kind) and maintains running aggregates: count, sum,
min, max of amount. This is the hot path; it is pure Python with no I/O so it
benchmarks and unit-tests with zero infrastructure.

Windows close when the watermark (max event time seen minus an allowed
lateness) passes the window end. Closed windows are emitted as RollupRows and
evicted, bounding memory even on an unbounded stream.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from rtanalytics.events import Event


@dataclass(slots=True)
class _Acc:
    count: int = 0
    sum: float = 0.0
    min: float = float("inf")
    max: float = float("-inf")

    def add(self, amount: float) -> None:
        self.count += 1
        self.sum += amount
        if amount < self.min:
            self.min = amount
        if amount > self.max:
            self.max = amount


@dataclass(frozen=True, slots=True)
class RollupRow:
    """One closed window's aggregate for a (tenant, kind) pair."""

    window_start_ms: int
    window_end_ms: int
    tenant: str
    kind: str
    count: int
    sum: float
    min: float
    max: float

    @property
    def avg(self) -> float:
        return self.sum / self.count if self.count else 0.0


@dataclass
class TumblingWindowAggregator:
    """Stateful tumbling-window aggregator.

    Parameters
    ----------
    window_ms
        Window size in milliseconds.
    allowed_lateness_ms
        How long after a window's end to keep accepting late events before the
        window is closed and emitted.
    """

    window_ms: int = 60_000
    allowed_lateness_ms: int = 5_000

    _windows: dict[tuple[int, str, str], _Acc] = field(default_factory=dict, init=False)
    _watermark: int = field(default=0, init=False)
    # Earliest window_end among open windows. The eviction scan is skipped
    # while the cutoff is below this, making the per-event path O(1) amortized
    # instead of O(open_windows).
    _min_open_end: float = field(default=float("inf"), init=False)
    dropped_late: int = field(default=0, init=False)

    def window_start(self, event_time_ms: int) -> int:
        return (event_time_ms // self.window_ms) * self.window_ms

    def add(self, event: Event) -> list[RollupRow]:
        """Add one event. Returns any windows that closed as a result."""
        ws = self.window_start(event.event_time_ms)
        window_end = ws + self.window_ms

        # Late event for an already-closed window: drop and count.
        if window_end + self.allowed_lateness_ms <= self._watermark:
            self.dropped_late += 1
            return []

        key = (ws, event.tenant, event.kind)
        acc = self._windows.get(key)
        if acc is None:
            acc = _Acc()
            self._windows[key] = acc
            if window_end < self._min_open_end:
                self._min_open_end = window_end
        acc.add(event.amount)

        if event.event_time_ms > self._watermark:
            self._watermark = event.event_time_ms

        # Fast path: nothing can close yet, so skip the scan entirely.
        cutoff = self._watermark - self.allowed_lateness_ms
        if cutoff < self._min_open_end:
            return []
        return self._evict_closed(cutoff)

    def _evict_closed(self, cutoff: int) -> list[RollupRow]:
        closed: list[RollupRow] = []
        new_min = float("inf")
        for key in list(self._windows.keys()):
            ws, tenant, kind = key
            window_end = ws + self.window_ms
            if window_end <= cutoff:
                acc = self._windows.pop(key)
                closed.append(self._row(ws, window_end, tenant, kind, acc))
            elif window_end < new_min:
                new_min = window_end
        self._min_open_end = new_min
        return closed

    def flush(self) -> list[RollupRow]:
        """Close and emit all remaining open windows. Call at end of stream."""
        rows = [
            self._row(ws, ws + self.window_ms, tenant, kind, acc)
            for (ws, tenant, kind), acc in self._windows.items()
        ]
        self._windows.clear()
        self._min_open_end = float("inf")
        return rows

    @staticmethod
    def _row(ws: int, we: int, tenant: str, kind: str, acc: _Acc) -> RollupRow:
        return RollupRow(
            window_start_ms=ws,
            window_end_ms=we,
            tenant=tenant,
            kind=kind,
            count=acc.count,
            sum=round(acc.sum, 4),
            min=acc.min,
            max=acc.max,
        )

    @property
    def open_windows(self) -> int:
        return len(self._windows)
