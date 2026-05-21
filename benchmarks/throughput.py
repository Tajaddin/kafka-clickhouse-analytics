"""In-process throughput benchmark for the tumbling-window aggregator.

This measures the hot path that runs per event in production: window
assignment, accumulation, watermark advance, and closed-window eviction. No
Kafka, no ClickHouse, no Docker. The number is the ceiling the live pipeline's
aggregation stage can sustain on one core.

    python -m benchmarks.throughput --events 2000000
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

from rtanalytics.events import random_event
from rtanalytics.windows import TumblingWindowAggregator

RESULTS = Path(__file__).parent / "results" / "throughput.json"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=2_000_000)
    ap.add_argument("--window-ms", type=int, default=60_000)
    ap.add_argument("--out", type=Path, default=RESULTS)
    args = ap.parse_args(argv)

    # Pre-generate events so the timed loop measures aggregation, not RNG.
    rng = random.Random(7)
    base = 1_700_000_000_000
    print(f"generating {args.events} events...", file=sys.stderr)
    events = [random_event(base + i * 50, rng) for i in range(args.events)]

    agg = TumblingWindowAggregator(window_ms=args.window_ms, allowed_lateness_ms=5_000)
    rollups = 0

    print("running aggregation...", file=sys.stderr)
    t0 = time.perf_counter()
    for e in events:
        closed = agg.add(e)
        rollups += len(closed)
    rollups += len(agg.flush())
    elapsed = time.perf_counter() - t0

    eps = args.events / elapsed
    summary = {
        "events": args.events,
        "window_ms": args.window_ms,
        "wall_seconds": round(elapsed, 4),
        "throughput_events_per_sec": round(eps),
        "throughput_millions_per_sec": round(eps / 1_000_000, 3),
        "rollup_rows_emitted": rollups,
        "ns_per_event": round(elapsed / args.events * 1e9, 1),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    print(f"\nwrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
