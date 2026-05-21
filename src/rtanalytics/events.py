"""Event schema and a deterministic generator.

Events model a clickstream / product-interaction feed: each carries a
timestamp, a tenant, an event kind, and a numeric amount (e.g. price, latency).
The aggregator rolls these into per-window, per-(tenant, kind) summaries.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

EVENT_KINDS = ("view", "click", "add_to_cart", "purchase", "refund")
TENANTS = ("acme", "globex", "initech", "umbrella", "soylent")


@dataclass(slots=True)
class Event:
    """One raw event. event_time_ms is epoch milliseconds."""

    event_time_ms: int
    tenant: str
    kind: str
    amount: float

    def to_dict(self) -> dict:
        return {
            "event_time_ms": self.event_time_ms,
            "tenant": self.tenant,
            "kind": self.kind,
            "amount": self.amount,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Event:
        return cls(
            event_time_ms=int(d["event_time_ms"]),
            tenant=str(d["tenant"]),
            kind=str(d["kind"]),
            amount=float(d["amount"]),
        )


def random_event(base_time_ms: int, rng: random.Random) -> Event:
    """Generate one pseudo-random event within ~10s of base_time_ms."""
    return Event(
        event_time_ms=base_time_ms + rng.randint(0, 10_000),
        tenant=rng.choice(TENANTS),
        kind=rng.choice(EVENT_KINDS),
        amount=round(rng.uniform(1.0, 500.0), 2),
    )
