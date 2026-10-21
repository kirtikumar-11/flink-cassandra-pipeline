"""Pure-Python business rules used by the Flink job. No Flink imports, so they are easy to unit test.

An order moves PLACED -> ACCEPTED -> PICKED_UP -> DELIVERED, or ends early as CANCELLED.
Events can arrive twice (producer retries) or out of order (network, retries, partitions),
so the job never trusts arrival order: it keeps the "furthest" status it has seen.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal

STATUS_RANK = {"PLACED": 1, "ACCEPTED": 2, "PICKED_UP": 3, "DELIVERED": 4, "CANCELLED": 4}
TERMINAL = {"DELIVERED", "CANCELLED"}
REQUIRED = {
    "event_id": str,
    "order_id": int,
    "customer_id": int,
    "restaurant_id": int,
    "city": str,
    "amount": (int, float),
    "status": str,
    "placed_at": int,
    "event_time": int,
}


def parse_event(raw: str):
    """Return (event, None) for a valid message, or (None, reason) for a bad one."""
    try:
        event = json.loads(raw)
    except (ValueError, TypeError):
        return None, "not_json"
    if not isinstance(event, dict):
        return None, "not_an_object"
    for field, kind in REQUIRED.items():
        if field not in event or event[field] is None:
            return None, f"missing_{field}"
        if isinstance(event[field], bool) or not isinstance(event[field], kind):
            return None, f"bad_type_{field}"
    event["status"] = event["status"].strip().upper()
    if event["status"] not in STATUS_RANK:
        return None, "unknown_status"
    if event["amount"] <= 0:
        return None, "non_positive_amount"
    if event["event_time"] < event["placed_at"]:
        return None, "event_before_order_placed"
    return event, None


def merge_status(current, new):
    """Keep the furthest status. A late ACCEPTED can't move a PICKED_UP order backwards,
    and the first terminal status wins (a DELIVERED order can't become CANCELLED)."""
    if current is None:
        return new
    if current in TERMINAL:
        return current
    return new if STATUS_RANK[new] > STATUS_RANK[current] else current


def to_utc(epoch_ms: int) -> datetime:
    """Naive UTC datetime: Flink maps it to a Java timestamp, which Cassandra stores as UTC."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def day_bucket(epoch_ms: int) -> str:
    """Cassandra partitions by (key, day) so no partition grows forever."""
    return to_utc(epoch_ms).strftime("%Y-%m-%d")


def money(amount) -> Decimal:
    return Decimal(str(round(float(amount), 2)))
