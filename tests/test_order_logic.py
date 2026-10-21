import json
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flink", "jobs"))
from order_logic import day_bucket, merge_status, money, parse_event, to_utc

GOOD = {"event_id": "e1", "order_id": 7, "customer_id": 1001, "restaurant_id": 101, "city": "Pune",
        "amount": 250.5, "status": "placed ", "placed_at": 1_700_000_000_000, "event_time": 1_700_000_060_000}


def test_valid_event_is_normalised():
    event, reason = parse_event(json.dumps(GOOD))
    assert reason is None and event["status"] == "PLACED"


def test_bad_events_get_a_reason():
    cases = {
        '{"order_id": 1,': "not_json",
        json.dumps({**GOOD, "order_id": None}): "missing_order_id",
        json.dumps({**GOOD, "amount": "ten"}): "bad_type_amount",
        json.dumps({**GOOD, "amount": -5}): "non_positive_amount",
        json.dumps({**GOOD, "status": "TELEPORTED"}): "unknown_status",
        json.dumps({**GOOD, "order_id": True}): "bad_type_order_id",
        json.dumps({**GOOD, "event_time": GOOD["placed_at"] - 1}): "event_before_order_placed",
    }
    for raw, expected in cases.items():
        assert parse_event(raw) == (None, expected)


def test_status_never_moves_backwards():
    assert merge_status(None, "ACCEPTED") == "ACCEPTED"
    assert merge_status("PICKED_UP", "ACCEPTED") == "PICKED_UP"
    assert merge_status("ACCEPTED", "DELIVERED") == "DELIVERED"
    assert merge_status("DELIVERED", "CANCELLED") == "DELIVERED"
    assert merge_status("CANCELLED", "DELIVERED") == "CANCELLED"


def test_time_and_money_helpers():
    assert day_bucket(0) == "1970-01-01"
    assert to_utc(1_000).isoformat() == "1970-01-01T00:00:01"
    assert money(1 / 3) == Decimal("0.33")
    assert money(99.9) == Decimal("99.9")
