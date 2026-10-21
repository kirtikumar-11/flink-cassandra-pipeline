import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flink", "jobs"))
from order_logic import STATUS_RANK, parse_event  # noqa: E402
from producer import OrderSimulator, make_bad_message  # noqa: E402


def drain(sim, orders):
    events = []
    while True:
        item, orders = sim.next_event(orders)
        if item is None:
            return events
        events.append(item)


def test_events_come_out_in_simulated_time_order_and_are_valid():
    sim = OrderSimulator(random.Random(1), 1_700_000_000_000, 40, 0.05, 0.05, 0.0)
    events = drain(sim, 300)
    send_times = [t for t, _ in events]
    assert send_times == sorted(send_times)
    for _, e in events:
        parsed, reason = parse_event(json.dumps(e))
        assert reason is None, reason


def test_each_order_moves_forward():
    sim = OrderSimulator(random.Random(2), 1_700_000_000_000, 40, 0.05, 0.05, 0.0)
    by_order = {}
    for _, e in drain(sim, 200):
        by_order.setdefault(e["order_id"], []).append(e)
    assert len(by_order) == 200
    for events in by_order.values():
        assert events[0]["status"] == "PLACED"
        ranks = [STATUS_RANK[e["status"]] for e in events]
        assert ranks == sorted(ranks)


def test_late_events_keep_their_event_time_but_are_sent_later():
    sim = OrderSimulator(random.Random(3), 1_700_000_000_000, 40, 0.0, 0.0, 1.0)
    for send_at, e in drain(sim, 20):
        assert send_at - e["event_time"] == 10 * 60_000


def test_bad_messages_are_rejected():
    rng = random.Random(4)
    for _ in range(50):
        assert parse_event(make_bad_message(rng).decode())[0] is None
