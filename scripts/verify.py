"""Recompute the expected results straight from Kafka and compare them with what Flink wrote to Cassandra.

Reads every message in order-events from the beginning, applies the same rules in plain Python,
then checks each Cassandra table. Orders and windows near the end of the stream are skipped,
because Flink only fires their timers/windows once newer events move the watermark past them.

  python scripts/verify.py
"""

import json
import os
import sys
from collections import defaultdict
from decimal import Decimal

from cassandra.cluster import Cluster
from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flink", "jobs"))
from order_logic import TERMINAL, merge_status, money, parse_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "order-events")
SLA_MS = int(os.getenv("SLA_MINUTES", "45")) * 60_000
OUT_OF_ORDER_MS = int(os.getenv("OUT_OF_ORDER_SECONDS", "30")) * 1000
LATENESS_MS = int(os.getenv("ALLOWED_LATENESS_SECONDS", "60")) * 1000
MARGIN_MS = 2 * 60_000
AMBIGUOUS_MS = int(os.getenv("VERIFY_AMBIGUOUS_SECONDS", "180")) * 1000


def read_topic():
    """All messages, per partition, in offset order (the order Flink reads them), with their
    produce timestamp, which tells us roughly how Flink interleaved the partitions."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verify-readonly",
                         "enable.auto.commit": False, "isolation.level": "read_committed"})
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    by_partition = {}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
        consumer.assign([TopicPartition(TOPIC, p, low)])
        rows = []
        while len(rows) < high - low:
            msg = consumer.poll(5)
            if msg is None:
                break
            if not msg.error():
                rows.append((msg.timestamp()[1], msg.value().decode("utf-8", errors="replace")))
        by_partition[p] = rows
    consumer.close()
    return by_partition


def expected(by_partition):
    """Replay the messages in (approximate) arrival order and apply the job's rules.

    Flink's watermark is the minimum over all partitions of (max event_time seen - out-of-order delay),
    so we track it the same way to know when timers and windows fired."""
    merged = sorted((ts, p, i, raw) for p, rows in by_partition.items() for i, (ts, raw) in enumerate(rows))
    partition_max = {}
    seen, status, info, terminal_wm = set(), {}, {}, {}
    windows = defaultdict(lambda: [0, Decimal("0"), 0, 0])
    windows_with_late_data = set()
    stats = {"messages": 0, "bad": 0, "duplicates": 0, "too_late_for_window": 0}

    def watermark():
        if len(partition_max) < len(by_partition):
            return float("-inf")
        return min(partition_max.values()) - OUT_OF_ORDER_MS - 1

    for _, p, _, raw in merged:
        stats["messages"] += 1
        e, reason = parse_event(raw)
        if e is None:
            stats["bad"] += 1
            continue
        wm = watermark()
        partition_max[p] = max(partition_max.get(p, e["event_time"]), e["event_time"])
        if e["event_id"] in seen:
            stats["duplicates"] += 1
            continue
        seen.add(e["event_id"])
        oid = e["order_id"]
        if oid not in status:
            info[oid] = e
        new = merge_status(status.get(oid), e["status"])
        status[oid] = new
        if new in TERMINAL and oid not in terminal_wm:
            terminal_wm[oid] = wm
        win_start = e["event_time"] - e["event_time"] % 60_000
        if e["event_time"] < wm:
            windows_with_late_data.add((e["restaurant_id"], win_start))
        if wm >= win_start + 60_000 - 1 + LATENESS_MS:
            stats["too_late_for_window"] += 1
            continue
        w = windows[(e["restaurant_id"], win_start)]
        if e["status"] == "PLACED":
            w[0] += 1
            w[1] += Decimal(str(e["amount"]))
        elif e["status"] == "DELIVERED":
            w[2] += 1
        elif e["status"] == "CANCELLED":
            w[3] += 1
    return stats, status, info, terminal_wm, windows, windows_with_late_data, watermark()


def main():
    by_partition = read_topic()
    stats, status, info, terminal_wm, windows, late_windows, final_wm = expected(by_partition)
    cutoff = final_wm - MARGIN_MS
    print(f"Kafka: {stats} | orders {len(status)}")

    session = Cluster([os.getenv("CASSANDRA_HOST", "localhost")]).connect("food_delivery")
    problems = []

    rows = {r.order_id: r for r in session.execute("SELECT order_id, status, amount, sla_breached FROM orders_by_id")}
    if set(rows) != set(status):
        problems.append(f"orders_by_id has {len(rows)} orders, expected {len(status)}")
    wrong = [o for o, s in status.items() if o in rows and rows[o].status != s]
    if wrong:
        problems.append(f"{len(wrong)} orders with a wrong status, e.g. {wrong[:3]}")
    bad_amount = [o for o in rows if o in info and rows[o].amount != money(info[o]["amount"])]
    if bad_amount:
        problems.append(f"{len(bad_amount)} orders with a wrong amount")

    by_customer = session.execute("SELECT count(*) FROM orders_by_customer").one()[0]
    if by_customer != len(status):
        problems.append(f"orders_by_customer has {by_customer} rows, expected {len(status)}")

    due = {o for o, e in info.items() if e["placed_at"] + SLA_MS <= cutoff}
    expected_breaches, ambiguous = set(), set()
    for o in due:
        deadline = info[o]["placed_at"] + SLA_MS
        finished_wm = terminal_wm.get(o, float("inf"))
        if finished_wm >= deadline + AMBIGUOUS_MS:
            expected_breaches.add(o)
        elif finished_wm >= deadline - AMBIGUOUS_MS:
            ambiguous.add(o)
    due -= ambiguous
    alerted = {r.order_id for r in session.execute("SELECT order_id FROM late_orders_by_city")}
    flagged = {o for o, r in rows.items() if r.sla_breached}
    missing, extra = expected_breaches - alerted, (alerted & due) - expected_breaches
    if missing or extra:
        problems.append(f"SLA alerts: {len(missing)} missing, {len(extra)} unexpected")
    if alerted != flagged:
        problems.append("late_orders_by_city and orders_by_id.sla_breached disagree")

    metric_rows = session.execute("SELECT restaurant_id, window_start, placed_orders, revenue, "
                                  "delivered_orders, cancelled_orders FROM restaurant_metrics_by_minute")
    actual_windows = {(r.restaurant_id, int(r.window_start.timestamp() * 1000)):
                      [r.placed_orders, r.revenue, r.delivered_orders, r.cancelled_orders] for r in metric_rows}
    checked = 0
    for key, value in windows.items():
        if key[1] + 60_000 > cutoff or key in late_windows:
            continue
        checked += 1
        got = actual_windows.get(key)
        if got is None or got[0] != value[0] or got[2:] != value[2:] or abs(got[1] - value[1]) > Decimal("0.05"):
            problems.append(f"window {key}: expected {value}, got {got}")
            if len(problems) > 10:
                break
    late_rows = session.execute("SELECT count(*) FROM late_events_by_day").one()[0]
    if not 0.9 * stats["too_late_for_window"] <= late_rows <= stats["too_late_for_window"]:
        problems.append(f"late_events_by_day has {late_rows}, expected about {stats['too_late_for_window']}")

    print(f"Cassandra: orders {len(rows)} | SLA breaches {len(alerted)} (expected {len(expected_breaches)} "
          f"among {len(due)} orders past their deadline, {len(ambiguous)} borderline skipped) | windows checked {checked} | "
          f"late events {late_rows} (expected about {stats['too_late_for_window']})")
    if problems:
        print("PROBLEMS:\n  " + "\n  ".join(problems))
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
