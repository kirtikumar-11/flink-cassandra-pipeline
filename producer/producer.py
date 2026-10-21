"""Food-delivery event producer: simulates orders moving through their lifecycle and sends JSON to Kafka.

  PLACED -> ACCEPTED -> PICKED_UP -> DELIVERED      (most orders)
  PLACED -> CANCELLED                               (--cancel-ratio)
  PLACED -> ACCEPTED [-> PICKED_UP] -> (nothing)    (--stuck-ratio: these breach the delivery SLA)

Time is SIMULATED so you don't wait 45 real minutes for an SLA breach: with --speed 60,
one real second is one simulated minute. event_time inside each event is the simulated time,
and Flink works in event time, so it sees a realistic day of orders.

Failure-test switches:
  --dup-ratio 0.05   re-send 5% of events with the same event_id (producer retry)
  --bad-ratio 0.02   send 2% malformed messages (go to the dead-letter topic)
  --late-ratio 0.02  hold 2% of events back and send them 10 simulated minutes late

Examples:
  python producer/producer.py --orders 300
  python producer/producer.py --orders 2000 --dup-ratio 0.05 --bad-ratio 0.02 --late-ratio 0.02
  python producer/producer.py --orders 20000 --speed 0          # load test: as fast as possible
"""

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition

try:
    from dotenv import find_dotenv, load_dotenv

    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Pune"]
LATE_BY_MS = 10 * 60_000


def build_config() -> dict:
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "lz4",
        "client.id": "food-order-producer",
        "broker.address.family": os.getenv("KAFKA_ADDRESS_FAMILY", "any"),
    }
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    if protocol != "PLAINTEXT":
        config.update({
            "security.protocol": protocol,
            "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            "sasl.username": os.environ["KAFKA_USERNAME"],
            "sasl.password": os.environ["KAFKA_PASSWORD"],
            "ssl.ca.location": os.getenv("KAFKA_CA_LOCATION", "certs/ca.pem"),
        })
    return config


class OrderSimulator:
    """Creates orders on a simulated clock and schedules each order's future events."""

    def __init__(self, rng: random.Random, start_ms: int, orders_per_minute: float,
                 cancel_ratio: float, stuck_ratio: float, late_ratio: float):
        self.rng = rng
        self.next_order_ms = start_ms
        self.gap_ms = 60_000 / orders_per_minute
        self.cancel_ratio, self.stuck_ratio, self.late_ratio = cancel_ratio, stuck_ratio, late_ratio
        self.next_order_id = start_ms % 10_000_000_000
        self.queue = []
        self.seq = 0
        self.late_events = 0

    def _schedule(self, event):
        send_at = event["event_time"]
        if self.rng.random() < self.late_ratio:
            send_at += LATE_BY_MS
            self.late_events += 1
        heapq.heappush(self.queue, (send_at, self.seq, event))
        self.seq += 1

    def place_order(self):
        rng = self.rng
        placed = int(self.next_order_ms)
        self.next_order_ms += rng.expovariate(1.0) * self.gap_ms
        order = {
            "order_id": self.next_order_id,
            "customer_id": rng.randint(1_000, 9_999),
            "restaurant_id": rng.randint(100, 149),
            "city": rng.choice(CITIES),
            "amount": round(rng.uniform(120, 1800), 2),
            "placed_at": placed,
        }
        self.next_order_id += 1

        minute = 60_000
        steps = [("PLACED", placed)]
        roll = rng.random()
        if roll < self.cancel_ratio:
            steps.append(("CANCELLED", placed + rng.randint(1, 5) * minute))
        else:
            accepted = placed + rng.randint(1, 4) * minute
            picked = accepted + rng.randint(8, 20) * minute
            delivered = picked + rng.randint(8, 25) * minute
            steps.append(("ACCEPTED", accepted))
            if roll < self.cancel_ratio + self.stuck_ratio:
                if rng.random() < 0.5:
                    steps.append(("PICKED_UP", picked))
            else:
                steps += [("PICKED_UP", picked), ("DELIVERED", delivered)]
        for status, at in steps:
            self._schedule({"event_id": str(uuid.UUID(int=rng.getrandbits(128))), **order,
                            "status": status, "event_time": at})

    def next_event(self, orders_left: int):
        """Return (send_at_ms, event) in simulated-time order, creating new orders as the clock moves."""
        while orders_left > 0 and (not self.queue or self.next_order_ms <= self.queue[0][0]):
            self.place_order()
            orders_left -= 1
        if not self.queue:
            return None, orders_left
        send_at, _, event = heapq.heappop(self.queue)
        return (send_at, event), orders_left


def resume_clock(config: dict, topic: str) -> int:
    """Simulated time runs ahead of the real clock, so a second run must not start at "now":
    Flink's watermark is already past now, and every new event would count as late.
    Continue one minute after the newest event_time already in the topic."""
    connection = {k: v for k, v in config.items()
                  if k.split(".")[0] in ("bootstrap", "security", "sasl", "ssl", "broker")}
    consumer = Consumer({**connection, "group.id": "producer-clock", "enable.auto.commit": False})
    newest = 0
    try:
        for p in consumer.list_topics(topic, timeout=10).topics[topic].partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            consumer.assign([TopicPartition(topic, p, max(low, high - 20))])
            for _ in range(min(20, high - low)):
                msg = consumer.poll(5)
                if msg is None:
                    break
                try:
                    newest = max(newest, int(json.loads(msg.value())["event_time"]))
                except (ValueError, KeyError, TypeError):
                    continue
    finally:
        consumer.close()
    return max(int(time.time() * 1000), newest + 60_000)


def make_bad_message(rng: random.Random) -> bytes:
    kind = rng.choice(["not_json", "missing_order_id", "negative_amount", "unknown_status", "wrong_type"])
    base = {"event_id": str(uuid.uuid4()), "order_id": rng.randint(1, 10**6), "customer_id": 1,
            "restaurant_id": 100, "city": "Pune", "amount": 250.0, "status": "PLACED",
            "placed_at": int(time.time() * 1000), "event_time": int(time.time() * 1000)}
    if kind == "not_json":
        return b'{"order_id": 12, "amount": '
    if kind == "missing_order_id":
        base.pop("order_id")
    elif kind == "negative_amount":
        base["amount"] = -40.0
    elif kind == "unknown_status":
        base["status"] = "TELEPORTED"
    elif kind == "wrong_type":
        base["amount"] = "two hundred"
    return json.dumps(base).encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--orders", type=int, default=300, help="orders to create")
    parser.add_argument("--orders-per-minute", type=float, default=40, help="simulated order rate")
    parser.add_argument("--speed", type=float, default=60,
                        help="simulated seconds per real second (60: 1 s = 1 min). 0 = no waiting")
    parser.add_argument("--cancel-ratio", type=float, default=0.05)
    parser.add_argument("--stuck-ratio", type=float, default=0.05)
    parser.add_argument("--dup-ratio", type=float, default=0.0)
    parser.add_argument("--bad-ratio", type=float, default=0.0)
    parser.add_argument("--late-ratio", type=float, default=0.0)
    parser.add_argument("--start-ms", type=int, default=None,
                        help="simulated start time (default: after the newest event in the topic, or now)")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    topic = os.getenv("KAFKA_TOPIC", "order-events")
    rng = random.Random(args.seed)
    config = build_config()
    start_ms = args.start_ms or resume_clock(config, topic)
    sim = OrderSimulator(rng, start_ms, args.orders_per_minute, args.cancel_ratio, args.stuck_ratio,
                         args.late_ratio)
    producer = Producer(config)
    stats = {"sent": 0, "delivered": 0, "failed": 0, "orders": 0, "dups": 0, "bad": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"DELIVERY FAILED key={msg.key()}: {err}", file=sys.stderr)
        else:
            stats["delivered"] += 1

    def send(key: bytes, value: bytes):
        while True:
            try:
                producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        stats["sent"] += 1
        producer.poll(0)

    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))

    real_start = time.perf_counter()
    orders_left = args.orders
    last_good = None
    max_event_time = start_ms
    while not stop["now"]:
        item, orders_left = sim.next_event(orders_left)
        if item is None:
            break
        send_at, event = item
        if args.speed > 0:
            wait = (send_at - start_ms) / 1000 / args.speed - (time.perf_counter() - real_start)
            if wait > 0:
                time.sleep(wait)
        if event["status"] == "PLACED":
            stats["orders"] += 1
        max_event_time = max(max_event_time, event["event_time"])
        key, value = str(event["order_id"]).encode(), json.dumps(event).encode()
        send(key, value)
        last_good = (key, value)
        if rng.random() < args.dup_ratio:
            send(*last_good)
            stats["dups"] += 1
        if rng.random() < args.bad_ratio:
            send(b"bad", make_bad_message(rng))
            stats["bad"] += 1

    remaining = producer.flush(60)
    elapsed = time.perf_counter() - real_start
    stats["late"] = sim.late_events
    print(f"Done. {stats} still_in_queue={remaining}")
    print(f"Simulated {(max_event_time - start_ms) / 60_000:.0f} min in {elapsed:.1f} s | "
          f"delivered {stats['delivered'] / elapsed:,.0f} events/s")
    if stats["failed"] or remaining:
        sys.exit(1)


if __name__ == "__main__":
    main()
