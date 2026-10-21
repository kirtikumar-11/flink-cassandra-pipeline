"""Real-time food-delivery order tracking: Kafka -> Flink (PyFlink) -> Cassandra (+ Kafka alerts).

What the job does, per event:
  1. Parse + validate the JSON. Bad messages go to the Kafka topic order-events-dlq with a reason.
  2. Use the event's own time (event_time) and a watermark, so out-of-order data is handled.
  3. Key by order_id. Per order, Flink keeps state: the furthest status and the event_ids already seen.
     - duplicates are dropped
     - a status change is upserted into Cassandra (orders_by_id, orders_by_customer)
     - an event-time timer fires SLA_MINUTES after the order was placed; if the order is still
       not delivered or cancelled, an alert goes to Cassandra (late_orders_by_city) and Kafka (order-alerts)
  4. Per restaurant, 1-minute event-time windows count orders and revenue (restaurant_metrics_by_minute).
     Events later than the allowed lateness go to late_events_by_day instead of being lost silently.

Every Cassandra write is an upsert on a primary key built only from event data (never "now()"),
so replaying events after a failure rewrites the same rows: effectively exactly-once.

Submit with:  flink run -py /opt/jobs/order_tracker_job.py  (see README.md)
"""

import json
import os

from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import CheckpointingMode, OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

from order_logic import TERMINAL, day_bucket, merge_status, money, parse_event, to_utc

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SOURCE_TOPIC = os.getenv("KAFKA_TOPIC", "order-events")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "order-alerts")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "order-events-dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-tracker")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "food_delivery")
SLA_MINUTES = int(os.getenv("SLA_MINUTES", "45"))
OUT_OF_ORDER_SECONDS = int(os.getenv("OUT_OF_ORDER_SECONDS", "30"))
ALLOWED_LATENESS_SECONDS = int(os.getenv("ALLOWED_LATENESS_SECONDS", "60"))
CHECKPOINT_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000"))


def kafka_security_properties() -> dict:
    """Empty for the local Docker Kafka. For a managed Kafka (e.g. Aiven SASL_SSL) set the env vars.
    The Kafka client inside Flink's connector jar is shaded, hence the long login-module class name."""
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    if protocol == "PLAINTEXT":
        return {}
    props = {"security.protocol": protocol}
    if protocol.startswith("SASL"):
        user, password = os.environ["KAFKA_USERNAME"], os.environ["KAFKA_PASSWORD"]
        props["sasl.mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
        props["sasl.jaas.config"] = (
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule "
            f'required username="{user}" password="{password}";'
        )
    if os.getenv("KAFKA_CA_LOCATION"):
        props["ssl.truststore.type"] = "PEM"
        props["ssl.truststore.location"] = os.environ["KAFKA_CA_LOCATION"]
    return props


ORDER_ROW = Types.ROW([Types.LONG(), Types.STRING(), Types.LONG(), Types.LONG(), Types.STRING(),
                       Types.BIG_DEC(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP()])
CUSTOMER_ROW = Types.ROW([Types.LONG(), Types.SQL_TIMESTAMP(), Types.LONG(), Types.STRING(),
                          Types.LONG(), Types.BIG_DEC()])
ALERT_ROW = Types.ROW([Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.LONG(),
                       Types.LONG(), Types.STRING(), Types.SQL_TIMESTAMP()])
BREACH_FLAG_ROW = Types.ROW([Types.LONG(), Types.BOOLEAN()])
METRICS_ROW = Types.ROW([Types.LONG(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.LONG(),
                         Types.BIG_DEC(), Types.LONG(), Types.LONG()])
LATE_ROW = Types.ROW([Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.LONG(),
                      Types.LONG(), Types.STRING()])

ORDERS_BY_ID = (f"INSERT INTO {KEYSPACE}.orders_by_id (order_id, status, customer_id, restaurant_id, "
                "city, amount, placed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);")
ORDERS_BY_CUSTOMER = (f"INSERT INTO {KEYSPACE}.orders_by_customer (customer_id, placed_at, order_id, "
                      "status, restaurant_id, amount) VALUES (?, ?, ?, ?, ?, ?);")
LATE_ORDERS = (f"INSERT INTO {KEYSPACE}.late_orders_by_city (city, day, detected_at, order_id, "
               "restaurant_id, last_status, placed_at) VALUES (?, ?, ?, ?, ?, ?, ?);")
BREACH_FLAG = f"INSERT INTO {KEYSPACE}.orders_by_id (order_id, sla_breached) VALUES (?, ?);"
METRICS = (f"INSERT INTO {KEYSPACE}.restaurant_metrics_by_minute (restaurant_id, day, window_start, "
           "placed_orders, revenue, delivered_orders, cancelled_orders) VALUES (?, ?, ?, ?, ?, ?, ?);")
LATE_EVENTS = (f"INSERT INTO {KEYSPACE}.late_events_by_day (day, event_time, event_id, order_id, "
               "restaurant_id, status) VALUES (?, ?, ?, ?, ?, ?);")

BAD_TAG = OutputTag("bad-events", Types.STRING())
ORDER_TAG = OutputTag("order-updates", ORDER_ROW)
ALERT_TAG = OutputTag("sla-alerts", ALERT_ROW)
LATE_TAG = OutputTag("late-events", Types.PICKLED_BYTE_ARRAY())


class ParseEvents(ProcessFunction):
    """Valid events continue as dicts; bad ones leave through a side output with the reason."""

    def process_element(self, raw, ctx):
        event, reason = parse_event(raw)
        if event is None:
            yield BAD_TAG, json.dumps({"reason": reason, "raw": raw})
        else:
            yield event


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value["event_time"]


class OrderTracker(KeyedProcessFunction):
    """One logical instance per order_id. Flink stores the state below in its state backend and
    snapshots it in every checkpoint, so after a crash it continues with exactly this state."""

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("order_state", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(StateTtlConfig.new_builder(Time.days(2)).build())
        self.state = runtime_context.get_state(descriptor)

    def process_element(self, e, ctx):
        state = self.state.value()
        if state is None:
            ctx.timer_service().register_event_time_timer(e["placed_at"] + SLA_MINUTES * 60_000)
            state = {"status": None, "order": e, "seen": []}
        elif e["event_id"] in state["seen"]:
            return
        state["seen"].append(e["event_id"])
        yield e

        new = merge_status(state["status"], e["status"])
        if new != state["status"]:
            state["status"] = new
            yield ORDER_TAG, Row(e["order_id"], new, e["customer_id"], e["restaurant_id"], e["city"],
                                 money(e["amount"]), to_utc(e["placed_at"]), to_utc(e["event_time"]))
        self.state.update(state)

    def on_timer(self, timestamp, ctx):
        state = self.state.value()
        if state is None or state["status"] in TERMINAL:
            return
        o = state["order"]
        yield ALERT_TAG, Row(o["city"], day_bucket(timestamp), to_utc(timestamp), o["order_id"],
                             o["restaurant_id"], state["status"], to_utc(o["placed_at"]))


class CountOrders(AggregateFunction):
    """Accumulator: [placed, revenue_of_placed, delivered, cancelled]."""

    def create_accumulator(self):
        return [0, 0.0, 0, 0]

    def add(self, e, acc):
        if e["status"] == "PLACED":
            acc[0] += 1
            acc[1] += e["amount"]
        elif e["status"] == "DELIVERED":
            acc[2] += 1
        elif e["status"] == "CANCELLED":
            acc[3] += 1
        return acc

    def get_result(self, acc):
        return acc

    def merge(self, a, b):
        return [a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]]


class ToMetricsRow(ProcessWindowFunction):
    def process(self, restaurant_id, context, totals):
        placed, revenue, delivered, cancelled = next(iter(totals))
        start = context.window().start
        yield Row(restaurant_id, day_bucket(start), to_utc(start), placed, money(revenue), delivered,
                  cancelled)


def cassandra_sink(stream, query, name):
    (CassandraSink.add_sink(stream)
     .set_query(query)
     .set_host(CASSANDRA_HOST, CASSANDRA_PORT)
     .build()
     .name(name)
     .uid(name))


def build(env: StreamExecutionEnvironment):
    security = kafka_security_properties()

    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
        .set_value_only_deserializer(SimpleStringSchema())
    )
    for key, value in security.items():
        source_builder.set_property(key, value)

    raw = env.from_source(source_builder.build(), WatermarkStrategy.no_watermarks(), "kafka: order-events")
    parsed = raw.process(ParseEvents(), output_type=Types.PICKLED_BYTE_ARRAY()).name("parse + validate").uid("parse")

    def kafka_sink(topic, guarantee, prefix=None):
        builder = (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(topic)
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .set_delivery_guarantee(guarantee)
        )
        if prefix:
            builder.set_transactional_id_prefix(prefix).set_property("transaction.timeout.ms", "600000")
        for key, value in security.items():
            builder.set_property(key, value)
        return builder.build()

    parsed.get_side_output(BAD_TAG).sink_to(kafka_sink(DLQ_TOPIC, DeliveryGuarantee.AT_LEAST_ONCE)) \
        .name("kafka: dead-letter queue").uid("dlq-sink")

    events = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(OUT_OF_ORDER_SECONDS))
        .with_timestamp_assigner(EventTime())
    ).name("event-time watermarks").uid("watermarks")

    unique_events = (
        events.key_by(lambda e: e["order_id"], key_type=Types.LONG())
        .process(OrderTracker(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("order state: dedup + status + SLA timer").uid("order-tracker")
    )

    order_updates = unique_events.get_side_output(ORDER_TAG)
    cassandra_sink(order_updates, ORDERS_BY_ID, "cassandra: orders_by_id")
    by_customer = order_updates.map(lambda r: Row(r[2], r[6], r[0], r[1], r[3], r[5]), output_type=CUSTOMER_ROW)
    cassandra_sink(by_customer, ORDERS_BY_CUSTOMER, "cassandra: orders_by_customer")

    alerts = unique_events.get_side_output(ALERT_TAG)
    cassandra_sink(alerts, LATE_ORDERS, "cassandra: late_orders_by_city")
    cassandra_sink(alerts.map(lambda r: Row(r[3], True), output_type=BREACH_FLAG_ROW), BREACH_FLAG,
                   "cassandra: orders_by_id.sla_breached")
    alert_json = alerts.map(
        lambda r: json.dumps({"type": "SLA_BREACH", "order_id": r[3], "city": r[0], "restaurant_id": r[4],
                              "last_status": r[5], "placed_at": r[6].isoformat(),
                              "detected_at": r[2].isoformat()}),
        output_type=Types.STRING())
    alert_json.sink_to(kafka_sink(ALERT_TOPIC, DeliveryGuarantee.EXACTLY_ONCE, prefix="order-alerts")) \
        .name("kafka: order-alerts (exactly-once)").uid("alert-sink")

    metrics = (
        unique_events.key_by(lambda e: e["restaurant_id"], key_type=Types.LONG())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(ALLOWED_LATENESS_SECONDS * 1000)
        .side_output_late_data(LATE_TAG)
        .aggregate(CountOrders(), window_function=ToMetricsRow(),
                   accumulator_type=Types.PICKLED_BYTE_ARRAY(), output_type=METRICS_ROW)
        .name("1-minute restaurant windows").uid("restaurant-windows")
    )
    cassandra_sink(metrics, METRICS, "cassandra: restaurant_metrics_by_minute")

    too_late = metrics.get_side_output(LATE_TAG).map(
        lambda e: Row(day_bucket(e["event_time"]), to_utc(e["event_time"]), e["event_id"], e["order_id"],
                      e["restaurant_id"], e["status"]),
        output_type=LATE_ROW)
    cassandra_sink(too_late, LATE_EVENTS, "cassandra: late_events_by_day")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(CHECKPOINT_MS, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.add_python_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "order_logic.py"))
    build(env)
    env.execute("food-delivery-order-tracker")


if __name__ == "__main__":
    main()
