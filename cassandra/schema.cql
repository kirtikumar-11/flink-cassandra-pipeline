-- Cassandra is modelled QUERY FIRST: one table per question the app asks, with the data
-- duplicated across tables. There are no joins; each read hits exactly one partition.

-- Local single node. In production: NetworkTopologyStrategy with replication 3 per data center.
CREATE KEYSPACE IF NOT EXISTS food_delivery
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE food_delivery;

-- Q1 "Where is my order?"  ->  SELECT * FROM orders_by_id WHERE order_id = ?
CREATE TABLE IF NOT EXISTS orders_by_id (
  order_id       bigint PRIMARY KEY,
  status         text,
  customer_id    bigint,
  restaurant_id  bigint,
  city           text,
  amount         decimal,
  placed_at      timestamp,
  updated_at     timestamp,   -- event time of the event that set this status
  sla_breached   boolean      -- set by the SLA alert (partial upsert of one column)
);

-- Q2 "Show my recent orders"  ->  SELECT * FROM orders_by_customer WHERE customer_id = ? LIMIT 20
CREATE TABLE IF NOT EXISTS orders_by_customer (
  customer_id    bigint,
  placed_at      timestamp,
  order_id       bigint,
  status         text,
  restaurant_id  bigint,
  amount         decimal,
  PRIMARY KEY ((customer_id), placed_at, order_id)
) WITH CLUSTERING ORDER BY (placed_at DESC, order_id ASC)
  AND default_time_to_live = 7776000;  -- 90 days, then Cassandra deletes it automatically

-- Q3 "Orders per minute and revenue for restaurant X today"
--     ->  SELECT * FROM restaurant_metrics_by_minute WHERE restaurant_id = ? AND day = ?
-- (restaurant_id, day) is the partition key so one restaurant's partition can't grow forever.
CREATE TABLE IF NOT EXISTS restaurant_metrics_by_minute (
  restaurant_id    bigint,
  day              text,
  window_start     timestamp,
  placed_orders    bigint,
  revenue          decimal,
  delivered_orders bigint,
  cancelled_orders bigint,
  PRIMARY KEY ((restaurant_id, day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Q4 Ops dashboard: "Orders that breached the delivery SLA in Bengaluru today"
--     ->  SELECT * FROM late_orders_by_city WHERE city = ? AND day = ?
CREATE TABLE IF NOT EXISTS late_orders_by_city (
  city           text,
  day            text,
  detected_at    timestamp,
  order_id       bigint,
  restaurant_id  bigint,
  last_status    text,
  placed_at      timestamp,
  PRIMARY KEY ((city, day), detected_at, order_id)
) WITH CLUSTERING ORDER BY (detected_at DESC, order_id ASC);

-- Q5 Data quality: "Which events arrived too late for the minute windows?"
CREATE TABLE IF NOT EXISTS late_events_by_day (
  day            text,
  event_time     timestamp,
  event_id       text,
  order_id       bigint,
  restaurant_id  bigint,
  status         text,
  PRIMARY KEY ((day), event_time, event_id)
);
