CREATE KEYSPACE IF NOT EXISTS food_delivery
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE food_delivery;

CREATE TABLE IF NOT EXISTS orders_by_id (
  order_id       bigint PRIMARY KEY,
  status         text,
  customer_id    bigint,
  restaurant_id  bigint,
  city           text,
  amount         decimal,
  placed_at      timestamp,
  updated_at     timestamp,
  sla_breached   boolean
);

CREATE TABLE IF NOT EXISTS orders_by_customer (
  customer_id    bigint,
  placed_at      timestamp,
  order_id       bigint,
  status         text,
  restaurant_id  bigint,
  amount         decimal,
  PRIMARY KEY ((customer_id), placed_at, order_id)
) WITH CLUSTERING ORDER BY (placed_at DESC, order_id ASC)
  AND default_time_to_live = 7776000;

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

CREATE TABLE IF NOT EXISTS late_events_by_day (
  day            text,
  event_time     timestamp,
  event_id       text,
  order_id       bigint,
  restaurant_id  bigint,
  status         text,
  PRIMARY KEY ((day), event_time, event_id)
);
