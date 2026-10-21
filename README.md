# Real-Time Food-Delivery Order Tracking: Kafka → Flink → Cassandra

Order events (PLACED → ACCEPTED → PICKED_UP → DELIVERED / CANCELLED) stream through **Kafka**. A stateful **Apache Flink** job (PyFlink) tracks every order in real time, raises an alert when an order misses its 45-minute delivery SLA, and computes per-restaurant metrics per minute. The results land in **Apache Cassandra**, in tables shaped for the app's queries ("where is my order?", "my recent orders", "restaurant dashboard", "late orders in my city").

This is the pattern behind order tracking at Swiggy, Zomato, Uber Eats and DoorDash: a stream processor holds per-order state, and a wide-column store serves millisecond reads to millions of app users.

```text
producer.py ──JSON──► Kafka "order-events" (3 partitions, key = order_id)
                                   │
                    ┌──────────────▼───────────────────────────────────────────┐
                    │ Flink job (parallelism 3, checkpoint every 10 s)          │
                    │  parse + validate ──bad──► Kafka "order-events-dlq"        │
                    │  event-time watermarks (30 s out-of-order allowed)        │
                    │  keyBy(order_id): dedup, furthest status, SLA timer ──────┼──► Kafka "order-alerts" (exactly-once)
                    │  keyBy(restaurant_id): 1-minute event-time windows        │
                    └──────┬─────────────┬──────────────┬──────────────┬────────┘
                           ▼             ▼              ▼              ▼
             orders_by_id  orders_by_customer  late_orders_by_city  restaurant_metrics_by_minute   (Cassandra)
```

## What's in here

| Path | What it is |
|---|---|
| `WALKTHROUGH.md` | **Start here.** Follows one order through every file, with interview angles. |
| `docker-compose.yml` | Whole stack on your laptop: Kafka (KRaft), Cassandra 5, Flink 1.20 JobManager + TaskManager, plus a `tools` container. |
| `flink/Dockerfile` | Flink image with Python, PyFlink and the Kafka + Cassandra connectors. |
| `flink/jobs/order_tracker_job.py` | The Flink job: sources, state, timers, windows, sinks. |
| `flink/jobs/order_logic.py` | Plain-Python business rules (validation, status ordering). Unit tested. |
| `cassandra/schema.cql` | Keyspace and 5 query-first tables. |
| `producer/producer.py` | Simulates a day of orders on a fast simulated clock. Flags for duplicates, bad and late events. |
| `scripts/verify.py` | Recomputes the expected results from Kafka and checks every Cassandra table. |
| `tests/` | Unit tests (`pytest`). |
| `docs/design_decisions.md` | Why Flink, why Cassandra, why each setting. |
| `docs/failure_tests.md` | Crash, duplicate, bad-data and late-data tests, with the results we got. |
| `docs/interview_questions.md` | Scenario questions on this stack, each with "why this over that" and a real-world example. |

## Run it (Windows, macOS or Linux)

You need **Docker Desktop** (give it at least 6 GB of memory in Settings → Resources). Nothing else needs installing; the producer and the checks run in a container too.

### 1. Start the stack
```bash
docker compose up -d --build
```
The first build downloads Flink, PyFlink and the connectors (a few minutes). `kafka-init` creates the topics, `cassandra-init` creates the tables. Check that everything is up:
```bash
docker compose ps -a
```
`kafka`, `cassandra`, `jobmanager` and `taskmanager` should be running; `kafka-init` and `cassandra-init` should be `Exited (0)`. The Flink UI is at http://localhost:8081.

### 2. Submit the Flink job
```bash
docker compose exec jobmanager flink run -d -py /opt/jobs/order_tracker_job.py -pyfs /opt/jobs
```
In the Flink UI the job `food-delivery-order-tracker` should show **RUNNING** with 3 boxes (operator chains).

### 3. Send orders
```bash
docker compose run --rm tools producer/producer.py --orders 300
```
With the default `--speed 60`, one real second is one simulated minute, so this takes about a minute and simulates about an hour of orders. Expect `Done. {'sent': ~1150, 'delivered': ~1150, 'failed': 0, ...}`.

### 4. Look at the results
```bash
docker compose exec cassandra cqlsh
```
```sql
USE food_delivery;
SELECT order_id, status, amount, sla_breached FROM orders_by_id LIMIT 5;
SELECT * FROM orders_by_customer WHERE customer_id = <a customer_id from above> ;
SELECT * FROM late_orders_by_city WHERE city = 'Pune' AND day = '<today, UTC, YYYY-MM-DD>';
SELECT * FROM restaurant_metrics_by_minute WHERE restaurant_id = 120 AND day = '<same day>' LIMIT 10;
```
Every query names the full partition key. That's the rule in Cassandra: one query, one partition.

### 5. Check everything automatically
```bash
docker compose run --rm tools scripts/verify.py
```
It reads every message from Kafka, works out what Cassandra should contain, and compares. Expect `All checks passed.`

### 6. Stop
```bash
docker compose down        # keeps data
docker compose down -v     # deletes Kafka, Cassandra and checkpoint data too
```

## What was tested (in a sandbox, not on your laptop)

Same compose file, same images (Flink 1.20.1, PyFlink 1.20.1, Kafka 3.9.1, Cassandra 5.0):
- Clean run, 300 orders: 300 orders in both order tables, 801 restaurant-minute windows exactly right, SLA alerts as expected. `verify.py`: all checks passed.
- Failure run, 1,000 more orders with 5% duplicates, 2% bad and 2% late (10 minutes): 198 duplicates dropped, 92 bad messages in the dead-letter topic, 67 too-late events in `late_events_by_day`. All checks passed.
- Killed the TaskManager in the middle of a run and restarted it: the job restored checkpoint 21, replayed from Kafka, and the checks still passed. `order-alerts` had 123 alerts in Kafka and 123 in Cassandra, so no alert was lost or sent twice.
- 8 unit tests pass.

Not tested: the managed-Kafka (Aiven SASL_SSL) settings, and sustained high load. In a burst of 76,000 events the Python window operator became the bottleneck (see `docs/design_decisions.md`, "Known limits").

## Using Aiven Kafka instead of the local one (optional, untested)
Set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD` and `KAFKA_CA_LOCATION` (path to the CA file inside the containers) in the `environment` of `jobmanager`, `taskmanager` and `tools`. Keep the values in a git-ignored `.env` file (see `.env.example`), never in the compose file. Create the three topics in the Aiven console.

## Troubleshooting
| Symptom | Cause and fix |
|---|---|
| `jobmanager` never starts | `kafka-init` or `cassandra-init` failed. Run `docker compose logs kafka-init cassandra-init`. Cassandra needs 1 to 2 minutes on first start. |
| Job keeps restarting | Open the Flink UI → the job → Exceptions. Often Cassandra isn't reachable yet; it retries on its own. |
| Nothing in `late_orders_by_city` or the newest windows | Timers and windows fire when the **watermark** passes them, and the watermark only moves when newer events arrive. Send more orders and they'll appear. |
| A second producer run lands as "late" | The producer continues its simulated clock after the newest event already in Kafka. If you pass `--start-ms`, use a time after the previous run. |
| Docker Desktop runs out of memory | Raise the memory limit to at least 6 GB, or lower `taskmanager.memory.process.size` in `docker-compose.yml`. |
