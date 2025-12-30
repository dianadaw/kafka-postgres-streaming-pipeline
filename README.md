# Kafka Streaming Demo (Producer → Kafka → Consumer → Postgres)

A small streaming pipeline that demonstrates core data engineering patterns:
- Kafka topics with partitioning and consumer groups
- At-least-once processing with manual offset commits
- Idempotent ingestion (dedup) into Postgres using `event_id` as primary key
- Data validation + Dead Letter Queue (DLQ) for invalid events
- Near-real-time aggregate table (events per minute per event type)

## Architecture
Producer (simulated events) → Kafka topic `events` → Consumer → Postgres:
- `raw_events`: normalized raw table
- `agg_events_minute`: mart/aggregate table
Invalid events are sent to Kafka topic `events_dlq` and stored in Postgres table `dlq_events`.

## Tech
- Python 3.11
- Kafka (Docker)
- Postgres (Docker)
- kafka-python + psycopg2 + pydantic

## Run locally

### 1) Start infrastructure
```bash
docker compose up -d

Kafka UI: http://localhost:8080

2) Create Python environment
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

3) Start consumer
source .venv/bin/activate
python -m consumer.consumer

4) Start producer (new terminal)
source .venv/bin/activate
python -m producer.producer_simulated

Verify results
docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app -c "select count(*) from raw_events;"
docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app -c "select count(*) from dlq_events;"
docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app -c "select * from agg_events_minute order by minute_ts desc limit 10;"

Notes on reliability

Processing is at-least-once (offsets committed only after successful DB commit).

Duplicate deliveries are handled via idempotent inserts (event_id primary key + ON CONFLICT DO NOTHING).


## 3) Städa bort “PYTHONPATH=.” från instruktioner
Du kör nu med `python -m ...` vilket är bäst. Se till att du har dessa filer (de kan vara tomma):
```bash
touch common/__init__.py consumer/__init__.py producer/__init__.py
