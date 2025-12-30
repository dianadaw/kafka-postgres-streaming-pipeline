import json
from collections import defaultdict
from datetime import timezone
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from psycopg2.extras import Json, execute_values
from common.schemas import Event

BOOTSTRAP = "localhost:9092"
TOPIC = "events"
DLQ_TOPIC = "events_dlq"
GROUP_ID = "events_to_postgres"

DB = dict(host="localhost", port=5432, dbname="app", user="app", password="app")

INSERT_RAW_TMPL = """
INSERT INTO raw_events (event_id, event_time, user_id, event_type, payload)
VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""

INSERT_DLQ = """
INSERT INTO dlq_events (error, raw_payload)
VALUES (%s, %s);
"""

UPSERT_AGG_TMPL = """
INSERT INTO agg_events_minute (minute_ts, event_type, cnt)
VALUES %s
ON CONFLICT (minute_ts, event_type)
DO UPDATE SET cnt = agg_events_minute.cnt + EXCLUDED.cnt;
"""

def minute_bucket(dt):
    dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0)

def main(batch_size: int = 200, max_wait_s: float = 1.0):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        max_poll_records=batch_size,
        consumer_timeout_ms=int(max_wait_s * 1000),
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    buffer_valid = []
    buffer_invalid = []

    def flush():
        nonlocal buffer_valid, buffer_invalid

        if not buffer_valid and not buffer_invalid:
            return

        try:
            with conn.cursor() as cur:
                for err, payload in buffer_invalid:
                    cur.execute(INSERT_DLQ, (err, Json(payload)))
                    dlq_producer.send(DLQ_TOPIC, value={"error": err, "raw_payload": payload})

                if buffer_valid:
                    rows = [
                        (ev.event_id, ev.event_time, ev.user_id, ev.event_type, Json(ev.payload))
                        for ev in buffer_valid
                    ]
                    execute_values(cur, INSERT_RAW_TMPL, rows, page_size=500)

                    counts = defaultdict(int)
                    for ev in buffer_valid:
                        key = (minute_bucket(ev.event_time), ev.event_type)
                        counts[key] += 1

                    agg_rows = [(k[0], k[1], v) for k, v in counts.items()]
                    execute_values(cur, UPSERT_AGG_TMPL, agg_rows, page_size=500)

                conn.commit()

            consumer.commit()

        except Exception as e:
            conn.rollback()
            print("flush error:", repr(e))

        finally:
            buffer_valid = []
            buffer_invalid = []

    print("Consumer started. Reading from Kafka topic:", TOPIC)

    try:
        while True:
            any_msg = False
            for msg in consumer:
                any_msg = True
                try:
                    ev = Event.model_validate(msg.value)
                    buffer_valid.append(ev)
                except Exception as e:
                    buffer_invalid.append((str(e), msg.value))

                if len(buffer_valid) + len(buffer_invalid) >= batch_size:
                    flush()

            if not any_msg:
                flush()

    finally:
        try:
            flush()
        except Exception:
            pass
        conn.close()
        consumer.close()
        dlq_producer.close()

if __name__ == "__main__":
    main()
