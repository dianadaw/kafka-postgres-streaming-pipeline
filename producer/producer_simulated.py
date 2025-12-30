import json
import time
import uuid
import random
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9092"
TOPIC = "events"

EVENT_TYPES = ["page_view", "click", "purchase"]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_event():
    user_id = f"user_{random.randint(1, 50)}"
    event_type = random.choice(EVENT_TYPES)
    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_time": now_iso(),
        "user_id": user_id,
        "event_type": event_type,
        "payload": {"value": random.randint(1, 100)}
    }

def make_bad_event():
    # Missing required fields on purpose
    return {"oops": True, "event_time": now_iso()}

def main(rate_per_sec: float = 10.0, duplicate_rate: float = 0.03, bad_rate: float = 0.02, out_of_order_rate: float = 0.03):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=20,
    )

    last_event = None
    sleep_s = 1.0 / max(rate_per_sec, 0.1)

    while True:
        r = random.random()

        if r < bad_rate:
            ev = make_bad_event()
            key = "bad"
        elif last_event and r < bad_rate + duplicate_rate:
            ev = last_event  # duplicate
            key = ev.get("user_id", "dup")
        else:
            ev = make_event()
            # sometimes set an earlier timestamp (out-of-order)
            if random.random() < out_of_order_rate:
                ts = datetime.now(timezone.utc) - timedelta(seconds=random.randint(5, 120))
                ev["event_time"] = ts.isoformat()
            key = ev["user_id"]
            last_event = ev

        producer.send(TOPIC, key=key, value=ev)
        producer.flush()

        print("sent", ev.get("event_type"), ev.get("user_id"), ev.get("event_id"), "bad" if "event_id" not in ev else "")
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
