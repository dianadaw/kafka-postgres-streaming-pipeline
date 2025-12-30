CREATE TABLE IF NOT EXISTS raw_events (
  event_id TEXT PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL,
  user_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS agg_events_minute (
  minute_ts TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  cnt BIGINT NOT NULL,
  PRIMARY KEY (minute_ts, event_type)
);

CREATE TABLE IF NOT EXISTS dlq_events (
  dlq_id BIGSERIAL PRIMARY KEY,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  error TEXT NOT NULL,
  raw_payload JSONB NOT NULL
);
