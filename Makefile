up:
	docker compose up -d

down:
	docker compose down

consumer:
	. .venv/bin/activate && python -m consumer.consumer

producer:
	. .venv/bin/activate && python -m producer.producer_simulated

psql:
	docker compose exec postgres psql -U app -d app

stats:
	docker compose exec -T postgres psql -U app -d app -c "select count(*) as raw_events from raw_events;"
	docker compose exec -T postgres psql -U app -d app -c "select count(*) as dlq_events from dlq_events;"
	docker compose exec -T postgres psql -U app -d app -c "select * from agg_events_minute order by minute_ts desc limit 10;"
