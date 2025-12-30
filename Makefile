up:
	docker compose up -d

down:
	docker compose down

consumer:
	. .venv/bin/activate && python -m consumer.consumer

producer:
	. .venv/bin/activate && python -m producer.producer_simulated

psql:
	docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app

stats:
	docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app -c "select count(*) as raw_events from raw_events;"
	docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app -c "select count(*) as dlq_events from dlq_events;"
	docker exec -it kafka-streaming-demo-postgres-1 psql -U app -d app -c "select * from agg_events_minute order by minute_ts desc limit 10;"
