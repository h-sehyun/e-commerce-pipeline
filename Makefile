include .env
export

# ── Kafka ─────────────────────────────────────────────
kafka-up:
	docker-compose up -d zookeeper kafka kafka-ui

kafka-down:
	docker-compose stop zookeeper kafka kafka-ui

kafka-ps:
	docker-compose ps

kafka-logs:
	docker-compose logs -f kafka

# ── Spark + PostgreSQL ─────────────────────────────────
spark-up:
	docker-compose up -d spark-master spark-worker-1 postgres

spark-down:
	docker-compose stop spark-master spark-worker-1 postgres

spark-logs:
	docker-compose logs -f spark-master

# ── Airflow (3단계) ────────────────────────────────────
airflow-up:
	docker-compose up -d airflow

airflow-down:
	docker-compose stop airflow

# ── 데이터 준비 ────────────────────────────────────────
users:
	python src/generate_users.py
	

# ── 스트리밍 ──────────────────────────────────────────
consumer:
	python kafka/consumer.py

producer:
	python kafka/producer.py

# ── 전체 ──────────────────────────────────────────────
all-up: kafka-up spark-up

all-down: kafka-down spark-down

.PHONY: kafka-up kafka-down kafka-ps kafka-logs \
        spark-up spark-down spark-logs \
        airflow-up airflow-down \
        users consumer producer \
        all-up all-down