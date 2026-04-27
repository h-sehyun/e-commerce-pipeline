include .env
export

# ── Kafka ─────────────────────────────────────────────
kafka-up:
	cd kafka && docker-compose up -d

kafka-down:
	cd kafka && docker-compose down

kafka-ps:
	cd kafka && docker-compose ps

kafka-logs:
	cd kafka && docker-compose logs -f kafka

# ── Airflow (2단계) ───────────────────────────────────
airflow-up:
	cd airflow && docker-compose up -d

airflow-down:
	cd airflow && docker-compose down

# ── 데이터 준비 ────────────────────────────────────────
download:
	python src/download_data.py --mode sample

users:
	python src/generate_users.py

# ── 스트리밍 ──────────────────────────────────────────
consumer:
	python kafka/consumer.py

producer:
	python kafka/producer.py

# ── 전체 ──────────────────────────────────────────────
all-up: kafka-up airflow-up

all-down: kafka-down airflow-down

.PHONY: kafka-up kafka-down kafka-ps kafka-logs \
        airflow-up airflow-down \
        download users consumer producer \
        all-up all-down