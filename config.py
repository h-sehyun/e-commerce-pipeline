"""
공통 설정 파일 (루트)
kafka/, airflow/ 양쪽에서 모두 참조
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 루트의 .env 로드 (어느 디렉터리에서 실행해도 찾을 수 있도록)
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

# ── EC2 ───────────────────────────────────────────────
EC2_PUBLIC_IP = os.getenv("EC2_PUBLIC_IP", "localhost")

# ── AWS ───────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_BUCKET  = os.getenv("S3_BUCKET", "")

# ── S3 경로 규칙 ───────────────────────────────────────
# raw/otto/{event_type}/YYYY/MM/DD/HH/{uuid}.jsonl
# airflow DAG의 raw read 경로와 1:1 대응
S3_PREFIX = "raw/otto"

# ── Kafka ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = {
    "click": "otto.clicks",
    "cart":  "otto.carts",
    "order": "otto.orders",
}

# ── 데이터 경로 ────────────────────────────────────────
DATA_DIR   = ROOT_DIR / "data"
USERS_PATH = DATA_DIR / "users.json"
TRAIN_PATH = DATA_DIR / "raw" / "train.jsonl"

# ── Producer ──────────────────────────────────────────
TOTAL_EVENTS   = int(os.getenv("TOTAL_EVENTS", "10000"))   # 0 = 전체
DELAY_SECONDS  = float(os.getenv("DELAY_SECONDS", "0.005"))

# ── 유저 ──────────────────────────────────────────────
NUM_USERS = int(os.getenv("NUM_USERS", "50000"))

# ── Consumer / S3 Writer ──────────────────────────────
CONSUMER_GROUP_ID      = "otto-s3-writer"
FLUSH_INTERVAL_SECONDS = 60    # 최대 N초마다 S3 flush
FLUSH_BATCH_SIZE       = 500   # N개 쌓이면 즉시 flush