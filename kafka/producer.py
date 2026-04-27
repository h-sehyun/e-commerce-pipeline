"""
OTTO Commerce Kafka Producer (실제 데이터 기반)

흐름:
  1. data/users.json  로드 (Faker 고정 유저풀)
  2. data/raw/train.jsonl 읽기 (OTTO 실제 이벤트)
  3. 각 이벤트에 유저 랜덤 배정 (session_id → user 고정 매핑)
  4. Kafka 토픽으로 전송
     - otto.clicks / otto.carts / otto.orders

OTTO 원본 스키마:
  {"session": int, "events": [{"aid": int, "ts": int, "type": "clicks|carts|orders"}]}
"""
import sys
from pathlib import Path

# 루트 경로를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent))

import json
import random
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

import config

# ── 경로 ──────────────────────────────────────────────
USERS_PATH = Path("data/users.json")
TRAIN_PATH = Path("data/raw/otto-recsys-train.jsonl")


# ── 유저 풀 로드 ───────────────────────────────────────
def load_users() -> list[dict]:
    if not USERS_PATH.exists():
        raise FileNotFoundError(
            f"❌ {USERS_PATH} 없음 — 먼저 실행: python generate_users.py"
        )
    with open(USERS_PATH, encoding="utf-8") as f:
        users = json.load(f)
    print(f"✅ 유저 로드: {len(users):,}명")
    return users


# ── OTTO session_id → Faker user 고정 매핑 ────────────
def get_user_for_session(
    session_id: int,
    session_map: dict,
    user_pool: list[dict],
) -> dict:
    """
    session_id를 처음 만날 때 유저를 랜덤 배정, 이후 동일 유저 사용.
    같은 세션 내 이벤트는 항상 같은 유저 → 세션 일관성 보장
    """
    if session_id not in session_map:
        session_map[session_id] = random.choice(user_pool)
    return session_map[session_id]


# ── 이벤트 변환: OTTO 원본 → Kafka 메시지 ─────────────
TYPE_TO_TOPIC = {
    "clicks": config.TOPICS["click"],
    "carts":  config.TOPICS["cart"],
    "orders": config.TOPICS["order"],
}

TYPE_NORMALIZE = {
    "clicks": "click",
    "carts":  "cart",
    "orders": "order",
}


def build_event(session_id: int, raw_event: dict, user: dict) -> tuple[str, dict]:
    """OTTO 원본 이벤트 + Faker 유저 → Kafka 전송용 dict"""
    etype = raw_event["type"]
    topic = TYPE_TO_TOPIC[etype]

    event = {
        # OTTO 원본 필드
        "session_id":      session_id,
        "aid":             raw_event["aid"],
        "ts":              raw_event["ts"],
        "event_type":      TYPE_NORMALIZE[etype],
        # Faker 유저 정보
        "user_id":         user["user_id"],
        "gender":          user["gender"],
        "age_group":       user["age_group"],
        "region":          user["region"],
        "membership":      user["membership"],
        "platform":        user["preferred_device"],
    }
    return topic, event


# ── Producer 초기화 ────────────────────────────────────
def create_producer(retries: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=32768,
                compression_type="gzip",
            )
            print(f"✅ Kafka 연결: {config.KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            wait = attempt * 3
            print(f"⚠️  연결 실패 ({attempt}/{retries}) — {wait}초 후 재시도...")
            time.sleep(wait)
    raise RuntimeError("❌ Kafka 연결 실패")


# ── 메인 ──────────────────────────────────────────────
def main():
    if not TRAIN_PATH.exists():
        raise FileNotFoundError(
            f"❌ {TRAIN_PATH} 없음 — 먼저 실행: python download_data.py"
        )

    user_pool   = load_users()
    session_map = {}
    producer    = create_producer()

    counts = {"click": 0, "cart": 0, "order": 0}
    total  = 0
    start  = time.time()

    print("\n" + "=" * 65)
    print("OTTO Producer 시작")
    print(f"  데이터   : {TRAIN_PATH}")
    print(f"  유저풀   : {len(user_pool):,}명")
    print(f"  딜레이   : {config.DELAY_SECONDS}s/이벤트")
    print(f"  최대 전송: {config.TOTAL_EVENTS:,}건  (0이면 전체)")
    print("=" * 65)

    def on_error(exc):
        print(f"  ❌ 전송 실패: {exc}")

    with open(TRAIN_PATH, encoding="utf-8") as f:
        for line in f:
            session_data = json.loads(line)
            session_id   = session_data["session"]
            user         = get_user_for_session(session_id, session_map, user_pool)

            for raw_ev in session_data["events"]:
                if config.TOTAL_EVENTS > 0 and total >= config.TOTAL_EVENTS:
                    break

                topic, event = build_event(session_id, raw_ev, user)
                etype = event["event_type"]

                try:
                    future = producer.send(topic, key=user["user_id"], value=event)
                    future.add_errback(on_error)
                    counts[etype] += 1
                    total += 1
                except KafkaError as e:
                    print(f"❌ KafkaError: {e}")

                if total % 1000 == 0:
                    elapsed = time.time() - start
                    rps = total / elapsed if elapsed > 0 else 0
                    print(
                        f"[{total:>8,}] "
                        f"clicks={counts['click']:,}  "
                        f"carts={counts['cart']:,}  "
                        f"orders={counts['order']:,}  "
                        f"({rps:.0f} ev/s)"
                    )

                time.sleep(config.DELAY_SECONDS)

            if config.TOTAL_EVENTS > 0 and total >= config.TOTAL_EVENTS:
                break

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    print("\n" + "=" * 65)
    print("전송 완료")
    print(f"  clicks : {counts['click']:,}")
    print(f"  carts  : {counts['cart']:,}")
    print(f"  orders : {counts['order']:,}")
    print(f"  합계   : {total:,}건  ({elapsed:.1f}s / {total/elapsed:.0f} ev/s)")
    print(f"  세션-유저 매핑: {len(session_map):,}건")
    print("=" * 65)


if __name__ == "__main__":
    main()