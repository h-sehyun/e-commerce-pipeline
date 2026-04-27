"""
OTTO Commerce S3 Consumer
- Kafka 토픽(otto.clicks / otto.carts / otto.orders)을 구독
- 메시지를 버퍼에 쌓고 flush 조건(시간 or 건수) 달성 시 S3에 jsonl로 저장
- S3 경로: s3://{bucket}/raw/otto/{event_type}/YYYY/MM/DD/HH/{uuid}.jsonl
- Airflow DAG이 이 경로를 raw read하는 구조와 맞춰져 있음
"""

import sys
from pathlib import Path

# 루트 경로를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent))

import io
import json
import signal
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import config


# ── S3 클라이언트 ─────────────────────────────────────
s3 = boto3.client("s3", region_name=config.AWS_REGION)


# ── S3 경로 생성 ───────────────────────────────────────
def s3_key(event_type: str) -> str:
    """
    raw/otto/clicks/2024/01/15/09/abc123.jsonl
    Airflow DAG의 raw read 경로와 1:1 대응
    """
    now = datetime.now(timezone.utc)
    return (
        f"{config.S3_PREFIX}/{event_type}/"
        f"{now.year:04d}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/"
        f"{uuid.uuid4().hex}.jsonl"
    )


# ── S3 업로드 ──────────────────────────────────────────
def flush_to_s3(event_type: str, buffer: list[dict]) -> bool:
    if not buffer:
        return True
    key = s3_key(event_type)
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in buffer)
    try:
        s3.put_object(
            Bucket=config.S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        print(f"  📦 S3 업로드: s3://{config.S3_BUCKET}/{key}  ({len(buffer)}건)")
        return True
    except Exception as e:
        print(f"  ❌ S3 업로드 실패 [{event_type}]: {e}")
        return False


# ── Consumer ──────────────────────────────────────────
class OttoS3Consumer:
    def __init__(self):
        self.consumer  = self._create_consumer()
        self.buffers   = defaultdict(list)      # event_type → [dict, ...]
        self.last_flush = defaultdict(float)    # event_type → timestamp
        self.counts    = defaultdict(int)       # 총 S3 업로드 건수
        self._stop     = threading.Event()

        # 주기적 flush 스레드
        self._flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)

    def _create_consumer(self) -> KafkaConsumer:
        topics = list(config.TOPICS.values())
        for attempt in range(1, 6):
            try:
                consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=config.CONSUMER_GROUP_ID,
                    auto_offset_reset="earliest",       # 처음부터 읽기 (재실행 안전)
                    enable_auto_commit=False,            # 수동 커밋 (S3 성공 후 커밋)
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    key_deserializer=lambda k: k.decode("utf-8") if k else None,
                    max_poll_records=200,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                )
                print(f"✅ Kafka Consumer 연결: {config.KAFKA_BOOTSTRAP_SERVERS}")
                print(f"   구독 토픽: {topics}")
                return consumer
            except KafkaError as e:
                wait = attempt * 3
                print(f"⚠️  Consumer 연결 실패 ({attempt}/5) — {wait}초 후 재시도: {e}")
                time.sleep(wait)
        raise RuntimeError("❌ Kafka Consumer 연결 실패")

    def _should_flush(self, event_type: str) -> bool:
        size_ok = len(self.buffers[event_type]) >= config.FLUSH_BATCH_SIZE
        time_ok = (time.time() - self.last_flush[event_type]) >= config.FLUSH_INTERVAL_SECONDS
        return size_ok or (time_ok and len(self.buffers[event_type]) > 0)

    def _do_flush(self, event_type: str):
        buf = self.buffers[event_type]
        if not buf:
            return
        if flush_to_s3(event_type, buf):
            self.counts[event_type] += len(buf)
            self.buffers[event_type] = []
            self.last_flush[event_type] = time.time()

    def _periodic_flush(self):
        """FLUSH_INTERVAL_SECONDS마다 모든 버퍼 강제 flush"""
        while not self._stop.is_set():
            time.sleep(10)
            for event_type in list(self.buffers.keys()):
                if self._should_flush(event_type):
                    self._do_flush(event_type)

    def run(self):
        print("\n" + "=" * 60)
        print("S3 Consumer 시작")
        print(f"  Flush 조건: {config.FLUSH_BATCH_SIZE}건 OR {config.FLUSH_INTERVAL_SECONDS}초")
        print(f"  S3 경로   : s3://{config.S3_BUCKET}/{config.S3_PREFIX}/{{event_type}}/YYYY/MM/DD/HH/")
        print("  Ctrl+C로 종료")
        print("=" * 60)

        self._flush_thread.start()

        # 각 토픽의 last_flush 초기화
        for et in ["click", "cart", "order"]:
            self.last_flush[et] = time.time()

        msg_count = 0
        try:
            for msg in self.consumer:
                if self._stop.is_set():
                    break

                event      = msg.value
                event_type = event.get("event_type", "unknown")

                self.buffers[event_type].append(event)
                msg_count += 1

                # 건수 조건 달성 시 즉시 flush
                if len(self.buffers[event_type]) >= config.FLUSH_BATCH_SIZE:
                    self._do_flush(event_type)
                    self.consumer.commit()

                if msg_count % 100 == 0:
                    buf_sizes = {k: len(v) for k, v in self.buffers.items()}
                    s3_totals = dict(self.counts)
                    print(
                        f"  메시지 수신 {msg_count:,}건  |  버퍼: {buf_sizes}  |  S3 누적: {s3_totals}"
                    )

        except KeyboardInterrupt:
            print("\n⏹  종료 신호 수신")
        finally:
            self._stop.set()
            # 남은 버퍼 모두 flush
            print("남은 버퍼 S3 flush 중...")
            for et in list(self.buffers.keys()):
                self._do_flush(et)
            self.consumer.commit()
            self.consumer.close()
            self._print_summary(msg_count)

    def _print_summary(self, msg_count: int):
        print("\n" + "=" * 60)
        print("Consumer 종료 요약")
        print(f"  수신 총계 : {msg_count:,}건")
        for et, cnt in self.counts.items():
            print(f"  S3 저장   : {et} → {cnt:,}건")
        print("=" * 60)


# ── SIGTERM 핸들러 (EC2 systemd 등에서 안전 종료) ─────
_consumer_ref: OttoS3Consumer | None = None

def _handle_sigterm(signum, frame):
    print("SIGTERM 수신 — 안전 종료 시작")
    if _consumer_ref:
        _consumer_ref._stop.set()

signal.signal(signal.SIGTERM, _handle_sigterm)


def main():
    global _consumer_ref
    consumer = OttoS3Consumer()
    _consumer_ref = consumer
    consumer.run()


if __name__ == "__main__":
    main()