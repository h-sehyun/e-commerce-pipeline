# E-Commerce Pipeline
OTTO 이커머스 실제 데이터를 Faker 유저와 결합해 Kafka로 스트리밍하고 Spark로 처리해 PostgreSQL에 적재하는 데이터 파이프라인

## 데이터 아키텍처
![ETL_파이프라인_아키텍처](ETL_파이프라인_아키텍처.png)

## 프로젝트 구조

```
e-commerce-pipeline/
├── .env.example
├── .gitignore
├── config.py
├── requirements.txt
├── Makefile
├── docker-compose.yml
├── README.md
│
├── src/
│   └── generate_users.py     # Faker 고정 유저 생성
│
├── kafka/
│   ├── producer.py           # OTTO 이벤트 + 유저 배정 → Kafka
│   └── consumer.py           # Kafka → S3 저장
│
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       ├── transform.py      # S3 raw → 정제 → S3 stg
│       ├── dq_check.py       # DQ 검증
│       ├── mart.py           # 비즈니스 지표 집계 → S3 mart
│       ├── load_mart.py      # S3 mart → PostgreSQL
│       └── load_raw.py       # S3 stg  → PostgreSQL stg_events
│
├── airflow/
│   └── dags/                 # 예정
│
└── data/                     # gitignore
    ├── raw/otto-recsys-train.jsonl
    └── users.json
```

## S3 경로

```
otto/raw/{event_type}/YYYY/MM/DD/HH/    ← Kafka Consumer 적재
otto/stg/                               ← Spark 정제본 (parquet)
otto/mart/{mart_name}/                  ← 집계 결과 (parquet)
```
### 설계 고려사항

**이벤트 타입별 폴더 분리 (`click/cart/order`)**
- Spark에서 필요한 이벤트 타입만 골라 읽을 수 있어 불필요한 데이터 스캔 감수.

**시간 파티션 (`YYYY/MM/DD/HH/`)**
- 날짜 기준 증분 처리를 위해 시간 단위로 파티셔닝
- 3단계 Airflow 일배치 전환 시 `YYYY/MM/DD/`로 변경 예정

**UTC 기준 시간**
- Spark, Airflow 모두 UTC를 기본으로 동작하므로 시간대를 UTC로 통일
- 대시보드 표시 시 KST로 변환

**jsonl 포맷**
- 한 줄이 하나의 이벤트로 구성되어 스트리밍으로 읽고 쓰기 용이
- Spark에서 스키마 추론 없이 읽을 수 있음


## PostgreSQL 테이블

| 테이블 | 설명 |
|--------|------|
| `stg_events` | 정제된 전체 이벤트 (raw_load) |
| `users` | 고객 정보 |
| `mart_funnel_daily` | 일별 퍼널 전환율 |
| `mart_session` | 세션 분석 |
| `mart_cohort` | 코호트 리텐션 |
| `mart_item_stats` | 상품별 인기도 |


## OTTO 데이터셋

- 출처: [OTTO Recommender Systems Dataset](https://github.com/otto-de/recsys-dataset)
- 라이센스: CC-BY 4.0
- 규모: 세션 12,899,779개 / 이벤트 216,716,096건

## 전체 진행 단계

- [x] 1단계: Kafka 스트리밍 파이프라인
  - [x] Kafka Docker 배포
  - [x] OTTO Producer / S3 Consumer 구현

- [x] 2단계: Spark 배치 처리
  - [x] Spark Docker 배포
  - [x] ETL 파이프라인 구현 (transform → dq_check → mart → load)

- [ ] 3단계: Airflow 스케줄링
  - [ ] Airflow Docker 배포
  - [ ] 일배치 DAG 구현 (증분 처리)

- [ ] 4단계: 시각화
  - [ ] Redash / Superset 대시보드 연동

- [ ] 5단계: 부가 설정
  - [ ] DataHub(데이터 카탈로그) 구성
  - [ ] Slack 알림 기능 추가