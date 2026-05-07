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
│   ├── jars/                 # Spark 연동용 jar 파일(gitignore)
│   │   ├── hadoop-aws-3.3.4.jar
│   │   ├── aws-java-sdk-bundle-1.12.262.jar
│   │   └── postgresql-42.7.1.jar
│   └── jobs/
│       ├── transform.py      # S3 raw → 정제 → S3 stg
│       ├── dq_check.py       # DQ 검증
│       ├── mart.py           # 비즈니스 지표 집계 → S3 mart
│       ├── load_mart.py      # S3 mart → PostgreSQL
│       └── load_raw.py       # S3 stg  → PostgreSQL stg_events
│
├── airflow/
│   ├── Dockerfile            # Airflow 2.8.1 + Java + Spark provider
│   └── dags/
│       └── otto_pipeline_dag.py  # Spark ETL 배치 DAG
│
└── data/                     # gitignore
    ├── raw/otto-recsys-train.jsonl
    └── users.json
```


## 실행 순서

### 1. 환경 설정
```bash
git clone https://github.com/<your-repo>/e-commerce-pipeline.git
cd e-commerce-pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
nano .env
```

### 2. 인프라 실행
```bash
# Spark 이미지 빌드 (최초 1회)
docker compose build spark-master spark-worker-1

# Spark jar 파일 복사 (최초 1회)
mkdir -p spark/jars
docker cp spark-master:/opt/spark/jars/hadoop-aws-3.3.4.jar spark/jars/
docker cp spark-master:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar spark/jars/
docker cp spark-master:/opt/spark/jars/postgresql-42.7.1.jar spark/jars/

# Airflow 이미지 빌드 (최초 1회)
docker compose build airflow-webserver airflow-scheduler

# 전체 실행
docker compose up -d
```

### 3. 데이터 준비
```bash
make users       # Faker 유저 생성 (1회)
make download    # OTTO 데이터 다운로드 (선택)
```

### 4. 스트리밍
```bash
make consumer    # 터미널 1
make producer    # 터미널 2
```

### 5. Spark 배치 처리 (수동 실행)
```bash
docker exec airflow-scheduler /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 512m \
    --driver-memory 512m \
    --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/postgresql-42.7.1.jar \
    /opt/spark/jobs/transform.py
```

### 6. Airflow DAG 실행
```bash
# Airflow UI 접속
http://<Elastic-IP>:8082
# ID: admin / PW: admin

# spark_default Connection 등록 (최초 1회)
Admin → Connections → + 추가
  Connection Id   : spark_default
  Connection Type : Spark
  Host            : spark://spark-master
  Port            : 7077
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


## 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| `NoBrokersAvailable` | `KAFKA_BOOTSTRAP_SERVERS` 오류 | `.env`에서 `localhost:9092` 확인 |
| `defaulting to yarn` | spark_default connection 없음 | Airflow UI에서 connection 등록 |
| `S3AFileSystem not found` | hadoop-aws jar 없음 | DAG `jars` 파라미터에 jar 경로 추가 |
| `postgresql Driver not found` | postgresql jar 없음 | DAG `jars`에 postgresql jar 추가 |
| `permission denied` dags 파일 | airflow 컨테이너 권한 | `chmod 777 airflow/dags/` |
| EC2 재시작 후 연결 끊김 | IP 변경 | Elastic IP 필수 |
| 대용량 파일 git push 실패 | `data/` 미제외 | `.gitignore`에 `data/`, `*.zip` 추가 |


## 전체 진행 단계

- [x] 1단계: Kafka 스트리밍 파이프라인
  - [x] Kafka Docker 배포
  - [x] OTTO Producer / S3 Consumer 구현

- [x] 2단계: Spark 배치 처리
  - [x] Spark Docker 배포
  - [x] ETL 파이프라인 구현 (transform → dq_check → mart → load)

- [ ] 3단계: Airflow 스케줄링
  - [x] Airflow Docker 배포 (Airflow 2.8.1 + LocalExecutor)
  - [x] otto_pipeline_dag 구현 (SparkSubmitOperator)
  - [ ] 일배치 DAG 증분 처리 (날짜 파라미터 추가)

- [ ] 4단계: 시각화
  - [ ] Redash / Superset 대시보드 연동

- [ ] 5단계: 부가 설정
  - [ ] DataHub(데이터 카탈로그) 구성
  - [ ] Slack 알림 기능 추가