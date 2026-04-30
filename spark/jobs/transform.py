"""
Transform Job
- S3 otto/raw/{event_type}/ 읽기
- 타입 변환 (ts: ms → timestamp)
- Null 처리
- 중복 제거
- S3 otto/stg/ parquet 저장
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)

import config

# 스키마 정의 
RAW_SCHEMA = StructType([
    StructField("session_id",  LongType(),   True),
    StructField("aid",         LongType(),   True),
    StructField("ts",          LongType(),   True),
    StructField("event_type",  StringType(), True),
    StructField("user_id",     StringType(), True),
    StructField("gender",      StringType(), True),
    StructField("age_group",   StringType(), True),
    StructField("region",      StringType(), True),
    StructField("membership",  StringType(), True),
    StructField("platform",    StringType(), True),
])

# S3 경로 
RAW_PATH = f"s3a://{config.S3_BUCKET}/otto/raw/"
STG_PATH = f"s3a://{config.S3_BUCKET}/otto/stg/"


def create_spark_session() -> SparkSession:
    """SparkSession 생성 (S3 접근 설정 포함)"""
    
    return (
        SparkSession.builder
        .appName("otto-transform")
        .master("spark://spark-master:7077")
        # .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID or "")
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY or "")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_raw(spark: SparkSession):
    """S3 raw/*.jsonl 읽기"""
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .json(RAW_PATH + "*/*/*/*/*/*.jsonl")
    )


def transform(df):
    """정제 및 변환"""

    # 1. Null 제거 (핵심 컬럼)
    df = df.dropna(subset=["session_id", "aid", "ts", "event_type", "user_id"])

    # 2. ts (ms) → timestamp 변환
    df = df.withColumn(
        "event_dt",
        F.to_timestamp(F.from_unixtime(F.col("ts") / 1000))
    )

    # 3. event_date 컬럼 추가 (파티션용)
    df = df.withColumn("event_date", F.to_date(F.col("event_dt")))

    # 4. 중복 제거 (session_id + aid + ts + event_type 기준)
    df = df.dropDuplicates(["session_id", "aid", "ts", "event_type"])

    # 5. event_type 유효값 필터
    df = df.filter(F.col("event_type").isin("click", "cart", "order"))

    # 6. 컬럼 정리
    df = df.select(
        "session_id",
        "aid",
        "ts",
        "event_dt",
        "event_date",
        "event_type",
        "user_id",
        "gender",
        "age_group",
        "region",
        "membership",
        "platform",
    )

    return df


def save_stg(df):
    """S3 stg/ parquet 저장 (event_date 파티션)"""
    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date", "event_type")
        .parquet(STG_PATH)
    )
    print(f"stg 저장 완료: {STG_PATH}")


def main():
    print("=" * 60)
    print("Transform Job 시작")
    print(f"  RAW : {RAW_PATH}")
    print(f"  STG : {STG_PATH}")
    print("=" * 60)

    spark = create_spark_session()

    # 읽기
    df_raw = read_raw(spark)
    print(f"raw 읽기 완료: {df_raw.count():,}건")
    df_raw.printSchema()

    # 변환
    df_stg = transform(df_raw)
    print(f"변환 완료: {df_stg.count():,}건")

    # 저장
    save_stg(df_stg)

    # 샘플 확인
    print("\n샘플 데이터:")
    df_stg.show(5, truncate=False)

    # 이벤트 타입별 통계
    print("\n이벤트 타입별 통계:")
    df_stg.groupBy("event_type").count().orderBy("event_type").show()

    spark.stop()
    print("Transform Job 완료")


if __name__ == "__main__":
    main()