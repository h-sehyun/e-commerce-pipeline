"""
Mart Load Job
- S3 otto/mart/{mart_name}/ parquet 읽기
- PostgreSQL mart_* 테이블 적재 (overwrite)
- 나중에 일배치로 바꿀 때: mode="append" + 날짜 파라미터
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession

import config

POSTGRES_URL   = f"jdbc:postgresql://postgres:5432/{config.POSTGRES_DB}"
POSTGRES_PROPS = {
    "user":     config.POSTGRES_USER,
    "password": config.POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

MART_TABLES = {
    "funnel_daily": "mart_funnel_daily",
    "session":      "mart_session",
    "cohort":       "mart_cohort",
    "item_stats":   "mart_item_stats",
}

MART_BASE = f"s3a://{config.S3_BUCKET}/otto/mart/"


def create_spark_session() -> SparkSession:
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


def load_to_postgres(spark: SparkSession, mart_name: str, table: str):
    path = f"{MART_BASE}{mart_name}/"
    print(f" 읽기: {path}")
    df = spark.read.parquet(path)
    count = df.count()
    df.write.jdbc(
        url=POSTGRES_URL,
        table=table,
        mode="overwrite",      # 일배치로 바꿀 때 → append
        properties=POSTGRES_PROPS,
    )
    print(f" {table} 적재 완료: {count:,}건")


def main():
    print("=" * 60)
    print("Mart Load Job 시작")
    print("=" * 60)

    spark = create_spark_session()

    for mart_name, table in MART_TABLES.items():
        try:
            load_to_postgres(spark, mart_name, table)
        except Exception as e:
            print(f" {table} 적재 실패: {e}")
            raise

    spark.stop()
    print(" Mart Load Job 완료")


if __name__ == "__main__":
    main()