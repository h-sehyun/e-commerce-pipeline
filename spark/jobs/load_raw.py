"""
Raw Load Job
- S3 stg/otto/ parquet 읽기
- PostgreSQL stg_events 테이블 적재 (overwrite)
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

STG_PATH  = f"s3a://{config.S3_BUCKET}/stg/otto/"
STG_TABLE = "stg_events"


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


def main():
    print("=" * 60)
    print("Raw Load Job 시작")
    print(f"  STG : {STG_PATH}")
    print(f"  TABLE: {STG_TABLE}")
    print("=" * 60)

    spark = create_spark_session()

    df    = spark.read.parquet(STG_PATH)
    count = df.count()
    print(f" stg 읽기: {count:,}건")

    df.write.jdbc(
        url=POSTGRES_URL,
        table=STG_TABLE,
        mode="overwrite",      # 일배치로 바꿀 때 → append
        properties=POSTGRES_PROPS,
    )

    print(f" {STG_TABLE} 적재 완료: {count:,}건")

    spark.stop()
    print(" Raw Load Job 완료")


if __name__ == "__main__":
    main()