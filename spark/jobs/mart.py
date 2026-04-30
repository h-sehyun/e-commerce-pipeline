"""
Mart Job
- S3 stg/otto/ 전체 읽기
- 비즈니스 지표 집계
    - mart_funnel_daily | 일별 퍼널 전환율  | Daily
    - mart_session      | 세션 분석        | Daily
    - mart_cohort       | 코호트 리텐션     | Monthly
    - mart_item_stats   | 상품별 인기도     | Daily
- S3 mart/otto/{mart_name}/ parquet 저장
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import config

STG_PATH  = f"s3a://{config.S3_BUCKET}/stg/otto/"
MART_BASE = f"s3a://{config.S3_BUCKET}/mart/otto/"


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


def save_mart(df: DataFrame, mart_name: str):
    path = f"{MART_BASE}{mart_name}/"
    df.write.mode("overwrite").parquet(path)
    print(f" {mart_name} 저장 : {path}  ({df.count():,}건)")

# 1. 퍼널 전환율
def build_funnel_daily(df: DataFrame) -> DataFrame:
    agg = df.groupBy("event_date").agg(
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.sum(F.when(F.col("event_type") == "cart",  1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "order", 1).otherwise(0)).alias("order_count"),
    )
    return agg.withColumn(
        "click_to_cart",  F.round(F.col("cart_count")  / F.col("click_count") * 100, 2)
    ).withColumn(
        "cart_to_order",  F.round(F.col("order_count") / F.col("cart_count")  * 100, 2)
    ).withColumn(
        "click_to_order", F.round(F.col("order_count") / F.col("click_count") * 100, 2)
    ).withColumnRenamed("event_date", "dt")


# 2. 세션 분석
def build_session(df: DataFrame) -> DataFrame:
    return df.groupBy("session_id", "user_id", "platform", "region").agg(
        F.min("event_dt").alias("session_start"),
        F.max("event_dt").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.sum(F.when(F.col("event_type") == "cart",  1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "order", 1).otherwise(0)).alias("order_count"),
    ).withColumn(
        "duration_sec",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
    )


# 3. 코호트 리텐션
def build_cohort(df: DataFrame) -> DataFrame:
    first_event = df.groupBy("user_id").agg(
        F.date_format(F.min("event_dt"), "yyyy-MM").alias("cohort_month")
    )
    activity = df.select(
        "user_id",
        F.date_format("event_dt", "yyyy-MM").alias("activity_month")
    ).distinct()
 
    joined = activity.join(first_event, on="user_id")
    cohort_size = first_event.groupBy("cohort_month").agg(
        F.count("user_id").alias("cohort_size")
    )
    return joined.groupBy("cohort_month", "activity_month").agg(
        F.countDistinct("user_id").alias("active_users")
    ).join(cohort_size, on="cohort_month").withColumn(
        "retention_rate",
        F.round(F.col("active_users") / F.col("cohort_size") * 100, 2)
    )

# 4. 상품별 인기도
def build_item_stats(df: DataFrame) -> DataFrame:
    agg = df.groupBy("aid").agg(
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.sum(F.when(F.col("event_type") == "cart",  1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "order", 1).otherwise(0)).alias("order_count"),
    )
    return agg.withColumn(
        "cart_rate",  F.round(F.col("cart_count")  / F.col("click_count") * 100, 2)
    ).withColumn(
        "order_rate", F.round(F.col("order_count") / F.col("click_count") * 100, 2)
    )
 
def main():
    print("=" * 60)
    print("Mart Job 시작")
    print("=" * 60)
 
    spark = create_spark_session()
    df    = spark.read.parquet(STG_PATH)
    print(f" stg 읽기: {df.count():,}건")
 
    save_mart(build_funnel_daily(df), "funnel_daily")
    save_mart(build_session(df),      "session")
    save_mart(build_cohort(df),       "cohort")
    save_mart(build_item_stats(df),   "item_stats")
 
    spark.stop()
    print(" Mart Job 완료")
 
 
if __name__ == "__main__":
    main()