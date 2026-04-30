"""
DQ Check Job
- S3 stg/ 의 데이터 품질 점검(otto/stg/{date}/*.parquet)
- Null 체크 / 중복 체크 / 타입 체크 / 범위 체크 등
- PASS -> 다음 단계 / FAIL -> Airflow 중단
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import config

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


def run_check(df : DataFrame) -> dict: 
    results = {}
    total   = df.count()

    # 1. 건수 확인
    results["total_count"] = {
        "pass": total > 0,
        "detail": f"총 {total:,}건"
    }

    # 2. Null 체크 
    for col in ["session_id", "aid", "ts", "event_type", "user_id"]:
        null_count = df.filter(F.col(col).isNull()).count()
        results[f"null_{col}"] = {
            "pass": null_count == 0,
            "detail": f"{col} Null {null_count:,}건",
        }
    
    # 3. 중복 체크 
    dup_count = total - df.dropDuplicates(["session_id", "aid", "ts", "event_type"]).count()
    results["duplicate"] = {
        "pass": dup_count == 0,
        "detail": f"중복 {dup_count:,}건",
    }

    # 4. event_type 유효값 체크
    invalid = df.filter(~F.col("event_type").isin("click", "cart", "order")).count()
    results["invalid_event_type"] = {
        "pass": invalid == 0,
        "detail": f"invalid_event_type {invalid:,}건",
    }

    return results

def print_results(results: dict) -> bool:
    print("\n" + "=" * 60)
    print("DQ Check 결과")
    print("=" * 60)
    all_pass = True
    for check, result in results.items():
        status = "PASS" if result["pass"] else "FAIL"
        print(f"  {status} | {check:<25} | {result['detail']}")
        if not result["pass"]:
            all_pass = False
    print("=" * 60)
    print(f"최종 결과: {'ALL PASS' if all_pass else 'FAIL'}")
    print("=" * 60)
    return all_pass

def main():
    print("=" * 60)
    print("DQ Check Job 시작")
    print("=" * 60)

    spark   = create_spark_session()
    df      = spark.read.parquet(STG_PATH)
    results = run_check(df)
    passed = print_results(results)

    spark.stop()

    if not passed:
        raise ValueError("DQ Check 실패 - 파이프라인 중단")
    
    print("DQ Check 통과")

if __name__ == "__main__":
    main()