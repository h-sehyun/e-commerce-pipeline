"""
otto_pipeline_dag
- Airflow DAG 정의
- 매일 자정 실행
- transform → dq_check → mart → mart_load 순서로 태스크 구성
- 각 태스크는 SparkSubmitOperator로 Spark 작업 실행 

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG 기본 설정
default_args = {
    "owner": "airflow", 
    "depends_on_past": False, 
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONN_ID = "spark_default"
SPARK_CONF    = {
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
}
JOB_PATHS = "/opt/spark/jobs"

JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/jars/postgresql-42.7.1.jar",
])

# DAG 정의
with DAG(
    dag_id="otto_pipeline_dag",
    default_args=default_args,
    description="OTTO ETL 파이프라인 (transform → dq_check → mart → mart_load)",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["otto", "spark", "etl"],
) as dag:
    
    # Task 1: S3 raw → 정제 → S3 stg
    transform = SparkSubmitOperator(
        task_id="transform",
        application=f"{JOB_PATHS}/transform.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        jars=JARS,      
    )

    # Task 2: DQ 검증 (실패 시 중단)
    dq_check = SparkSubmitOperator(
        task_id="dq_check",
        application=f"{JOB_PATHS}/dq_check.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",

    )

    # Task 3: 비즈니스 지표 집계 → S3 mart
    mart = SparkSubmitOperator(
        task_id="mart",
        application=f"{JOB_PATHS}/mart.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        jars=JARS,

    )

    # Task 4: S3 mart → PostgreSQL
    load_mart = SparkSubmitOperator(
        task_id="load_mart",
        application=f"{JOB_PATHS}/load_mart.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        jars=JARS,

    )

    # 태스크 의존성 
    transform >> dq_check >> mart >> load_mart  