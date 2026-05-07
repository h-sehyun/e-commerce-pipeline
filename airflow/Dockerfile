FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java

# jar 파일 다운로드
RUN mkdir -p /opt/spark/jars && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.7.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.4 \
    kafka-python-ng==2.2.3 \
    Faker==24.0.0 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-apache-spark==4.1.5