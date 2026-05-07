FROM apache/airflow:2.7.3
 
USER root
 
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && apt-get clean
 
ENV JAVA_HOME=/usr/lib/jvm/default-java
 
USER airflow
 
RUN pip install --no-cache-dir \
    pyspark==3.5.4 \
    kafka-python==2.0.2 \
    Faker==19.13.0 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-apache-spark==4.1.5
 