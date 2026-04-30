FROM apache/spark:3.5.4
 
USER root

RUN pip3 install --no-cache-dir \
    pyspark==3.5.4 \
    psycopg2-binary \
    python-dotenv

# PostgreSQL JDBC 드라이버 (Spark → PostgreSQL 연결)
RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar
 
# AWS S3 연결용 jar (Spark → S3 읽기/쓰기)
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
 
# Spark 기본 설정
RUN mkdir -p /opt/spark/conf && \
    printf '\
spark.driver.host spark-master\n\
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem\n\
spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.DefaultAWSCredentialsProviderChain\n\
' > /opt/spark/conf/spark-defaults.conf
 
USER spark
 