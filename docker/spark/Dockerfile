FROM bitnami/spark:3.5.4

USER root

# Install Python dependencies
COPY pyproject.toml /opt/bitnami/spark/
RUN pip install --no-cache-dir \
    pyspark==3.5.4 \
    minio==7.2.3 \
    pyarrow==14.0.2 \
    snowflake-connector-python==3.6.0 \
    python-dotenv==1.0.1 \
    numpy==1.26.4

# Download required JARs for S3 (MinIO) and Snowflake connectivity
RUN cd /opt/bitnami/spark/jars && \
    curl -sO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sO https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.16.0-spark_3.4/spark-snowflake_2.12-2.16.0-spark_3.4.jar && \
    curl -sO https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.4/snowflake-jdbc-3.14.4.jar

USER 1001
