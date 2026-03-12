"""Shared Spark session factory with MinIO and Snowflake configurations."""

from pyspark.sql import SparkSession
from src.config.settings import minio_config, snowflake_config


def get_spark_session(app_name: str = "chatgpt-etl") -> SparkSession:
    """Create a SparkSession configured for MinIO (S3A) and Snowflake."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # S3A / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", minio_config.s3a_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_config.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_config.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    return builder.getOrCreate()


def read_from_snowflake(spark: SparkSession, table: str, schema: str = "WAREHOUSE"):
    """Read a table from Snowflake into a Spark DataFrame."""
    sf_options = {
        **snowflake_config.connection_params,
        "sfSchema": schema,
        "dbtable": table,
    }
    return spark.read.format("snowflake").options(**sf_options).load()


def write_to_snowflake(df, table: str, schema: str = "WAREHOUSE", mode: str = "append"):
    """Write a Spark DataFrame to a Snowflake table."""
    sf_options = {
        **snowflake_config.connection_params,
        "sfSchema": schema,
        "dbtable": table,
    }
    df.write.format("snowflake").options(**sf_options).mode(mode).save()
