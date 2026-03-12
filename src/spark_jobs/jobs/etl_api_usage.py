"""ETL job: API usage events from staging to warehouse fact table."""

import sys
import logging

from pyspark.sql.functions import col, when, lit

from src.spark_jobs.common.spark_session import get_spark_session, write_to_snowflake
from src.spark_jobs.transformations.deduplication import deduplicate_events
from src.config.settings import minio_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_date: str):
    spark = get_spark_session("etl-api-usage")
    staging_path = f"s3a://{minio_config.staging_bucket}/api_usage_events/dt={execution_date}/"
    processed_path = f"s3a://{minio_config.processed_bucket}/fact_api_usage/dt={execution_date}/"

    try:
        logger.info(f"Reading staging data from {staging_path}")
        raw = spark.read.parquet(staging_path)
        logger.info(f"Read {raw.count():,} raw events")

        # Deduplicate
        deduped = deduplicate_events(raw)

        # Enrich: classify API response categories
        enriched = deduped.withColumn(
            "response_category",
            when(col("http_status_code") == 200, lit("success"))
            .when(col("http_status_code") == 429, lit("rate_limited"))
            .when(col("http_status_code").between(400, 499), lit("client_error"))
            .when(col("http_status_code").between(500, 599), lit("server_error"))
            .otherwise(lit("unknown")),
        )

        # Compute token cost estimate (simplified pricing)
        enriched = enriched.withColumn(
            "estimated_cost_usd",
            when(col("model_id") == "gpt-4o", (col("input_tokens") * 2.5 + col("output_tokens") * 10) / 1_000_000)
            .when(col("model_id") == "gpt-4o-mini", (col("input_tokens") * 0.15 + col("output_tokens") * 0.6) / 1_000_000)
            .when(col("model_id") == "gpt-4-turbo", (col("input_tokens") * 10 + col("output_tokens") * 30) / 1_000_000)
            .when(col("model_id") == "gpt-3.5-turbo", (col("input_tokens") * 0.5 + col("output_tokens") * 1.5) / 1_000_000)
            .otherwise(lit(0.0)),
        )

        # Write to processed and Snowflake
        enriched.write.mode("overwrite").parquet(processed_path)
        write_to_snowflake(enriched, "FACT_API_USAGE", schema="WAREHOUSE", mode="append")

        logger.info(f"ETL API usage complete for {execution_date}")

    except Exception:
        logger.exception(f"ETL API usage failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
