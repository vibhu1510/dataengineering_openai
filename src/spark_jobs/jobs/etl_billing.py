"""ETL job: Billing events from staging to warehouse fact table."""

import sys
import logging

from pyspark.sql.functions import col, when, lit

from src.spark_jobs.common.spark_session import get_spark_session, write_to_snowflake
from src.spark_jobs.transformations.deduplication import deduplicate_events
from src.config.settings import minio_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_date: str):
    spark = get_spark_session("etl-billing")
    staging_path = f"s3a://{minio_config.staging_bucket}/billing_events/dt={execution_date}/"
    processed_path = f"s3a://{minio_config.processed_bucket}/fact_billing/dt={execution_date}/"

    try:
        logger.info(f"Reading staging data from {staging_path}")
        raw = spark.read.parquet(staging_path)
        logger.info(f"Read {raw.count():,} raw billing events")

        # Deduplicate
        deduped = deduplicate_events(raw)

        # Classify revenue impact
        enriched = deduped.withColumn(
            "revenue_impact",
            when(col("event_type").isin("subscription_created", "payment_processed", "upgrade"), lit("positive"))
            .when(col("event_type") == "refund", lit("negative"))
            .when(col("event_type") == "downgrade", lit("at_risk"))
            .otherwise(lit("neutral")),
        )

        # Flag failed payments for follow-up
        enriched = enriched.withColumn(
            "requires_followup",
            when(
                (col("event_type") == "payment_processed") & (col("payment_status") == "failed"),
                lit(True),
            ).otherwise(lit(False)),
        )

        # Write outputs
        enriched.write.mode("overwrite").parquet(processed_path)
        write_to_snowflake(enriched, "FACT_BILLING", schema="WAREHOUSE", mode="append")

        logger.info(f"ETL billing complete for {execution_date}")

    except Exception:
        logger.exception(f"ETL billing failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
