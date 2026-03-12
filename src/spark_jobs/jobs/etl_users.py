"""ETL job: User dimension with SCD Type 2 change tracking."""

import sys
import logging

from pyspark.sql.functions import col

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake
from src.spark_jobs.transformations.deduplication import deduplicate_events
from src.spark_jobs.transformations.scd_type2 import apply_scd_type2
from src.config.settings import minio_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRACKED_COLUMNS = ["user_tier", "country_code", "display_name", "organization_id", "account_status"]


def run(execution_date: str):
    spark = get_spark_session("etl-users-scd2")
    staging_path = f"s3a://{minio_config.staging_bucket}/user_events/dt={execution_date}/"

    try:
        logger.info(f"Reading user events from {staging_path}")
        raw = spark.read.parquet(staging_path)
        logger.info(f"Read {raw.count():,} raw user events")

        # Deduplicate and filter to signup/profile events
        deduped = deduplicate_events(raw)
        user_changes = deduped.filter(
            col("event_type").isin("user_signup", "user_tier_change")
        )

        if user_changes.count() == 0:
            logger.info("No user changes to process. Skipping.")
            return

        # Prepare incoming data with columns matching dim_users
        incoming = user_changes.select(
            col("user_id"),
            col("email_hash"),
            col("display_name"),
            col("country_code"),
            col("user_tier"),
            col("organization_id"),
            col("platform"),
        ).withColumn("account_status", col("user_tier").alias("account_status"))

        # Read existing dimension from Snowflake
        try:
            existing_dim = read_from_snowflake(spark, "DIM_USERS", schema="WAREHOUSE")
            logger.info(f"Existing dimension has {existing_dim.count():,} rows")
        except Exception:
            logger.info("DIM_USERS does not exist yet, creating initial load")
            from pyspark.sql.functions import lit, current_timestamp
            existing_dim = spark.createDataFrame([], incoming.schema).withColumns({
                "valid_from": current_timestamp().cast("string"),
                "valid_to": lit(None).cast("string"),
                "is_current": lit(True),
            })

        # Apply SCD Type 2
        result = apply_scd_type2(
            existing_dim=existing_dim,
            incoming_data=incoming,
            natural_key="user_id",
            tracked_columns=TRACKED_COLUMNS,
        )

        logger.info(f"SCD2 result: {result.count():,} total dimension rows")

        # Write back to Snowflake (full replace for dim tables)
        write_to_snowflake(result, "DIM_USERS", schema="WAREHOUSE", mode="overwrite")

        logger.info(f"ETL users SCD2 complete for {execution_date}")

    except Exception:
        logger.exception(f"ETL users failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
