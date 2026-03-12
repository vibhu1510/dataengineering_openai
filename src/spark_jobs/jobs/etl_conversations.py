"""ETL job: Conversation events from staging to warehouse fact table.

Pipeline: MinIO staging Parquet -> dedup -> sessionize -> PII mask -> Snowflake
"""

import sys
import logging

from src.spark_jobs.common.spark_session import get_spark_session, write_to_snowflake
from src.spark_jobs.transformations.deduplication import deduplicate_events
from src.spark_jobs.transformations.sessionization import sessionize
from src.spark_jobs.transformations.pii_masking import apply_pii_masking
from src.config.settings import minio_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_date: str):
    """Run conversation ETL for a given date."""
    spark = get_spark_session("etl-conversations")
    staging_path = f"s3a://{minio_config.staging_bucket}/conversation_events/dt={execution_date}/"
    processed_path = f"s3a://{minio_config.processed_bucket}/fact_conversations/dt={execution_date}/"

    try:
        logger.info(f"Reading staging data from {staging_path}")
        raw = spark.read.parquet(staging_path)
        raw_count = raw.count()
        logger.info(f"Read {raw_count:,} raw events")

        # Step 1: Deduplicate
        deduped = deduplicate_events(raw)
        deduped_count = deduped.count()
        logger.info(f"After deduplication: {deduped_count:,} events ({raw_count - deduped_count} dupes removed)")

        # Step 2: Sessionize (assign computed session IDs based on 30-min inactivity gap)
        sessionized = sessionize(deduped, gap_minutes=30)

        # Step 3: PII masking on any free-text fields
        text_cols = [c for c in sessionized.columns if "text" in c.lower() or "message" in c.lower()]
        cleaned = apply_pii_masking(sessionized, text_cols)

        # Step 4: Write to processed bucket (landing zone for Snowflake COPY INTO)
        logger.info(f"Writing {deduped_count:,} processed events to {processed_path}")
        cleaned.write.mode("overwrite").parquet(processed_path)

        # Step 5: Write to Snowflake
        logger.info("Loading into Snowflake WAREHOUSE.FACT_CONVERSATIONS")
        write_to_snowflake(cleaned, "FACT_CONVERSATIONS", schema="WAREHOUSE", mode="append")

        logger.info(f"ETL conversations complete for {execution_date}")

    except Exception:
        logger.exception(f"ETL conversations failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
