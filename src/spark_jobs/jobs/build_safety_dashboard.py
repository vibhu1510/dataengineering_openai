"""Build safety dashboard metrics from safety events."""

import sys
import logging

from pyspark.sql.functions import (
    col, count, when, avg, lit, current_timestamp,
)

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_time: str):
    spark = get_spark_session("build-safety-dashboard")

    try:
        # Read safety events for the current date
        execution_date = execution_time[:10]  # Extract date from timestamp

        safety = read_from_snowflake(spark, "SAFETY_EVENTS", schema="RAW")
        quarantine = read_from_snowflake(spark, "QUARANTINED_ACCOUNTS", schema="SAFETY")

        daily_safety = safety.filter(
            col("timestamp").cast("date") == execution_date
        )

        dashboard = daily_safety.groupBy(
            lit(execution_date).alias("event_date"),
        ).agg(
            count("*").alias("total_safety_signals"),
            count(when(col("violation_type") == "prompt_injection", True)).alias("prompt_injection_attempts"),
            count(when(col("violation_type") == "jailbreak_attempt", True)).alias("jailbreak_attempts"),
            count(when(col("violation_type") == "rate_limit_abuse", True)).alias("rate_limit_violations"),
            count(when(col("violation_type") == "content_policy_violation", True)).alias("content_policy_flags"),
            avg("confidence_score").alias("avg_confidence"),
        )

        # Quarantine stats for the day
        quarantine_today = quarantine.filter(
            col("quarantined_at").cast("date") == execution_date
        )
        quarantined_count = quarantine_today.filter(col("is_active") == True).count()  # noqa: E712
        released_count = quarantine_today.filter(col("review_outcome") == "released").count()

        dashboard = dashboard.withColumn("accounts_quarantined", lit(quarantined_count))
        dashboard = dashboard.withColumn("accounts_released", lit(released_count))

        write_to_snowflake(dashboard, "SAFETY_DASHBOARD", schema="SAFETY", mode="append")
        logger.info(f"Safety dashboard built for {execution_date}")

    except Exception:
        logger.exception(f"Build safety dashboard failed for {execution_time}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    ts = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11T00:00:00"
    run(ts)
