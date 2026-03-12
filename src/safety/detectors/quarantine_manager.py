"""Manage account quarantine based on safety signals and anomaly detection."""

import logging
from datetime import datetime, timezone

from pyspark.sql.functions import col, count, max as spark_max, lit, current_timestamp

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake

logger = logging.getLogger(__name__)

# Quarantine thresholds
VIOLATION_THRESHOLD_HIGH = 5      # Auto-quarantine after 5 high-severity violations
VIOLATION_THRESHOLD_CRITICAL = 1   # Auto-quarantine after 1 critical violation
VIOLATION_WINDOW_DAYS = 30         # Look-back window


def update_quarantine_table(execution_time: str):
    """Evaluate recent safety signals and quarantine accounts that exceed thresholds."""
    spark = get_spark_session("quarantine-manager")

    try:
        # Read recent safety events
        safety = read_from_snowflake(spark, "SAFETY_EVENTS", schema="RAW")

        # Aggregate violations per user in the lookback window
        user_violations = (
            safety
            .groupBy("user_id")
            .agg(
                count("*").alias("violation_count"),
                count(
                    col("severity").isin("high", "critical") | None
                ).alias("severe_count"),
                spark_max("severity").alias("max_severity"),
                spark_max("confidence_score").alias("max_confidence"),
                spark_max("violation_type").alias("primary_violation_type"),
            )
        )

        # Determine which accounts should be quarantined
        to_quarantine = user_violations.filter(
            (col("severe_count") >= VIOLATION_THRESHOLD_HIGH)
            | (
                (col("max_severity") == "critical")
                & (col("max_confidence") >= 0.7)
            )
        ).select(
            col("user_id"),
            col("primary_violation_type").alias("violation_type"),
            col("max_severity").alias("severity"),
            col("violation_count"),
            col("max_confidence").alias("confidence_score"),
            current_timestamp().alias("quarantined_at"),
            lit(None).cast("timestamp").alias("reviewed_at"),
            lit(None).cast("string").alias("review_outcome"),
            lit(True).alias("is_active"),
        ).withColumn(
            "quarantine_reason",
            lit("Auto-quarantined: exceeded violation thresholds"),
        )

        quarantine_count = to_quarantine.count()

        if quarantine_count > 0:
            write_to_snowflake(to_quarantine, "QUARANTINED_ACCOUNTS", schema="SAFETY", mode="append")
            logger.warning(f"Quarantined {quarantine_count} accounts")
        else:
            logger.info("No new accounts to quarantine")

        return {"quarantined": quarantine_count}

    finally:
        spark.stop()
