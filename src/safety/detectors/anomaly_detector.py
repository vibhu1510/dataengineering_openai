"""Anomaly detection for user behavior using Z-score analysis.

Detects users whose activity deviates significantly from their baseline,
which may indicate abuse, credential stuffing, or automated scraping.
"""

import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, mean, stddev, abs as spark_abs, when, lit, current_timestamp,
)

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake

logger = logging.getLogger(__name__)


def detect_zscore_anomalies(
    df: DataFrame,
    metric_col: str,
    group_col: str = "user_id",
    threshold: float = 3.0,
) -> DataFrame:
    """Flag records where the metric deviates > threshold standard deviations from the mean.

    Uses per-user historical baselines to detect anomalous behavior.
    A Z-score > 3.0 means the value is in the 99.7th percentile.
    """
    stats = df.groupBy(group_col).agg(
        mean(metric_col).alias("_mean"),
        stddev(metric_col).alias("_stddev"),
        count("*").alias("_sample_size"),
    )

    joined = df.join(stats, group_col)

    return (
        joined
        .withColumn(
            "_zscore",
            when(
                (col("_stddev").isNotNull()) & (col("_stddev") > 0) & (col("_sample_size") >= 5),
                spark_abs((col(metric_col) - col("_mean")) / col("_stddev")),
            ).otherwise(lit(0.0)),
        )
        .withColumn("is_anomaly", col("_zscore") > threshold)
        .withColumn("anomaly_zscore", col("_zscore"))
        .drop("_mean", "_stddev", "_zscore", "_sample_size")
    )


def detect_rate_anomalies(
    df: DataFrame,
    window_minutes: int = 60,
    threshold_multiplier: float = 5.0,
) -> DataFrame:
    """Detect users with abnormally high request rates vs their baseline."""
    hourly_counts = (
        df.groupBy("user_id")
        .agg(count("*").alias("request_count"))
    )

    stats = hourly_counts.agg(
        mean("request_count").alias("global_mean"),
        stddev("request_count").alias("global_stddev"),
    ).collect()[0]

    global_mean = stats["global_mean"] or 0
    global_stddev = stats["global_stddev"] or 1

    return hourly_counts.withColumn(
        "is_rate_anomaly",
        col("request_count") > (global_mean + threshold_multiplier * global_stddev),
    )


def run_hourly_detection(execution_time: str) -> dict:
    """Entry point for hourly anomaly detection, called by Airflow."""
    spark = get_spark_session("safety-anomaly-detection")

    try:
        # Read recent safety events
        safety_events = read_from_snowflake(spark, "SAFETY_EVENTS", schema="RAW")

        # Read recent API usage for rate anomalies
        api_usage = read_from_snowflake(spark, "FACT_API_USAGE", schema="WAREHOUSE")

        # Z-score anomalies on violation count
        if safety_events.count() > 0:
            anomalies = detect_zscore_anomalies(
                safety_events,
                metric_col="user_violation_count_30d",
                threshold=3.0,
            )
            flagged = anomalies.filter(col("is_anomaly") == True)  # noqa: E712
            flagged_count = flagged.count()
        else:
            flagged_count = 0

        # Rate anomalies on API usage
        if api_usage.count() > 0:
            rate_anomalies = detect_rate_anomalies(api_usage)
            rate_flagged = rate_anomalies.filter(col("is_rate_anomaly") == True).count()  # noqa: E712
        else:
            rate_flagged = 0

        total_anomalies = flagged_count + rate_flagged
        logger.info(
            f"Anomaly detection complete: {flagged_count} behavioral anomalies, "
            f"{rate_flagged} rate anomalies"
        )

        return {"anomaly_count": total_anomalies, "behavioral": flagged_count, "rate": rate_flagged}

    finally:
        spark.stop()
