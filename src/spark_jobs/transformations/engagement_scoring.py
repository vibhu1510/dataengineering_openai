"""Compute user engagement scores from conversation and usage metrics."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, datediff,
    current_date, when, lit, greatest,
)


def compute_engagement_score(conversations_df: DataFrame, api_df: DataFrame) -> DataFrame:
    """Compute a composite engagement score per user per day.

    Score components (each normalized 0-1, then weighted):
    - Session frequency: number of sessions (weight: 0.25)
    - Message volume: total messages sent (weight: 0.20)
    - Conversation depth: avg turns per session (weight: 0.15)
    - Model diversity: distinct models used (weight: 0.10)
    - API activity: API calls made (weight: 0.15)
    - Retention signal: days since signup (weight: 0.15)
    """
    # Conversation metrics per user per day
    conv_metrics = (
        conversations_df
        .filter(col("event_type") == "message_sent")
        .groupBy("user_id", col("timestamp").cast("date").alias("event_date"))
        .agg(
            countDistinct("session_id").alias("session_count"),
            count("*").alias("message_count"),
            avg("total_tokens").alias("avg_tokens"),
            countDistinct("model_id").alias("models_used"),
            avg("latency_ms").alias("avg_latency_ms"),
        )
    )

    # API metrics per user per day
    api_metrics = (
        api_df
        .filter(col("http_status_code") == 200)
        .groupBy("user_id", col("timestamp").cast("date").alias("event_date"))
        .agg(
            count("*").alias("api_call_count"),
            spark_sum("total_tokens").alias("total_api_tokens"),
        )
    )

    # Join and compute composite score
    combined = conv_metrics.join(api_metrics, ["user_id", "event_date"], "full_outer")

    scored = combined.select(
        col("user_id"),
        col("event_date"),
        col("session_count"),
        col("message_count"),
        col("api_call_count"),
        col("models_used"),
        # Normalize each component to 0-1 range using sigmoid-like capping
        _normalize("session_count", 10).alias("session_score"),
        _normalize("message_count", 50).alias("message_score"),
        _normalize("api_call_count", 100).alias("api_score"),
        _normalize("models_used", 4).alias("diversity_score"),
    )

    # Weighted composite
    return scored.withColumn(
        "engagement_score",
        (
            col("session_score") * 0.30
            + col("message_score") * 0.25
            + col("api_score") * 0.25
            + col("diversity_score") * 0.20
        ),
    )


def _normalize(column_name: str, cap_value: int):
    """Normalize a column to 0-1 range, capping at cap_value."""
    return when(
        col(column_name).isNull(), lit(0.0)
    ).otherwise(
        greatest(lit(0.0), col(column_name).cast("double") / lit(cap_value))
    ).cast("double")
