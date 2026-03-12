"""Build canonical Weekly Engagement metrics table."""

import sys
import logging
from datetime import datetime, timedelta

from pyspark.sql.functions import (
    col, count, countDistinct, avg, sum as spark_sum,
    percentile_approx, lit,
)

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_date: str):
    spark = get_spark_session("build-canonical-engagement")

    try:
        # Compute week boundaries (Monday to Sunday)
        exec_date = datetime.strptime(execution_date, "%Y-%m-%d")
        week_start = exec_date - timedelta(days=exec_date.weekday())
        week_end = week_start + timedelta(days=6)

        conversations = read_from_snowflake(spark, "FACT_CONVERSATIONS", schema="WAREHOUSE")
        dim_users = read_from_snowflake(spark, "DIM_USERS", schema="WAREHOUSE").filter(col("is_current") == True)  # noqa: E712

        # Filter to current week's messages
        weekly_msgs = (
            conversations
            .filter(col("event_type") == "message_sent")
            .filter(col("timestamp").cast("date").between(
                week_start.strftime("%Y-%m-%d"),
                week_end.strftime("%Y-%m-%d"),
            ))
            .join(dim_users.select("user_id", "user_tier"), "user_id", "left")
        )

        # Aggregate by tier
        engagement = weekly_msgs.groupBy(
            lit(week_start.strftime("%Y-%m-%d")).alias("week_start"),
            col("user_tier"),
        ).agg(
            countDistinct("session_id").alias("total_sessions"),
            count("*").alias("total_messages"),
            avg("total_tokens").alias("avg_tokens_per_message"),
            percentile_approx("latency_ms", 0.5).alias("p50_latency_ms"),
            percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
            percentile_approx("latency_ms", 0.99).alias("p99_latency_ms"),
            countDistinct("model_id").alias("unique_models_used"),
        )

        # Compute avg messages per session
        session_stats = weekly_msgs.groupBy("session_id").agg(
            count("*").alias("msg_count"),
        )
        avg_msg_per_session = session_stats.select(avg("msg_count").alias("avg_messages_per_session"))

        result = engagement.crossJoin(avg_msg_per_session)

        write_to_snowflake(result, "WEEKLY_ENGAGEMENT", schema="CANONICAL", mode="append")
        logger.info(f"Canonical weekly engagement built for week of {week_start.strftime('%Y-%m-%d')}")

    except Exception:
        logger.exception(f"Build canonical engagement failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
