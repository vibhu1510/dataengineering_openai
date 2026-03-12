"""Build canonical Daily Active Users (DAU) table.

Aggregates across conversations and API usage to compute DAU
segmented by tier, platform, and new vs returning users.
"""

import sys
import logging

from pyspark.sql.functions import (
    col, countDistinct, count, lit, when,
)

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_date: str):
    spark = get_spark_session("build-canonical-dau")

    try:
        # Read fact tables
        conversations = read_from_snowflake(spark, "FACT_CONVERSATIONS", schema="WAREHOUSE")
        api_usage = read_from_snowflake(spark, "FACT_API_USAGE", schema="WAREHOUSE")
        dim_users = read_from_snowflake(spark, "DIM_USERS", schema="WAREHOUSE").filter(col("is_current") == True)  # noqa: E712

        # Union active user IDs from both sources
        conv_users = (
            conversations
            .filter(col("timestamp").cast("date") == execution_date)
            .select("user_id", lit("conversation").alias("activity_source"))
        )
        api_users = (
            api_usage
            .filter(col("timestamp").cast("date") == execution_date)
            .select("user_id", lit("api").alias("activity_source"))
        )
        all_active = conv_users.unionByName(api_users)

        # Join with dimension for segmentation
        enriched = all_active.join(dim_users, "user_id", "left")

        # Compute DAU by segments
        dau = enriched.groupBy(lit(execution_date).alias("event_date")).agg(
            countDistinct("user_id").alias("total_dau"),
            countDistinct(when(col("user_tier") == "free", col("user_id"))).alias("dau_free"),
            countDistinct(when(col("user_tier") == "plus", col("user_id"))).alias("dau_plus"),
            countDistinct(when(col("user_tier") == "enterprise", col("user_id"))).alias("dau_enterprise"),
            countDistinct(when(col("platform") == "web", col("user_id"))).alias("dau_web"),
            countDistinct(when(col("platform").isin("ios", "android"), col("user_id"))).alias("dau_mobile"),
            countDistinct(when(col("activity_source") == "api", col("user_id"))).alias("dau_api"),
        )

        # New vs returning (users whose signup_date == execution_date)
        new_users = (
            enriched
            .filter(col("signup_date").cast("date") == execution_date)
            .select(countDistinct("user_id").alias("new_users_today"))
        )

        # Combine
        result = dau.crossJoin(new_users)
        result = result.withColumn(
            "returning_users",
            col("total_dau") - col("new_users_today"),
        )

        write_to_snowflake(result, "DAILY_ACTIVE_USERS", schema="CANONICAL", mode="append")
        logger.info(f"Canonical DAU built for {execution_date}: {result.collect()[0]['total_dau']} DAU")

    except Exception:
        logger.exception(f"Build canonical DAU failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
