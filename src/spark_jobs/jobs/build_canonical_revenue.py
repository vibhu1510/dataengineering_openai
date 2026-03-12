"""Build canonical Monthly Revenue metrics table."""

import sys
import logging
from datetime import datetime

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, lit, when,
)

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake, write_to_snowflake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(execution_date: str):
    spark = get_spark_session("build-canonical-revenue")

    try:
        exec_date = datetime.strptime(execution_date, "%Y-%m-%d")
        month_start = exec_date.replace(day=1).strftime("%Y-%m-%d")

        billing = read_from_snowflake(spark, "FACT_BILLING", schema="WAREHOUSE")

        # Filter to current month
        monthly = billing.filter(
            col("timestamp").cast("date") >= month_start
        ).filter(
            col("timestamp").cast("date") <= execution_date
        )

        revenue = monthly.groupBy(
            lit(month_start).alias("month_start"),
        ).agg(
            # Total revenue
            spark_sum(
                when(col("event_type").isin("payment_processed", "subscription_created"), col("amount_usd"))
                .otherwise(lit(0.0))
            ).alias("total_revenue_usd"),

            # Subscription revenue
            spark_sum(
                when(col("event_type") == "subscription_created", col("amount_usd")).otherwise(lit(0.0))
            ).alias("subscription_revenue_usd"),

            # API usage revenue (from payments)
            spark_sum(
                when(col("event_type") == "payment_processed", col("amount_usd")).otherwise(lit(0.0))
            ).alias("api_usage_revenue_usd"),

            # Counts
            count(when(col("event_type") == "subscription_created", True)).alias("new_subscriptions"),
            count(when(col("event_type") == "downgrade", True)).alias("churned_subscriptions"),
            count(when(col("event_type") == "upgrade", True)).alias("upgrades"),
            count(when(col("event_type") == "downgrade", True)).alias("downgrades"),

            # Refunds
            spark_sum(
                when(col("event_type") == "refund", col("amount_usd")).otherwise(lit(0.0))
            ).alias("refunds_usd"),
        )

        # MRR = total subscription revenue (simplified)
        result = revenue.withColumn(
            "mrr_usd", col("subscription_revenue_usd") + col("api_usage_revenue_usd") - col("refunds_usd"),
        )

        write_to_snowflake(result, "MONTHLY_REVENUE", schema="CANONICAL", mode="append")
        logger.info(f"Canonical monthly revenue built for {month_start}")

    except Exception:
        logger.exception(f"Build canonical revenue failed for {execution_date}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-11"
    run(date)
