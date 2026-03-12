"""Post-load warehouse validation checks."""

import logging

from src.spark_jobs.common.spark_session import get_spark_session, read_from_snowflake

logger = logging.getLogger(__name__)


def run(execution_date: str):
    """Run post-load validation on warehouse tables."""
    spark = get_spark_session("quality-warehouse")

    try:
        checks_passed = 0
        checks_total = 0

        # Check 1: fact_conversations has data for the execution date
        conversations = read_from_snowflake(spark, "FACT_CONVERSATIONS")
        conv_count = conversations.filter(
            conversations.event_timestamp.cast("date") == execution_date
        ).count()
        checks_total += 1
        if conv_count > 0:
            checks_passed += 1
            logger.info(f"[PASS] FACT_CONVERSATIONS: {conv_count:,} rows for {execution_date}")
        else:
            logger.warning(f"[FAIL] FACT_CONVERSATIONS: 0 rows for {execution_date}")

        # Check 2: dim_users has current rows
        dim_users = read_from_snowflake(spark, "DIM_USERS")
        current_users = dim_users.filter(dim_users.is_current == True).count()  # noqa: E712
        checks_total += 1
        if current_users > 0:
            checks_passed += 1
            logger.info(f"[PASS] DIM_USERS: {current_users:,} current rows")
        else:
            logger.warning("[FAIL] DIM_USERS: 0 current rows")

        # Check 3: No orphaned fact records (referential integrity)
        orphans = conversations.join(
            dim_users.select("user_id"),
            conversations.user_id == dim_users.user_id,
            "left_anti",
        ).count()
        checks_total += 1
        if orphans == 0:
            checks_passed += 1
            logger.info("[PASS] Referential integrity: no orphaned conversations")
        else:
            logger.warning(f"[WARN] {orphans:,} conversations with no matching user dimension")

        logger.info(f"Warehouse validation: {checks_passed}/{checks_total} passed")

        if checks_passed < checks_total:
            raise ValueError(f"Warehouse validation: {checks_total - checks_passed} checks failed")

    finally:
        spark.stop()
