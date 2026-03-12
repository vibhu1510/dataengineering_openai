"""Data retention and compliance utilities.

Implements data lifecycle management:
- Automated deletion of data past retention period
- PII removal for expired records
- Audit logging of retention actions
"""

import logging
from datetime import datetime, timezone, timedelta

from src.spark_jobs.common.spark_session import get_spark_session
from src.config.settings import snowflake_config

logger = logging.getLogger(__name__)

# Retention policies (days)
RETENTION_POLICIES = {
    "RAW.SAFETY_EVENTS": 90,
    "WAREHOUSE.FACT_CONVERSATIONS": 730,      # 2 years
    "WAREHOUSE.FACT_API_USAGE": 730,
    "WAREHOUSE.FACT_BILLING": 2555,            # 7 years (financial records)
    "SAFETY.QUARANTINED_ACCOUNTS": 365,
    "CANONICAL.DAILY_ACTIVE_USERS": 1095,      # 3 years
    "CANONICAL.WEEKLY_ENGAGEMENT": 1095,
    "CANONICAL.MONTHLY_REVENUE": 2555,
}


def apply_retention_policy(table_name: str, timestamp_column: str = "event_timestamp"):
    """Delete records older than the retention period for a given table."""
    retention_days = RETENTION_POLICIES.get(table_name)
    if retention_days is None:
        logger.warning(f"No retention policy defined for {table_name}")
        return

    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    schema, table = table_name.split(".")

    logger.info(f"Applying retention to {table_name}: deleting records before {cutoff_date}")

    import snowflake.connector
    conn = snowflake.connector.connect(
        account=snowflake_config.account,
        user=snowflake_config.user,
        password=snowflake_config.password,
        database=snowflake_config.database,
        warehouse=snowflake_config.warehouse,
        role=snowflake_config.role,
        schema=schema,
    )

    try:
        cursor = conn.cursor()

        # Count affected rows first
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {timestamp_column} < '{cutoff_date}'"
        )
        count = cursor.fetchone()[0]

        if count > 0:
            cursor.execute(
                f"DELETE FROM {table} WHERE {timestamp_column} < '{cutoff_date}'"
            )
            logger.info(f"Deleted {count:,} expired records from {table_name}")

            # Log the retention action for audit
            cursor.execute("""
                INSERT INTO RAW.RETENTION_AUDIT_LOG (table_name, records_deleted, cutoff_date, executed_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP())
            """, (table_name, count, cutoff_date))
        else:
            logger.info(f"No expired records in {table_name}")

    finally:
        conn.close()


def run_all_retention_policies():
    """Run retention for all tables with defined policies."""
    for table_name in RETENTION_POLICIES:
        try:
            timestamp_col = "_computed_at" if "CANONICAL" in table_name else "event_timestamp"
            if "QUARANTINED" in table_name:
                timestamp_col = "quarantined_at"
            apply_retention_policy(table_name, timestamp_col)
        except Exception:
            logger.exception(f"Retention failed for {table_name}")
