"""SCD Type 2 implementation for slowly changing dimensions in Spark.

This is one of the most technically impressive transformations for a data engineering
portfolio — it demonstrates deep understanding of dimensional modeling and advanced
Spark DataFrame operations.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, md5, concat_ws, when,
)


def apply_scd_type2(
    existing_dim: DataFrame,
    incoming_data: DataFrame,
    natural_key: str,
    tracked_columns: list[str],
    surrogate_key: str = "user_sk",
) -> DataFrame:
    """Apply SCD Type 2 merge logic.

    For each incoming record:
    - If the natural key doesn't exist in the dimension: INSERT as new (is_current=True)
    - If it exists and tracked columns haven't changed: no action
    - If it exists and tracked columns HAVE changed: EXPIRE old row, INSERT new version

    Args:
        existing_dim: Current dimension table (with valid_from, valid_to, is_current)
        incoming_data: New/updated records from staging
        natural_key: Business key column (e.g., "user_id")
        tracked_columns: Columns that trigger a new version when changed
        surrogate_key: Auto-increment surrogate key column name

    Returns:
        Updated dimension DataFrame ready to be written back
    """
    now = datetime.now(timezone.utc).isoformat()

    # Create hash of tracked columns for change detection
    existing_with_hash = existing_dim.withColumn(
        "_existing_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in tracked_columns])),
    )

    incoming_with_hash = incoming_data.withColumn(
        "_incoming_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in tracked_columns])),
    )

    # --- 1. Identify unchanged records (keep as-is) ---
    unchanged = (
        existing_with_hash.alias("e")
        .join(
            incoming_with_hash.alias("i"),
            (col(f"e.{natural_key}") == col(f"i.{natural_key}"))
            & (col("e.is_current") == True)  # noqa: E712
            & (col("e._existing_hash") == col("i._incoming_hash")),
            "inner",
        )
        .select("e.*")
        .drop("_existing_hash")
    )

    # --- 2. Identify changed records — expire old versions ---
    changed_existing = (
        existing_with_hash.alias("e")
        .join(
            incoming_with_hash.alias("i"),
            (col(f"e.{natural_key}") == col(f"i.{natural_key}"))
            & (col("e.is_current") == True)  # noqa: E712
            & (col("e._existing_hash") != col("i._incoming_hash")),
            "inner",
        )
        .select("e.*")
        .drop("_existing_hash")
        .withColumn("valid_to", lit(now))
        .withColumn("is_current", lit(False))
    )

    # --- 3. New versions of changed records ---
    changed_new = (
        incoming_with_hash.alias("i")
        .join(
            existing_with_hash.alias("e"),
            (col(f"e.{natural_key}") == col(f"i.{natural_key}"))
            & (col("e.is_current") == True)  # noqa: E712
            & (col("e._existing_hash") != col("i._incoming_hash")),
            "inner",
        )
        .select("i.*")
        .drop("_incoming_hash")
        .withColumn("valid_from", lit(now))
        .withColumn("valid_to", lit(None).cast("string"))
        .withColumn("is_current", lit(True))
    )

    # --- 4. Entirely new records (no match in existing) ---
    new_records = (
        incoming_with_hash.alias("i")
        .join(
            existing_dim.alias("e"),
            col(f"e.{natural_key}") == col(f"i.{natural_key}"),
            "left_anti",
        )
        .drop("_incoming_hash")
        .withColumn("valid_from", lit(now))
        .withColumn("valid_to", lit(None).cast("string"))
        .withColumn("is_current", lit(True))
    )

    # --- 5. Records not in incoming data (untouched history) ---
    # These are existing records whose natural key is NOT in the incoming batch
    untouched = (
        existing_dim.alias("e")
        .join(
            incoming_data.alias("i"),
            col(f"e.{natural_key}") == col(f"i.{natural_key}"),
            "left_anti",
        )
    )

    # Combine all parts
    result = (
        untouched
        .unionByName(unchanged, allowMissingColumns=True)
        .unionByName(changed_existing, allowMissingColumns=True)
        .unionByName(changed_new, allowMissingColumns=True)
        .unionByName(new_records, allowMissingColumns=True)
    )

    return result
