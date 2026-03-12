"""Sessionization: group user events into sessions based on inactivity gaps."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lag, sum as spark_sum, when, unix_timestamp, lit,
)
from pyspark.sql.window import Window


def sessionize(
    df: DataFrame,
    user_column: str = "user_id",
    timestamp_column: str = "timestamp",
    gap_minutes: int = 30,
) -> DataFrame:
    """Assign session IDs based on inactivity gaps.

    Algorithm:
    1. Order events per user by timestamp
    2. Compute time gap from previous event
    3. If gap > threshold, mark as new session boundary
    4. Cumulative sum of boundaries = session number per user
    5. Create session_id = user_id + session_number

    Args:
        df: Input DataFrame with user events
        user_column: Column identifying the user
        timestamp_column: Timestamp column (ISO-8601 string or TimestampType)
        gap_minutes: Inactivity threshold in minutes to start new session
    """
    user_window = Window.partitionBy(user_column).orderBy(timestamp_column)

    gap_threshold = gap_minutes * 60  # Convert to seconds

    return (
        df
        # Compute seconds since previous event for this user
        .withColumn(
            "_prev_ts",
            lag(unix_timestamp(col(timestamp_column))).over(user_window),
        )
        .withColumn(
            "_gap_seconds",
            unix_timestamp(col(timestamp_column)) - col("_prev_ts"),
        )
        # Mark new session boundary where gap exceeds threshold or is first event
        .withColumn(
            "_new_session",
            when(
                col("_prev_ts").isNull() | (col("_gap_seconds") > gap_threshold),
                lit(1),
            ).otherwise(lit(0)),
        )
        # Cumulative sum of boundaries = session number
        .withColumn(
            "_session_num",
            spark_sum("_new_session").over(user_window),
        )
        # Create a deterministic session_id
        .withColumn(
            "computed_session_id",
            concat_session_id(col(user_column), col("_session_num")),
        )
        # Clean up temp columns
        .drop("_prev_ts", "_gap_seconds", "_new_session", "_session_num")
    )


def concat_session_id(user_col, session_num_col):
    """Create session_id by concatenating user_id and session number."""
    from pyspark.sql.functions import concat, lit, lpad
    return concat(user_col, lit("_sess_"), lpad(session_num_col.cast("string"), 6, "0"))
