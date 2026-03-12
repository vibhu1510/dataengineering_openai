"""Event deduplication using window functions."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def deduplicate_events(
    df: DataFrame,
    id_column: str = "event_id",
    timestamp_column: str = "timestamp",
    order: str = "desc",
) -> DataFrame:
    """Remove duplicate events, keeping the latest version of each event_id.

    Uses a window function with row_number() partitioned by event_id,
    ordered by timestamp. This is more efficient than groupBy + agg
    because it preserves all columns without explicit listing.

    Args:
        df: Input DataFrame with potential duplicates
        id_column: Column to deduplicate on
        timestamp_column: Column to order by (latest wins)
        order: "desc" keeps latest, "asc" keeps earliest
    """
    window = Window.partitionBy(id_column).orderBy(
        col(timestamp_column).desc() if order == "desc" else col(timestamp_column).asc()
    )

    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
