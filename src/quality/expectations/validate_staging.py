"""Data quality validation for staging data using rule-based checks.

These checks run before data is loaded into the warehouse to catch issues early.
"""

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct

from src.spark_jobs.common.spark_session import get_spark_session
from src.config.settings import minio_config

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    check_name: str
    passed: bool
    actual_value: float
    expected: str
    message: str


# ---- Expectation definitions per topic ----
EXPECTATIONS = {
    "conversation_events": [
        {"check": "not_empty", "column": None},
        {"check": "column_exists", "column": "event_id"},
        {"check": "column_exists", "column": "user_id"},
        {"check": "column_exists", "column": "timestamp"},
        {"check": "column_exists", "column": "event_type"},
        {"check": "no_nulls", "column": "event_id"},
        {"check": "no_nulls", "column": "user_id"},
        {"check": "unique", "column": "event_id"},
        {"check": "values_in_set", "column": "event_type", "values": [
            "session_start", "message_sent", "conversation_rated", "session_end",
        ]},
        {"check": "column_between", "column": "input_tokens", "min": 0, "max": 200000},
    ],
    "api_usage_events": [
        {"check": "not_empty", "column": None},
        {"check": "no_nulls", "column": "event_id"},
        {"check": "no_nulls", "column": "user_id"},
        {"check": "unique", "column": "event_id"},
        {"check": "values_in_set", "column": "http_status_code", "values": [
            200, 400, 401, 403, 404, 429, 500, 502, 503,
        ]},
    ],
    "billing_events": [
        {"check": "not_empty", "column": None},
        {"check": "no_nulls", "column": "event_id"},
        {"check": "unique", "column": "event_id"},
        {"check": "column_between", "column": "amount_usd", "min": 0, "max": 10000},
    ],
    "user_events": [
        {"check": "not_empty", "column": None},
        {"check": "no_nulls", "column": "event_id"},
        {"check": "no_nulls", "column": "user_id"},
        {"check": "unique", "column": "event_id"},
        {"check": "column_matches_regex", "column": "user_id", "regex": r"^usr_[a-z0-9]{16}$"},
    ],
}


def _run_check(df: DataFrame, expectation: dict) -> QualityResult:
    """Execute a single quality check and return the result."""
    check = expectation["check"]
    column = expectation.get("column")

    if check == "not_empty":
        row_count = df.count()
        return QualityResult("not_empty", row_count > 0, row_count, "> 0", f"Row count: {row_count}")

    elif check == "column_exists":
        exists = column in df.columns
        return QualityResult(f"column_exists:{column}", exists, int(exists), "True", f"Column '{column}' exists: {exists}")

    elif check == "no_nulls":
        null_count = df.filter(col(column).isNull()).count()
        total = df.count()
        return QualityResult(f"no_nulls:{column}", null_count == 0, null_count, "0", f"{null_count}/{total} nulls")

    elif check == "unique":
        total = df.count()
        distinct = df.select(countDistinct(column)).collect()[0][0]
        return QualityResult(f"unique:{column}", total == distinct, distinct, str(total), f"{distinct}/{total} unique")

    elif check == "values_in_set":
        valid_values = expectation["values"]
        invalid = df.filter(~col(column).isin(valid_values)).count()
        return QualityResult(
            f"values_in_set:{column}", invalid == 0, invalid, "0",
            f"{invalid} invalid values"
        )

    elif check == "column_between":
        min_val, max_val = expectation["min"], expectation["max"]
        out_of_range = df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        return QualityResult(
            f"between:{column}", out_of_range == 0, out_of_range, "0",
            f"{out_of_range} out of range [{min_val}, {max_val}]"
        )

    elif check == "column_matches_regex":
        regex = expectation["regex"]
        non_matching = df.filter(~col(column).rlike(regex)).count()
        return QualityResult(
            f"regex:{column}", non_matching == 0, non_matching, "0",
            f"{non_matching} don't match pattern"
        )

    return QualityResult(f"unknown:{check}", False, 0, "", "Unknown check type")


def run(topic: str, execution_date: str) -> dict:
    """Run all quality checks for a topic's staging data."""
    spark = get_spark_session(f"quality-{topic}")
    staging_path = f"s3a://{minio_config.staging_bucket}/{topic}/dt={execution_date}/"

    try:
        df = spark.read.parquet(staging_path)
        expectations = EXPECTATIONS.get(topic, [])

        results = []
        for exp in expectations:
            result = _run_check(df, exp)
            status = "PASS" if result.passed else "FAIL"
            logger.info(f"  [{status}] {result.check_name}: {result.message}")
            results.append(result)

        all_passed = all(r.passed for r in results)
        passed = sum(1 for r in results if r.passed)

        logger.info(f"Quality check for {topic}: {passed}/{len(results)} passed")

        return {
            "success": all_passed,
            "results": [
                {"check": r.check_name, "passed": r.passed, "message": r.message}
                for r in results
            ],
        }

    finally:
        spark.stop()
