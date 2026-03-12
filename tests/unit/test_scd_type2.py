"""Tests for SCD Type 2 dimension merge."""

from pyspark.sql.functions import lit

from src.spark_jobs.transformations.scd_type2 import apply_scd_type2


def test_scd2_new_record_inserted(spark, sample_user_events):
    """New users should be inserted with is_current=True."""
    # Empty existing dimension
    existing = spark.createDataFrame([], sample_user_events.schema).withColumns({
        "valid_from": lit("2026-01-01T00:00:00"),
        "valid_to": lit(None).cast("string"),
        "is_current": lit(True),
    })

    result = apply_scd_type2(
        existing_dim=existing,
        incoming_data=sample_user_events,
        natural_key="user_id",
        tracked_columns=["user_tier", "country_code"],
    )

    assert result.count() == 2
    assert result.filter(result.is_current == True).count() == 2  # noqa: E712


def test_scd2_unchanged_record_not_duplicated(spark, sample_user_events):
    """Re-ingesting the same data should not create new versions."""
    existing = sample_user_events.withColumns({
        "valid_from": lit("2026-01-01T00:00:00"),
        "valid_to": lit(None).cast("string"),
        "is_current": lit(True),
    })

    result = apply_scd_type2(
        existing_dim=existing,
        incoming_data=sample_user_events,
        natural_key="user_id",
        tracked_columns=["user_tier", "country_code"],
    )

    # Should still have exactly 2 rows, all current
    assert result.count() == 2
    assert result.filter(result.is_current == True).count() == 2  # noqa: E712


def test_scd2_changed_record_creates_new_version(spark):
    """Changing a tracked column should expire old row and insert new one."""
    existing_data = [
        {"user_id": "usr_001", "user_tier": "free", "country_code": "US",
         "email_hash": "a", "display_name": "alice", "organization_id": None,
         "account_status": "active",
         "valid_from": "2026-01-01T00:00:00", "valid_to": None, "is_current": True},
    ]
    existing = spark.createDataFrame(existing_data)

    incoming_data = [
        {"user_id": "usr_001", "user_tier": "plus", "country_code": "US",
         "email_hash": "a", "display_name": "alice", "organization_id": None,
         "account_status": "active"},
    ]
    incoming = spark.createDataFrame(incoming_data)

    result = apply_scd_type2(
        existing_dim=existing,
        incoming_data=incoming,
        natural_key="user_id",
        tracked_columns=["user_tier", "country_code"],
    )

    # Should have 2 rows: old (expired) + new (current)
    assert result.count() == 2
    assert result.filter(result.is_current == True).count() == 1  # noqa: E712
    assert result.filter(result.is_current == False).count() == 1  # noqa: E712

    # The current row should have the new tier
    current = result.filter(result.is_current == True).collect()[0]  # noqa: E712
    assert current.user_tier == "plus"
