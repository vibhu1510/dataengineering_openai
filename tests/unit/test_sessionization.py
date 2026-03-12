"""Tests for sessionization transformation."""

from src.spark_jobs.transformations.sessionization import sessionize


def test_sessionize_assigns_session_ids(spark):
    data = [
        {"user_id": "u1", "timestamp": "2026-03-10T10:00:00"},
        {"user_id": "u1", "timestamp": "2026-03-10T10:10:00"},  # within 30 min
        {"user_id": "u1", "timestamp": "2026-03-10T11:00:00"},  # 50 min gap -> new session
    ]
    df = spark.createDataFrame(data)
    result = sessionize(df, gap_minutes=30)

    assert "computed_session_id" in result.columns
    sessions = result.select("computed_session_id").distinct().count()
    assert sessions == 2


def test_sessionize_different_users_get_different_sessions(spark):
    data = [
        {"user_id": "u1", "timestamp": "2026-03-10T10:00:00"},
        {"user_id": "u2", "timestamp": "2026-03-10T10:00:00"},
    ]
    df = spark.createDataFrame(data)
    result = sessionize(df)

    sessions = result.select("computed_session_id").distinct().count()
    assert sessions == 2


def test_sessionize_single_event_gets_session(spark):
    data = [{"user_id": "u1", "timestamp": "2026-03-10T10:00:00"}]
    df = spark.createDataFrame(data)
    result = sessionize(df)

    assert result.count() == 1
    assert result.collect()[0]["computed_session_id"] is not None


def test_sessionize_custom_gap(spark):
    data = [
        {"user_id": "u1", "timestamp": "2026-03-10T10:00:00"},
        {"user_id": "u1", "timestamp": "2026-03-10T10:06:00"},  # 6 min gap
    ]
    df = spark.createDataFrame(data)

    # With 5-min gap, should be 2 sessions
    result_5min = sessionize(df, gap_minutes=5)
    assert result_5min.select("computed_session_id").distinct().count() == 2

    # With 10-min gap, should be 1 session
    result_10min = sessionize(df, gap_minutes=10)
    assert result_10min.select("computed_session_id").distinct().count() == 1
