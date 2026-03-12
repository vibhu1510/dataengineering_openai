"""Shared pytest fixtures for the test suite."""

import pytest
from datetime import datetime, timezone


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing (local mode, minimal resources)."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("chatgpt-platform-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_conversation_events(spark):
    """Sample conversation events with duplicates for testing."""
    data = [
        {"event_id": "e1", "event_type": "message_sent", "user_id": "usr_abc123",
         "timestamp": "2026-03-10T10:00:00", "session_id": "sess_001",
         "model_id": "gpt-4o", "input_tokens": 100, "output_tokens": 200,
         "total_tokens": 300, "latency_ms": 500, "platform": "web"},
        {"event_id": "e1", "event_type": "message_sent", "user_id": "usr_abc123",
         "timestamp": "2026-03-10T10:00:01", "session_id": "sess_001",
         "model_id": "gpt-4o", "input_tokens": 100, "output_tokens": 200,
         "total_tokens": 300, "latency_ms": 500, "platform": "web"},
        {"event_id": "e2", "event_type": "message_sent", "user_id": "usr_abc123",
         "timestamp": "2026-03-10T10:05:00", "session_id": "sess_001",
         "model_id": "gpt-4o", "input_tokens": 150, "output_tokens": 300,
         "total_tokens": 450, "latency_ms": 700, "platform": "web"},
        {"event_id": "e3", "event_type": "message_sent", "user_id": "usr_abc123",
         "timestamp": "2026-03-10T11:00:00", "session_id": "sess_002",
         "model_id": "gpt-4o-mini", "input_tokens": 50, "output_tokens": 100,
         "total_tokens": 150, "latency_ms": 200, "platform": "web"},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def sample_user_events(spark):
    """Sample user events for SCD Type 2 testing."""
    data = [
        {"user_id": "usr_001", "email_hash": "abc123", "display_name": "alice",
         "country_code": "US", "user_tier": "free", "organization_id": None,
         "account_status": "active"},
        {"user_id": "usr_002", "email_hash": "def456", "display_name": "bob",
         "country_code": "GB", "user_tier": "plus", "organization_id": None,
         "account_status": "active"},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def sample_safety_events(spark):
    """Sample safety events for anomaly detection testing."""
    data = [
        {"user_id": "usr_normal1", "violation_type": "content_policy_violation",
         "severity": "low", "confidence_score": 0.4, "user_violation_count_30d": 1},
        {"user_id": "usr_normal2", "violation_type": "rate_limit_abuse",
         "severity": "low", "confidence_score": 0.3, "user_violation_count_30d": 2},
        {"user_id": "usr_abuser", "violation_type": "prompt_injection",
         "severity": "critical", "confidence_score": 0.95, "user_violation_count_30d": 50},
    ]
    return spark.createDataFrame(data)
