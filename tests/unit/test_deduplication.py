"""Tests for event deduplication transformation."""

from src.spark_jobs.transformations.deduplication import deduplicate_events


def test_deduplicate_removes_exact_duplicates(sample_conversation_events):
    result = deduplicate_events(sample_conversation_events)
    assert result.count() == 3  # e1 (deduped), e2, e3


def test_deduplicate_keeps_latest_by_default(sample_conversation_events):
    result = deduplicate_events(sample_conversation_events)
    e1_row = result.filter(result.event_id == "e1").collect()[0]
    assert e1_row.timestamp == "2026-03-10T10:00:01"


def test_deduplicate_keeps_earliest_when_asc(sample_conversation_events):
    result = deduplicate_events(sample_conversation_events, order="asc")
    e1_row = result.filter(result.event_id == "e1").collect()[0]
    assert e1_row.timestamp == "2026-03-10T10:00:00"


def test_deduplicate_preserves_all_columns(sample_conversation_events):
    result = deduplicate_events(sample_conversation_events)
    assert set(result.columns) == set(sample_conversation_events.columns)


def test_deduplicate_no_duplicates_noop(spark):
    data = [
        {"event_id": "a1", "timestamp": "2026-03-10T10:00:00"},
        {"event_id": "a2", "timestamp": "2026-03-10T10:01:00"},
    ]
    df = spark.createDataFrame(data)
    result = deduplicate_events(df)
    assert result.count() == 2
