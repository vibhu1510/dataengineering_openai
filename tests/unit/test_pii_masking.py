"""Tests for PII masking transformation."""

from src.spark_jobs.transformations.pii_masking import apply_pii_masking


def test_masks_email_addresses(spark):
    data = [{"text_field": "Contact me at user@example.com for details"}]
    df = spark.createDataFrame(data)
    result = apply_pii_masking(df, ["text_field"])
    text = result.collect()[0]["text_field"]
    assert "[EMAIL_REDACTED]" in text
    assert "user@example.com" not in text


def test_masks_phone_numbers(spark):
    data = [{"text_field": "Call me at 555-123-4567"}]
    df = spark.createDataFrame(data)
    result = apply_pii_masking(df, ["text_field"])
    text = result.collect()[0]["text_field"]
    assert "[PHONE_REDACTED]" in text
    assert "555-123-4567" not in text


def test_masks_ssn(spark):
    data = [{"text_field": "My SSN is 123-45-6789"}]
    df = spark.createDataFrame(data)
    result = apply_pii_masking(df, ["text_field"])
    text = result.collect()[0]["text_field"]
    assert "[SSN_REDACTED]" in text


def test_masks_ip_addresses(spark):
    data = [{"text_field": "Server at 192.168.1.100"}]
    df = spark.createDataFrame(data)
    result = apply_pii_masking(df, ["text_field"])
    text = result.collect()[0]["text_field"]
    assert "[IP_REDACTED]" in text


def test_handles_null_values(spark):
    data = [{"text_field": None}]
    df = spark.createDataFrame(data)
    result = apply_pii_masking(df, ["text_field"])
    assert result.collect()[0]["text_field"] is None


def test_skips_nonexistent_columns(spark):
    data = [{"text_field": "no PII here"}]
    df = spark.createDataFrame(data)
    result = apply_pii_masking(df, ["nonexistent_column"])
    assert result.count() == 1
