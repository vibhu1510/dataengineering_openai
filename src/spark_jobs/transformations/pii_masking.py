"""PII masking transformations for compliance with data privacy requirements."""

import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

EMAIL_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
PHONE_PATTERN = r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"
SSN_PATTERN = r"\b\d{3}-\d{2}-\d{4}\b"
IP_PATTERN = r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"


@udf(returnType=StringType())
def mask_pii(text: str) -> str:
    """Mask PII patterns in free-text fields."""
    if text is None:
        return None
    text = re.sub(EMAIL_PATTERN, "[EMAIL_REDACTED]", text)
    text = re.sub(PHONE_PATTERN, "[PHONE_REDACTED]", text)
    text = re.sub(SSN_PATTERN, "[SSN_REDACTED]", text)
    text = re.sub(IP_PATTERN, "[IP_REDACTED]", text)
    return text


def apply_pii_masking(df: DataFrame, text_columns: list[str]) -> DataFrame:
    """Apply PII masking to specified text columns."""
    for col_name in text_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, mask_pii(col(col_name)))
    return df
