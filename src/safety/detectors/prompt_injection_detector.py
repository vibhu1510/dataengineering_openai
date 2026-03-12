"""Detect prompt injection attempts in user messages.

Uses pattern matching and heuristics to score the risk of prompt injection.
In production, this would be augmented with ML classifiers.
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, when, lit
from pyspark.sql.types import FloatType, StringType

# Known prompt injection patterns (regex)
INJECTION_PATTERNS = [
    (r"ignore\s+(all\s+)?previous\s+instructions", 0.9, "ignore_instructions"),
    (r"ignore\s+(all\s+)?prior\s+instructions", 0.9, "ignore_instructions"),
    (r"you\s+are\s+now\s+", 0.7, "role_override"),
    (r"pretend\s+(you\s+are|to\s+be)\s+", 0.7, "role_override"),
    (r"act\s+as\s+if\s+you", 0.6, "role_override"),
    (r"system:\s*", 0.8, "system_prompt_leak"),
    (r"<\|im_start\|>system", 0.95, "delimiter_attack"),
    (r"<\|endoftext\|>", 0.85, "delimiter_attack"),
    (r"ADMIN\s+OVERRIDE", 0.9, "authority_claim"),
    (r"DAN\s+mode", 0.85, "jailbreak"),
    (r"do\s+anything\s+now", 0.8, "jailbreak"),
    (r"developer\s+mode", 0.75, "jailbreak"),
    (r"bypass\s+(safety|content)\s+filter", 0.9, "filter_bypass"),
    (r"base64\s+decode", 0.6, "encoding_bypass"),
    (r"\\x[0-9a-fA-F]{2}", 0.5, "encoding_bypass"),
]


def score_injection_risk(text: str) -> tuple[float, str]:
    """Score a text for prompt injection risk.

    Returns (risk_score: 0-1, highest_pattern_match).
    """
    if not text:
        return 0.0, "none"

    max_score = 0.0
    max_pattern = "none"

    for pattern, weight, category in INJECTION_PATTERNS:
        matches = len(re.findall(pattern, text, re.IGNORECASE))
        if matches > 0:
            score = min(weight * matches, 1.0)
            if score > max_score:
                max_score = score
                max_pattern = category

    return min(max_score, 1.0), max_pattern


@udf(returnType=FloatType())
def injection_risk_score_udf(text: str) -> float:
    """Spark UDF for injection risk scoring."""
    if text is None:
        return 0.0
    score, _ = score_injection_risk(text)
    return score


@udf(returnType=StringType())
def injection_pattern_udf(text: str) -> str:
    """Spark UDF for identifying the injection pattern type."""
    if text is None:
        return "none"
    _, pattern = score_injection_risk(text)
    return pattern


def detect_injections(df: DataFrame, text_column: str, threshold: float = 0.5) -> DataFrame:
    """Add injection risk columns to a DataFrame.

    Adds:
    - injection_risk_score (0.0-1.0)
    - injection_pattern (category of detected pattern)
    - is_injection_attempt (boolean, True if score >= threshold)
    """
    return (
        df
        .withColumn("injection_risk_score", injection_risk_score_udf(col(text_column)))
        .withColumn("injection_pattern", injection_pattern_udf(col(text_column)))
        .withColumn(
            "is_injection_attempt",
            col("injection_risk_score") >= threshold,
        )
    )
