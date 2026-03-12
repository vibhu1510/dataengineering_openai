"""Faust streaming application — consumes Kafka topics and writes Parquet to MinIO."""

import os
import faust

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")

app = faust.App(
    "chatgpt-stream-processor",
    broker=f"kafka://{KAFKA_BROKER}",
    store="memory://",
    topic_replication_factor=1,
    stream_buffer_maxsize=10000,
)

# Import agents after app is created to avoid circular imports
from src.streaming.agents import (  # noqa: E402, F401
    user_events_agent,
    conversation_events_agent,
    api_usage_events_agent,
    billing_events_agent,
    safety_events_agent,
)
