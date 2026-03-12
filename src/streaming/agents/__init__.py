"""Faust agents — one per Kafka topic, consuming events and writing to MinIO."""

import json
import logging

from src.streaming.app import app
from src.streaming.sinks.minio_parquet_sink import MinIOParquetSink

logger = logging.getLogger(__name__)

# Define topics
user_events_topic = app.topic("user_events", value_type=bytes)
conversation_events_topic = app.topic("conversation_events", value_type=bytes)
api_usage_events_topic = app.topic("api_usage_events", value_type=bytes)
billing_events_topic = app.topic("billing_events", value_type=bytes)
safety_events_topic = app.topic("safety_events", value_type=bytes)

# Create sinks
user_sink = MinIOParquetSink("user_events")
conversation_sink = MinIOParquetSink("conversation_events")
api_usage_sink = MinIOParquetSink("api_usage_events")
billing_sink = MinIOParquetSink("billing_events")
safety_sink = MinIOParquetSink("safety_events")


def _decode(value) -> dict:
    """Decode event from Kafka — handles both bytes and dict."""
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, dict):
        return value
    return json.loads(str(value))


@app.agent(user_events_topic)
async def user_events_agent(stream):
    async for event in stream:
        user_sink.add(_decode(event))


@app.agent(conversation_events_topic)
async def conversation_events_agent(stream):
    async for event in stream:
        conversation_sink.add(_decode(event))


@app.agent(api_usage_events_topic)
async def api_usage_events_agent(stream):
    async for event in stream:
        api_usage_sink.add(_decode(event))


@app.agent(billing_events_topic)
async def billing_events_agent(stream):
    async for event in stream:
        billing_sink.add(_decode(event))


@app.agent(safety_events_topic)
async def safety_events_agent(stream):
    async for event in stream:
        safety_sink.add(_decode(event))


@app.timer(interval=60.0)
async def periodic_flush():
    """Flush any remaining buffered events every 60 seconds."""
    for sink_name, sink in [
        ("user_events", user_sink),
        ("conversation_events", conversation_sink),
        ("api_usage_events", api_usage_sink),
        ("billing_events", billing_sink),
        ("safety_events", safety_sink),
    ]:
        if sink.buffer:
            logger.info(f"Periodic flush: {sink_name} has {len(sink.buffer)} buffered events")
            sink.flush()
