"""Batched Parquet writer that flushes event buffers to MinIO as Parquet files."""

import io
import logging
from datetime import datetime, timezone
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from src.config.settings import minio_config

logger = logging.getLogger(__name__)

FLUSH_THRESHOLD = 1000  # Events per flush
FLUSH_INTERVAL_SECONDS = 60


class MinIOParquetSink:
    """Buffers events in memory and writes Parquet files to MinIO when threshold is reached."""

    def __init__(self, topic: str, bucket: str = None):
        self.topic = topic
        self.bucket = bucket or minio_config.staging_bucket
        self.buffer: list[dict] = []
        self.client = Minio(
            minio_config.endpoint,
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
            secure=minio_config.secure,
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")

    def add(self, event: dict):
        """Add an event to the buffer. Flushes if threshold reached."""
        self.buffer.append(self._flatten_event(event))
        if len(self.buffer) >= FLUSH_THRESHOLD:
            self.flush()

    def _flatten_event(self, event: dict) -> dict:
        """Flatten nested payload into top-level columns for Parquet."""
        flat = {
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "timestamp": event.get("timestamp"),
            "user_id": event.get("user_id"),
        }
        payload = event.get("payload", {})
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                import json
                flat[key] = json.dumps(value)
            else:
                flat[key] = value
        return flat

    def flush(self):
        """Write buffered events to MinIO as a Parquet file."""
        if not self.buffer:
            return

        now = datetime.now(timezone.utc)
        partition_path = f"dt={now.strftime('%Y-%m-%d')}/hr={now.strftime('%H')}"
        object_name = (
            f"{self.topic}/{partition_path}/"
            f"{self.topic}_{now.strftime('%Y%m%d_%H%M%S')}_{len(self.buffer)}.parquet"
        )

        try:
            table = pa.Table.from_pylist(self.buffer)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            self.client.put_object(
                self.bucket,
                object_name,
                buf,
                length=buf.getbuffer().nbytes,
                content_type="application/octet-stream",
            )

            logger.info(
                f"Flushed {len(self.buffer)} events to "
                f"s3://{self.bucket}/{object_name} "
                f"({buf.getbuffer().nbytes:,} bytes)"
            )
            self.buffer.clear()

        except Exception:
            logger.exception(f"Failed to flush {len(self.buffer)} events for topic {self.topic}")
