"""Custom Airflow sensors for the data platform."""

import os
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from minio import Minio


class MinIOFileSensor(BaseSensorOperator):
    """Sensor that pokes MinIO to check if files exist at a given prefix.

    Useful for waiting until streaming consumers have written staging data
    before triggering batch ETL jobs.
    """

    template_fields = ("prefix",)

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        prefix: str,
        min_objects: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.min_objects = min_objects

    def poke(self, context) -> bool:
        client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

        try:
            objects = list(client.list_objects(self.bucket, prefix=self.prefix, recursive=True))
            num_objects = len(objects)
            self.log.info(
                f"Found {num_objects} objects at s3://{self.bucket}/{self.prefix} "
                f"(need >= {self.min_objects})"
            )
            return num_objects >= self.min_objects
        except Exception as e:
            self.log.warning(f"Error checking MinIO: {e}")
            return False
