"""Central configuration module. All settings read from environment variables."""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaConfig:
    broker: str = os.getenv("KAFKA_BROKER", "localhost:29092")
    topics: dict[str, str] = field(default_factory=lambda: {
        "user_events": "user_events",
        "conversation_events": "conversation_events",
        "api_usage_events": "api_usage_events",
        "billing_events": "billing_events",
        "safety_events": "safety_events",
    })


@dataclass(frozen=True)
class MinIOConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure: bool = False
    staging_bucket: str = "staging"
    processed_bucket: str = "processed"
    archive_bucket: str = "archive"

    @property
    def s3a_endpoint(self) -> str:
        return f"http://{self.endpoint}"


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    user: str = os.getenv("SNOWFLAKE_USER", "")
    password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    database: str = os.getenv("SNOWFLAKE_DATABASE", "CHATGPT_PLATFORM")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role: str = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

    @property
    def connection_params(self) -> dict:
        return {
            "sfURL": f"{self.account}.snowflakecomputing.com",
            "sfUser": self.user,
            "sfPassword": self.password,
            "sfDatabase": self.database,
            "sfWarehouse": self.warehouse,
            "sfRole": self.role,
        }


@dataclass(frozen=True)
class GeneratorConfig:
    num_users: int = int(os.getenv("GENERATOR_NUM_USERS", "1000"))
    events_per_second: int = int(os.getenv("GENERATOR_EVENTS_PER_SECOND", "50"))
    user_tiers: list[str] = field(default_factory=lambda: ["free", "plus", "enterprise"])
    tier_weights: list[float] = field(default_factory=lambda: [0.70, 0.25, 0.05])
    models: list[str] = field(default_factory=lambda: [
        "gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo", "o1-preview", "o1-mini",
    ])
    platforms: list[str] = field(default_factory=lambda: ["web", "ios", "android", "api"])


# Singleton instances
kafka_config = KafkaConfig()
minio_config = MinIOConfig()
snowflake_config = SnowflakeConfig()
generator_config = GeneratorConfig()
