"""CLI entry point for synthetic data generation.

Usage:
    python -m src.generators.runner --mode stream --duration-seconds 300 --num-users 500
    python -m src.generators.runner --mode batch --num-days 30 --num-users 1000
"""

import json
import random
import time
import uuid
import logging
from datetime import datetime, timezone, timedelta

import click
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.config.settings import kafka_config, generator_config
from src.generators.user_events import UserEventGenerator
from src.generators.conversation_events import ConversationEventGenerator
from src.generators.api_usage_events import APIUsageEventGenerator
from src.generators.billing_events import BillingEventGenerator
from src.generators.safety_events import SafetyEventGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_user_pool(num_users: int) -> list[dict]:
    """Create a pool of synthetic user profiles."""
    users = []
    for _ in range(num_users):
        tier = random.choices(
            generator_config.user_tiers,
            weights=generator_config.tier_weights,
            k=1,
        )[0]
        users.append({
            "user_id": f"usr_{uuid.uuid4().hex[:16]}",
            "tier": tier,
            "platform": random.choice(generator_config.platforms),
            "country_code": random.choice(["US", "GB", "DE", "JP", "BR", "IN", "FR", "CA"]),
            "organization_id": f"org_{uuid.uuid4().hex[:8]}" if tier == "enterprise" else None,
            "signup_date": (
                datetime.now(timezone.utc) - timedelta(days=random.randint(1, 730))
            ).isoformat(),
        })
    return users


def create_kafka_producer(broker: str, max_retries: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                linger_ms=10,
                batch_size=32768,
            )
            logger.info(f"Connected to Kafka broker: {broker}")
            return producer
        except NoBrokersAvailable:
            wait = 2 ** attempt
            logger.warning(f"Kafka not ready, retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
    raise ConnectionError(f"Could not connect to Kafka at {broker} after {max_retries} attempts")


def stream_events(producer: KafkaProducer, users: list[dict], duration_seconds: int):
    """Stream synthetic events to Kafka in real-time."""
    generators = [
        (UserEventGenerator(), 0.15),
        (ConversationEventGenerator(), 0.40),
        (APIUsageEventGenerator(), 0.25),
        (BillingEventGenerator(), 0.10),
        (SafetyEventGenerator(), 0.10),
    ]

    start_time = time.time()
    total_events = 0
    eps = generator_config.events_per_second

    logger.info(f"Streaming events for {duration_seconds}s at ~{eps} events/sec...")

    while time.time() - start_time < duration_seconds:
        batch_start = time.time()

        for _ in range(eps):
            user = random.choice(users)
            generator, _ = random.choices(
                generators,
                weights=[w for _, w in generators],
                k=1,
            )[0]

            events = generator.generate(user["user_id"], user)
            for event in events:
                producer.send(
                    topic=generator.topic,
                    key=event["user_id"],
                    value=event,
                )
                total_events += 1

        producer.flush()

        # Pace to ~1 second per batch
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        # Log progress every 10 seconds
        total_elapsed = time.time() - start_time
        if int(total_elapsed) % 10 == 0 and int(total_elapsed) > 0:
            logger.info(
                f"Progress: {int(total_elapsed)}s / {duration_seconds}s | "
                f"Events sent: {total_events:,} | "
                f"Rate: {total_events / total_elapsed:.0f} events/sec"
            )

    producer.flush()
    logger.info(f"Streaming complete. Total events sent: {total_events:,}")


def batch_events(users: list[dict], num_days: int, output_dir: str = "output/batch"):
    """Generate batch event files (JSON lines) for historical backfill."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    generators = [
        UserEventGenerator(),
        ConversationEventGenerator(),
        APIUsageEventGenerator(),
        BillingEventGenerator(),
        SafetyEventGenerator(),
    ]

    today = datetime.now(timezone.utc).date()
    total_events = 0

    for day_offset in range(num_days, 0, -1):
        date = today - timedelta(days=day_offset)
        day_dir = os.path.join(output_dir, f"dt={date.isoformat()}")
        os.makedirs(day_dir, exist_ok=True)

        for gen in generators:
            events = []
            # Each user has a probability of being active on any given day
            for user in users:
                if random.random() < 0.3:  # 30% DAU rate
                    events.extend(gen.generate(user["user_id"], user))

            if events:
                filepath = os.path.join(day_dir, f"{gen.topic}.jsonl")
                with open(filepath, "w") as f:
                    for event in events:
                        f.write(json.dumps(event, default=str) + "\n")
                total_events += len(events)

        if day_offset % 5 == 0:
            logger.info(f"Generated data for {date} | Running total: {total_events:,} events")

    logger.info(f"Batch generation complete. Total events: {total_events:,} across {num_days} days")


@click.command()
@click.option("--mode", type=click.Choice(["stream", "batch"]), required=True)
@click.option("--duration-seconds", type=int, default=300, help="Duration for streaming mode")
@click.option("--num-users", type=int, default=500, help="Size of synthetic user pool")
@click.option("--num-days", type=int, default=30, help="Number of days for batch mode")
@click.option("--broker", type=str, default=None, help="Kafka broker (overrides config)")
def main(mode: str, duration_seconds: int, num_users: int, num_days: int, broker: str):
    """Generate synthetic ChatGPT platform event data."""
    logger.info(f"Creating user pool with {num_users} users...")
    users = create_user_pool(num_users)

    tier_counts = {}
    for u in users:
        tier_counts[u["tier"]] = tier_counts.get(u["tier"], 0) + 1
    logger.info(f"User distribution: {tier_counts}")

    if mode == "stream":
        kafka_broker = broker or kafka_config.broker
        producer = create_kafka_producer(kafka_broker)
        try:
            stream_events(producer, users, duration_seconds)
        finally:
            producer.close()
    elif mode == "batch":
        batch_events(users, num_days)


if __name__ == "__main__":
    main()
