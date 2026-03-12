"""Run all DDL scripts against Snowflake to set up the data warehouse."""

import os
import logging
from pathlib import Path

import snowflake.connector

from src.config.settings import snowflake_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DDL_DIR = Path(__file__).parent.parent / "src" / "warehouse" / "ddl"


def run():
    conn = snowflake.connector.connect(
        account=snowflake_config.account,
        user=snowflake_config.user,
        password=snowflake_config.password,
        warehouse=snowflake_config.warehouse,
        role=snowflake_config.role,
    )

    try:
        cursor = conn.cursor()
        ddl_files = sorted(DDL_DIR.glob("*.sql"))

        for ddl_file in ddl_files:
            logger.info(f"Executing {ddl_file.name}...")
            sql = ddl_file.read_text()

            # Split by semicolons and execute each statement
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    try:
                        cursor.execute(statement)
                        logger.info(f"  OK: {statement[:80]}...")
                    except Exception as e:
                        logger.warning(f"  WARN: {e}")

        logger.info("Snowflake setup complete!")

        # Seed dim_models with known models
        _seed_dim_models(cursor)
        _seed_dim_plans(cursor)

    finally:
        conn.close()


def _seed_dim_models(cursor):
    """Seed the model dimension with known ChatGPT models."""
    models = [
        ("gpt-4o", "gpt-4", "4o-2024-08-06", 128000, 16384, 2.50, 10.00, "2024-05-13"),
        ("gpt-4o-mini", "gpt-4", "4o-mini-2024-07-18", 128000, 16384, 0.15, 0.60, "2024-07-18"),
        ("gpt-4-turbo", "gpt-4", "turbo-2024-04-09", 128000, 4096, 10.00, 30.00, "2024-04-09"),
        ("gpt-3.5-turbo", "gpt-3.5", "0125", 16385, 4096, 0.50, 1.50, "2024-01-25"),
        ("o1-preview", "o1", "preview-2024-09-12", 128000, 32768, 15.00, 60.00, "2024-09-12"),
        ("o1-mini", "o1", "mini-2024-09-12", 128000, 65536, 3.00, 12.00, "2024-09-12"),
    ]

    cursor.execute("USE DATABASE CHATGPT_PLATFORM")
    cursor.execute("USE SCHEMA WAREHOUSE")

    for m in models:
        cursor.execute("""
            MERGE INTO DIM_MODELS t USING (SELECT %s AS model_id) s ON t.model_id = s.model_id
            WHEN NOT MATCHED THEN INSERT (model_id, model_family, model_version, context_window,
                max_output_tokens, cost_per_1k_input, cost_per_1k_output, release_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (m[0], *m))

    logger.info(f"Seeded {len(models)} models into DIM_MODELS")


def _seed_dim_plans(cursor):
    """Seed subscription plan dimension."""
    plans = [
        ("plan_free", "Free", 0.0, 0.0, 10, 10000),
        ("plan_plus", "ChatGPT Plus", 20.0, 200.0, 80, 1000000),
        ("plan_enterprise", "Enterprise", 60.0, 600.0, 500, 10000000),
    ]

    for p in plans:
        cursor.execute("""
            MERGE INTO DIM_SUBSCRIPTION_PLANS t USING (SELECT %s AS plan_id) s ON t.plan_id = s.plan_id
            WHEN NOT MATCHED THEN INSERT (plan_id, plan_name, monthly_price_usd, annual_price_usd,
                rate_limit_rpm, max_tokens_per_day, valid_from)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE)
        """, (p[0], *p))

    logger.info(f"Seeded {len(plans)} plans into DIM_SUBSCRIPTION_PLANS")


if __name__ == "__main__":
    run()
