"""Daily Batch ETL DAG — the main orchestration pipeline.

Schedule: 6 AM UTC daily
Pipeline: sensors -> pre-quality -> dim loads -> fact loads -> post-quality -> canonical builds

Features demonstrated:
- TaskGroups for logical organization
- Custom sensors (MinIO data availability)
- Retries with exponential backoff
- SLA monitoring
- Failure callbacks
- Task dependencies
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from dags.common.sensors import MinIOFileSensor
from dags.common.callbacks import sla_miss_callback, failure_callback, success_callback

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "email": ["data-eng-oncall@company.com"],
    "sla": timedelta(hours=2),
    "on_failure_callback": failure_callback,
    "on_success_callback": success_callback,
}

SPARK_SUBMIT = "spark-submit --master local[*] --driver-memory 2g"

with DAG(
    dag_id="daily_batch_etl",
    default_args=default_args,
    description="Daily ETL: staging -> warehouse facts/dims -> canonical tables",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "daily", "batch", "production"],
    sla_miss_callback=sla_miss_callback,
    doc_md="""
    ## Daily Batch ETL Pipeline

    **Owner**: Data Engineering Team
    **SLA**: 2 hours from scheduled start
    **Schedule**: 6:00 AM UTC daily

    ### Pipeline Steps:
    1. Wait for staging data availability in MinIO
    2. Run pre-load data quality checks
    3. Load dimension tables (SCD Type 2 for users)
    4. Load fact tables (conversations, API usage, billing)
    5. Run post-load quality validation
    6. Build canonical metric tables (DAU, engagement, revenue)
    """,
) as dag:

    # =========================================================================
    # Stage 1: Data Availability Sensors
    # =========================================================================
    with TaskGroup("data_availability_checks", tooltip="Wait for staging data in MinIO") as sensors:
        for topic in ["conversation_events", "api_usage_events", "billing_events", "user_events"]:
            MinIOFileSensor(
                task_id=f"wait_for_{topic}",
                bucket="staging",
                prefix=f"{topic}/dt={{{{ ds }}}}",
                min_objects=1,
                poke_interval=120,
                timeout=7200,
                mode="reschedule",
            )

    # =========================================================================
    # Stage 2: Pre-load Data Quality Checks
    # =========================================================================
    with TaskGroup("pre_load_quality", tooltip="Validate staging data before loading") as pre_quality:
        def run_staging_quality_check(topic: str, **context):
            """Run Great Expectations suite on staging data."""
            from src.quality.expectations import validate_staging
            execution_date = context["ds"]
            results = validate_staging.run(topic, execution_date)
            if not results["success"]:
                raise ValueError(f"Data quality check failed for {topic}: {results['results']}")

        for topic in ["conversation_events", "api_usage_events", "billing_events", "user_events"]:
            PythonOperator(
                task_id=f"validate_{topic}_staging",
                python_callable=run_staging_quality_check,
                op_kwargs={"topic": topic},
            )

    # =========================================================================
    # Stage 3: Dimension Loads
    # =========================================================================
    with TaskGroup("dimension_loads", tooltip="Load dimension tables (SCD Type 2)") as dim_loads:
        etl_users = BashOperator(
            task_id="etl_users_scd2",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/etl_users.py {{{{ ds }}}}",
        )

    # =========================================================================
    # Stage 4: Fact Loads (can run in parallel)
    # =========================================================================
    with TaskGroup("fact_loads", tooltip="Load fact tables from staging") as fact_loads:
        etl_conversations = BashOperator(
            task_id="etl_conversations",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/etl_conversations.py {{{{ ds }}}}",
        )
        etl_api_usage = BashOperator(
            task_id="etl_api_usage",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/etl_api_usage.py {{{{ ds }}}}",
        )
        etl_billing = BashOperator(
            task_id="etl_billing",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/etl_billing.py {{{{ ds }}}}",
        )

    # =========================================================================
    # Stage 5: Post-load Quality Validation
    # =========================================================================
    with TaskGroup("post_load_quality", tooltip="Validate warehouse data after loading") as post_quality:
        def run_warehouse_quality_check(**context):
            """Validate row counts and referential integrity in warehouse tables."""
            from src.quality.expectations import validate_warehouse
            validate_warehouse.run(context["ds"])

        PythonOperator(
            task_id="validate_warehouse_tables",
            python_callable=run_warehouse_quality_check,
        )

    # =========================================================================
    # Stage 6: Build Canonical Tables
    # =========================================================================
    with TaskGroup("canonical_builds", tooltip="Build business metric tables") as canonicals:
        build_dau = BashOperator(
            task_id="build_dau",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/build_canonical_dau.py {{{{ ds }}}}",
        )
        build_engagement = BashOperator(
            task_id="build_engagement",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/build_canonical_engagement.py {{{{ ds }}}}",
        )
        build_revenue = BashOperator(
            task_id="build_revenue",
            bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/spark_jobs/jobs/build_canonical_revenue.py {{{{ ds }}}}",
        )

    # =========================================================================
    # DAG Dependencies
    # =========================================================================
    sensors >> pre_quality >> dim_loads >> fact_loads >> post_quality >> canonicals
