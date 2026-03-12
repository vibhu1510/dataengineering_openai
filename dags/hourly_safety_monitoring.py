"""Hourly Safety Monitoring DAG — detect and respond to abuse in near-real-time.

Schedule: Every hour
Pipeline: detect anomalies -> update quarantine -> refresh safety dashboard
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from dags.common.callbacks import failure_callback

default_args = {
    "owner": "trust-and-safety",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": failure_callback,
}

SPARK_SUBMIT = "spark-submit --master local[*] --driver-memory 1g"

with DAG(
    dag_id="hourly_safety_monitoring",
    default_args=default_args,
    description="Hourly safety monitoring: anomaly detection and quarantine management",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["safety", "hourly", "monitoring"],
    doc_md="""
    ## Hourly Safety Monitoring

    **Owner**: Trust & Safety Team
    **Schedule**: Every hour

    Detects anomalous user behavior and manages account quarantine:
    1. Run anomaly detection on recent safety events
    2. Update quarantine list for flagged accounts
    3. Refresh the safety monitoring dashboard
    """,
) as dag:

    def detect_anomalies(**context):
        """Run anomaly detection on the last hour of safety events."""
        from src.safety.detectors.anomaly_detector import run_hourly_detection
        execution_time = context["ts"]
        results = run_hourly_detection(execution_time)
        context["ti"].xcom_push(key="anomaly_count", value=results.get("anomaly_count", 0))

    def update_quarantine(**context):
        """Update the quarantine list based on detected anomalies."""
        from src.safety.detectors.quarantine_manager import update_quarantine_table
        anomaly_count = context["ti"].xcom_pull(key="anomaly_count", task_ids="detect_anomalies")
        if anomaly_count and anomaly_count > 0:
            update_quarantine_table(context["ts"])

    detect = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    quarantine = PythonOperator(
        task_id="update_quarantine_list",
        python_callable=update_quarantine,
    )

    refresh_dashboard = BashOperator(
        task_id="refresh_safety_dashboard",
        bash_command=(
            f"{SPARK_SUBMIT} "
            "/opt/airflow/src/spark_jobs/jobs/build_safety_dashboard.py {{ ts }}"
        ),
    )

    detect >> quarantine >> refresh_dashboard
