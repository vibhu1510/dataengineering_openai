"""Airflow callback functions for alerting and SLA monitoring."""

import logging

logger = logging.getLogger(__name__)


def failure_callback(context):
    """Called when a task fails. In production, this would send to Slack/PagerDuty."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    alert_message = (
        f"ALERT: Task failed!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Error: {exception}\n"
        f"Log URL: {task_instance.log_url}"
    )
    logger.error(alert_message)
    # In production: send to Slack, PagerDuty, email, etc.


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when a DAG misses its SLA. Critical for data freshness guarantees."""
    dag_id = dag.dag_id
    missed_tasks = [str(t) for t in task_list]

    alert_message = (
        f"SLA MISS: DAG '{dag_id}' has exceeded its SLA!\n"
        f"Missed tasks: {', '.join(missed_tasks)}\n"
        f"Blocking tasks: {[str(t) for t in blocking_task_list]}\n"
        f"Action required: Check pipeline health and data freshness."
    )
    logger.warning(alert_message)


def success_callback(context):
    """Called on task success. Useful for metrics tracking."""
    task_instance = context.get("task_instance")
    duration = task_instance.duration
    logger.info(
        f"Task {task_instance.task_id} completed successfully in {duration:.1f}s"
    )
