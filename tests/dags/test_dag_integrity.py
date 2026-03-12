"""DAG integrity tests — verify all DAGs load without errors."""

import os
import sys
import pytest

# Add project root to path so DAG imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


@pytest.fixture(scope="module")
def dag_bag():
    """Load all DAGs for testing."""
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

    from airflow.models import DagBag
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dag_bag):
    """All DAGs should load without import errors."""
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"


def test_daily_batch_etl_exists(dag_bag):
    assert "daily_batch_etl" in dag_bag.dags


def test_hourly_safety_monitoring_exists(dag_bag):
    assert "hourly_safety_monitoring" in dag_bag.dags


def test_daily_etl_schedule(dag_bag):
    dag = dag_bag.get_dag("daily_batch_etl")
    assert dag.schedule_interval == "0 6 * * *"


def test_hourly_safety_schedule(dag_bag):
    dag = dag_bag.get_dag("hourly_safety_monitoring")
    assert dag.schedule_interval == "@hourly"


def test_daily_etl_has_required_tasks(dag_bag):
    dag = dag_bag.get_dag("daily_batch_etl")
    task_ids = [t.task_id for t in dag.tasks]
    # Check key tasks exist (they may be nested in task groups)
    task_id_str = " ".join(task_ids)
    assert "etl_users_scd2" in task_id_str or "dimension_loads" in task_id_str
    assert "etl_conversations" in task_id_str or "fact_loads" in task_id_str


def test_daily_etl_no_cycles(dag_bag):
    """DAG should be a valid DAG (no circular dependencies)."""
    dag = dag_bag.get_dag("daily_batch_etl")
    # If this doesn't raise, the DAG is acyclic
    assert dag is not None


def test_all_dags_have_owner(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get("owner"), f"DAG {dag_id} has no owner"
