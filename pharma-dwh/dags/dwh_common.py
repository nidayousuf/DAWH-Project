"""Shared settings for all pipeline DAGs (not a DAG itself)."""
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/pharma-dwh")   # project code is mounted here in the Airflow containers

DEFAULT_ARGS = {
    "owner": "dwh-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=30),
}
START = datetime(2026, 11, 1)
TAGS = ["pharma_dwh", "phase1-sdi"]


def refresh_label(context) -> str:
    """'refresh_1' when the run was triggered with {"refresh": 1}; 'historical' for 0."""
    r = int((context.get("params") or {}).get("refresh", 0) or 0)
    return "historical" if r == 0 else f"refresh_{r}"
