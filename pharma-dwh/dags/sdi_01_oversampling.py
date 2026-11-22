"""DAG 1 - Data oversampling layer.
Generates one quarter of new records for all 7 source files. The six batch files are written
atomically to data/landing (picked up by the file-watcher); the IoT file goes to data/stream
(replayed by DAG 3)."""
import json

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dwh_common import DEFAULT_ARGS, START, TAGS


def _generate(**ctx):
    from generator.generate import generate
    stats = generate(int(ctx["params"]["refresh"]))
    ctx["ti"].xcom_push(key="stats", value=json.loads(json.dumps(stats, default=str)))
    return {k: v["rows"] for k, v in stats["files"].items()}


def _report(**ctx):
    stats = ctx["ti"].xcom_pull(task_ids="generate_new_records", key="stats")
    for k, v in stats["files"].items():
        print(f"{k:14s} {v['rows']:>9,} rows  {v.get('min_date', '')}..{v.get('max_date', '')}")
    print("SCD events:", stats["scd_changes"])
    print("Injected defects:", stats["sales_defects_injected"], stats["iot_defects_injected"])


with DAG(
    dag_id="sdi_01_oversampling",
    description="Synthesise a new quarter of data for the 7 source files",
    default_args=DEFAULT_ARGS, start_date=START, schedule=None, catchup=False, tags=TAGS,
    params={"refresh": Param(1, type="integer", minimum=1, maximum=4)},
) as dag:
    gen = PythonOperator(task_id="generate_new_records", python_callable=_generate)
    rep = PythonOperator(task_id="log_generation_statistics", python_callable=_report)
    gen >> rep
