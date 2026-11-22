"""DAG 0 - Master orchestration for one refresh cycle of the SDI mart.

    oversampling -> batch ingestion -> stream ingestion -> SDI transform -> queries & monitoring

Trigger with {"refresh": 0} for the historical initial load (seed files are copied to the
landing folder, so they flow through the same watcher -> API path), then {"refresh": 1} and
{"refresh": 2} for the two demo refreshes.
"""
import shutil

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dwh_common import DEFAULT_ARGS, START, TAGS


def _branch(**ctx):
    return "stage_seed_files" if int(ctx["params"]["refresh"]) == 0 else "run_oversampling"


def _stage_seed(**_):
    from common import config
    # history (2023-2025) of all 7 sources, IoT included, goes through the batch path
    for p in sorted(config.SEED_DIR.glob("*.csv")):
        tmp = config.LANDING_DIR / (p.name + ".part")
        shutil.copy(p, tmp)
        tmp.rename(config.LANDING_DIR / p.name)


def trigger(task_id, dag_id, extra=None):
    conf = {"refresh": "{{ params.refresh }}"}
    conf.update(extra or {})
    return TriggerDagRunOperator(task_id=task_id, trigger_dag_id=dag_id, conf=conf,
                                 wait_for_completion=True, poke_interval=10,
                                 reset_dag_run=True, trigger_rule="none_failed_min_one_success")


with DAG(
    dag_id="sdi_00_master_pipeline",
    description="End-to-end refresh of the Sales & Demand Intelligence mart",
    default_args=DEFAULT_ARGS, start_date=START, schedule=None, catchup=False,
    tags=TAGS + ["master"], max_active_runs=1, render_template_as_native_obj=True,
    params={"refresh": Param(1, type="integer", minimum=0, maximum=4,
                             description="0 = historical load, 1..4 = generated quarter")},
) as dag:
    start = EmptyOperator(task_id="start")
    branch = BranchPythonOperator(task_id="initial_load_or_refresh", python_callable=_branch)
    seed = PythonOperator(task_id="stage_seed_files", python_callable=_stage_seed)
    gen = trigger("run_oversampling", "sdi_01_oversampling")
    batch = trigger("run_batch_ingestion", "sdi_02_batch_ingestion")
    stream = trigger("run_stream_ingestion", "sdi_03_stream_ingestion")
    sdi = trigger("run_sdi_transform", "sdi_04_sdi_transform")
    mon = trigger("run_queries_and_monitoring", "sdi_05_query_monitoring")
    end = EmptyOperator(task_id="refresh_complete")

    start >> branch >> [seed, gen]
    [seed, gen] >> batch >> stream >> sdi >> mon >> end
