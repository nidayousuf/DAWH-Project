"""DAG 4 - Transformation & dimensional modelling (SDI data mart).
Incremental extract from Postgres staging into DuckDB, then SCD-aware dimension loads,
fact loads with point-in-time surrogate-key lookups, pre-aggregates, quality gates, and
publication of the star schema to Postgres for Power BI."""
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dwh_common import DEFAULT_ARGS, START, TAGS, refresh_label


def _sync(**_):
    from transform.run_sdi import connect, sync_all
    con = connect()
    sync_all(con)
    con.close()


def _step(step):
    def fn(**ctx):
        from transform.run_sdi import build, connect
        con = connect()
        build(con, refresh_label(ctx), step)
        con.close()
    return fn


def _dq(**_):
    from transform.run_sdi import connect, quality_checks
    con = connect()
    quality_checks(con)
    con.close()


def _publish(**_):
    from transform.run_sdi import connect, publish
    con = connect()
    publish(con)
    con.close()


with DAG(
    dag_id="sdi_04_sdi_transform",
    description="Staging -> DuckDB star schema (SDI mart) -> Postgres sdi schema",
    default_args=DEFAULT_ARGS, start_date=START, schedule=None, catchup=False, tags=TAGS,
    max_active_runs=1, params={"refresh": Param(1, type="integer", minimum=0, maximum=4)},
) as dag:
    sync = PythonOperator(task_id="extract_new_staging_batches", python_callable=_sync)
    dims = PythonOperator(task_id="load_dimensions_scd", python_callable=_step("dimensions"))
    facts = PythonOperator(task_id="load_facts_incremental", python_callable=_step("facts"))
    aggs = PythonOperator(task_id="build_aggregates", python_callable=_step("aggregates"))
    dq = PythonOperator(task_id="data_quality_gates", python_callable=_dq)
    pub = PythonOperator(task_id="publish_star_to_postgres", python_callable=_publish)
    sync >> dims >> facts >> aggs >> dq >> pub
