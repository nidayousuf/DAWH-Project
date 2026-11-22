"""DAG 5 - Analytical query layer & storage observability.
Runs the OLAP query catalogue (base vs optimised, timed and logged), captures storage metrics,
and signals the BI layer to refresh."""
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dwh_common import DEFAULT_ARGS, START, TAGS, refresh_label


def _queries(**ctx):
    from query.run_queries import run
    run(refresh_label(ctx), repeat=3, inject_error=ctx["params"]["include_negative_test"])


def _storage(**ctx):
    from monitoring.storage_metrics import capture
    capture(refresh_label(ctx))


def _bi_refresh(**ctx):
    """Power BI Desktop: press Refresh (DirectQuery visuals update by themselves).
    Power BI Service: call the dataset refresh REST API here, e.g.
    POST https://api.powerbi.com/v1.0/myorg/datasets/{id}/refreshes"""
    from common import db
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO meta.mart_refresh_log (mart, refresh_label, started_at,
                       finished_at, duration_seconds, object_name, status)
                       VALUES ('SDI', %s, now(), now(), 0, 'BI refresh signal', 'READY')""",
                    (refresh_label(ctx),))
    print("Mart published - BI dashboards can refresh now.")


with DAG(
    dag_id="sdi_05_query_monitoring",
    description="Timed OLAP queries + storage metrics + BI refresh signal",
    default_args=DEFAULT_ARGS, start_date=START, schedule=None, catchup=False, tags=TAGS,
    params={"refresh": Param(1, type="integer", minimum=0, maximum=4),
            "include_negative_test": Param(True, type="boolean")},
) as dag:
    q = PythonOperator(task_id="run_timed_olap_queries", python_callable=_queries)
    s = PythonOperator(task_id="capture_storage_metrics", python_callable=_storage)
    b = PythonOperator(task_id="signal_bi_refresh", python_callable=_bi_refresh)
    [q, s] >> b
