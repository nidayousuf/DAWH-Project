"""DAG 2 - Batch ingestion layer.
The file-watcher service pushes each landing file through the ingestion API into Postgres
staging. This DAG orchestrates around it: checks the API is up, waits until the landing
folder has drained, then verifies the load (no FAILED batches, reject counts reported).
If the watcher is down, `fallback_direct_ingest` loads any files still waiting."""
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from dwh_common import DEFAULT_ARGS, START, TAGS


def _api_health():
    import requests
    from common import config
    r = requests.get(f"{config.INGEST_API_URL}/health", timeout=10)
    r.raise_for_status()
    print(r.json())


def _landing_drained():
    from common import config, db
    waiting = [p for p in config.LANDING_DIR.iterdir() if p.suffix in (".csv", ".xlsx")]
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM meta.batch_ingestion_log WHERE status='RUNNING'")
        running = cur.fetchone()[0]
    print(f"{len(waiting)} files waiting, {running} loads running")
    return not waiting and running == 0


def _fallback(**_):
    import shutil
    from common import config
    from ingestion.loader import ingest_file
    for p in sorted(config.LANDING_DIR.glob("*.csv")):
        print(ingest_file(p, triggered_by="airflow-fallback"))
        shutil.move(str(p), config.ARCHIVE_DIR / p.name)


def _verify(**ctx):
    from common import db
    label = f"refresh_{ctx['params']['refresh']}" if ctx["params"]["refresh"] else "historical"
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT file_name, status, rows_read, rows_loaded, rows_rejected,
                              rows_skipped, duration_seconds
                       FROM meta.batch_ingestion_log WHERE refresh_label=%s ORDER BY batch_id""",
                    (label,))
        rows = cur.fetchall()
    for r in rows:
        print(r)
    if not rows:
        raise ValueError(f"No batches logged for {label}")
    failed = [r[0] for r in rows if r[1] == "FAILED"]
    if failed:
        raise RuntimeError(f"Failed loads: {failed}")


with DAG(
    dag_id="sdi_02_batch_ingestion",
    description="File-watcher -> API -> Postgres staging, then verify",
    default_args=DEFAULT_ARGS, start_date=START, schedule=None, catchup=False, tags=TAGS,
    params={"refresh": Param(1, type="integer", minimum=0, maximum=4)},
) as dag:
    health = PythonOperator(task_id="check_ingestion_api", python_callable=_api_health)
    wait = PythonSensor(task_id="wait_for_landing_to_drain", python_callable=_landing_drained,
                        poke_interval=10, timeout=1800, mode="reschedule", soft_fail=True)
    fallback = PythonOperator(task_id="fallback_direct_ingest", python_callable=_fallback,
                              trigger_rule="all_done")
    verify = PythonOperator(task_id="verify_batch_results", python_callable=_verify)
    health >> wait >> fallback >> verify
