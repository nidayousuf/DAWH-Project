"""DAG 3 - Streaming ingestion layer.
Replays newly generated IoT sensor readings as a live stream (micro-batches POSTed to
/ingest/stream). Throughput, end-to-end latency and malformed counts land in
meta.stream_metrics. In Phase 3 this DAG is scheduled every 1-5 minutes."""
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dwh_common import DEFAULT_ARGS, START, TAGS


def _stream(**ctx):
    from ingestion.stream_producer import run
    p = ctx["params"]
    n = run(eps=p["events_per_second"], batch=p["micro_batch_size"],
            max_events=p["max_events_per_file"] or None)
    print(f"streamed {n:,} events")


def _summary():
    from common import db
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT run_label, count(*), sum(events_loaded), sum(events_malformed),
                              round(avg(throughput_eps),1), round(avg(avg_latency_sec),3)
                       FROM meta.stream_metrics GROUP BY 1 ORDER BY 1""")
        for r in cur.fetchall():
            print(r)


with DAG(
    dag_id="sdi_03_stream_ingestion",
    description="IoT micro-batch streaming into staging",
    default_args=DEFAULT_ARGS, start_date=START, schedule=None, catchup=False,
    tags=TAGS + ["streaming"], max_active_runs=1,
    params={"events_per_second": Param(400, type="number"),
            "micro_batch_size": Param(100, type="integer"),
            "max_events_per_file": Param(15000, type="integer")},
) as dag:
    s = PythonOperator(task_id="replay_sensor_stream", python_callable=_stream)
    m = PythonOperator(task_id="summarise_stream_metrics", python_callable=_summary)
    s >> m
