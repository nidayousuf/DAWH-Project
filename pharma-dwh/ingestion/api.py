"""Ingestion API (FastAPI) - the single entry point into the Operational Data Store.

  GET  /health                 liveness probe
  POST /ingest/batch           load a file that is already in the landing folder
  POST /ingest/upload          push a file over HTTP (saved to landing, then loaded)
  POST /ingest/stream          micro-batch of IoT events (streaming ingestion)
  GET  /metrics/batch          last 50 batch loads (JSON)

Run:  uvicorn ingestion.api:app --host 0.0.0.0 --port 8000
"""
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel

from common import config, db
from common.schema import SOURCES, business_rules, coerce_types
from ingestion.loader import ingest_file

app = FastAPI(title="Pharma DWH Ingestion API", version="1.0")
db.init_metadata()


class BatchRequest(BaseModel):
    path: str
    triggered_by: str = "api"


class StreamRequest(BaseModel):
    run_label: str = "stream"
    window_start: datetime | None = None
    events: list[dict[str, Any]]


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/ingest/batch")
def ingest_batch(req: BatchRequest):
    path = Path(req.path)
    if not path.is_absolute():
        path = config.LANDING_DIR / path
    if not path.exists():
        raise HTTPException(404, f"{path} not found")
    try:
        return ingest_file(path, triggered_by=req.triggered_by)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, f"{type(exc).__name__}: {exc}") from exc


@app.post("/ingest/upload")
async def ingest_upload(file: UploadFile = File(...)):
    dest = config.LANDING_DIR / Path(file.filename).name
    tmp = dest.with_suffix(".uploading")
    tmp.write_bytes(await file.read())
    tmp.rename(dest)          # the file-watcher picks it up from here
    return {"saved_to": str(dest), "note": "file-watcher will ingest it"}


@app.post("/ingest/stream")
def ingest_stream(req: StreamRequest):
    """Validates a micro-batch of sensor events and commits the good ones to stg.iot_readings.
    End-to-End Latency = db_inserted_at (DB clock at commit) - source_created_at (producer)."""
    received_at = datetime.now(timezone.utc)
    window_start = req.window_start or received_at
    raw = pd.DataFrame(req.events).astype("string")
    src_created = pd.to_datetime(raw.pop("source_created_at"), utc=True, errors="coerce") \
        if "source_created_at" in raw else pd.Series(received_at, index=raw.index)
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO meta.stream_metrics (run_label, window_start, events_received)
                       VALUES (%s,%s,%s) RETURNING window_id""",
                    (req.run_label, window_start, len(raw)))
        wid = cur.fetchone()[0]
    df, reasons = coerce_types(raw, "iot")
    reasons = business_rules(df, "iot", reasons)
    bad = reasons != ""
    good = df[~bad].copy()
    good["source_created_at"] = src_created[~bad].dt.tz_convert(None).values  # naive UTC
    good["load_batch_id"] = -wid          # negative id = streamed micro-batch
    good["source_file"] = f"stream:{req.run_label}"
    db.ensure_staging_table("iot", [c for c in df.columns])
    loaded = db.copy_dataframe(good, "stg.iot_readings")
    db.log_rejects(wid, "iot", "stream", df[bad], reasons)
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT avg(extract(epoch FROM db_inserted_at - source_created_at)),
                              percentile_cont(0.95) WITHIN GROUP
                                (ORDER BY extract(epoch FROM db_inserted_at - source_created_at)),
                              max(extract(epoch FROM db_inserted_at - source_created_at)),
                              max(db_inserted_at)
                       FROM stg.iot_readings WHERE load_batch_id = %s""", (-wid,))
        avg_l, p95_l, max_l, last_commit = cur.fetchone()
        window_end = last_commit or datetime.now(timezone.utc)
        cur.execute("""SELECT max(window_end) FROM meta.stream_metrics
                       WHERE run_label=%s AND window_id < %s""", (req.run_label, wid))
        prev_end = cur.fetchone()[0]
        # arrival throughput needs a previous window of the same run; the first window has none
        arrival_secs = (window_end - prev_end).total_seconds() if prev_end else None
        proc_secs = max((window_end - window_start).total_seconds(), 1e-3)
        cur.execute("""UPDATE meta.stream_metrics SET window_end=%s, events_loaded=%s,
                       events_malformed=%s, throughput_eps=%s, capacity_eps=%s,
                       avg_latency_sec=%s, p95_latency_sec=%s, max_latency_sec=%s
                       WHERE window_id=%s""",
                    (window_end, loaded, int(bad.sum()),
                     round(loaded / arrival_secs, 2) if arrival_secs else None,
                     round(loaded / proc_secs, 2), avg_l, p95_l, max_l, wid))
    return {"window_id": wid, "received": len(raw), "loaded": loaded,
            "malformed": int(bad.sum()), "avg_latency_sec": avg_l}


@app.get("/metrics/batch")
def batch_metrics():
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT batch_id, file_name, status, rows_read, rows_loaded, rows_rejected,
                              duration_seconds FROM meta.batch_ingestion_log
                       ORDER BY batch_id DESC LIMIT 50""")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
