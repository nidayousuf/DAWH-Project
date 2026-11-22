"""Batch ingestion: read one source file -> validate -> load into Postgres staging (ODS).

Every attempt writes one row to meta.batch_ingestion_log with the three required metrics:
  * Batch Load Duration (s) = finished_at - started_at
  * Rows Processed per Batch = len(df)
  * Batch Success Rate = SUCCESS runs / all runs
"""
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common import db
from common.schema import SOURCES, business_rules, coerce_types, detect_source


def _refresh_label(name: str) -> str:
    m = re.search(r"__(refresh_\d+|historical)", name)
    return m.group(1) if m else "historical"


def _start_log(path: Path, key, label, triggered_by) -> int:
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO meta.batch_ingestion_log
              (source_key, staging_table, file_name, file_size_mb, refresh_label, started_at,
               status, triggered_by)
            VALUES (%s,%s,%s,%s,%s,%s,'RUNNING',%s) RETURNING batch_id""",
                    (key, f"stg.{SOURCES[key]['table']}" if key else None, path.name,
                     round(path.stat().st_size / 1e6, 3), label,
                     datetime.now(timezone.utc), triggered_by))
        return cur.fetchone()[0]


def _finish_log(batch_id, t0, timings, rows_read, loaded, rejected, status, error=None, skipped=0):
    t_end = time.perf_counter()
    dur = t_end - t0
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""UPDATE meta.batch_ingestion_log SET finished_at=%s, read_seconds=%s,
               validate_seconds=%s, insert_seconds=%s, duration_seconds=%s, rows_read=%s,
               rows_loaded=%s, rows_rejected=%s, rows_skipped=%s, rows_per_second=%s, status=%s,
               error_message=%s WHERE batch_id=%s""",
                    (datetime.now(timezone.utc), timings.get("read"), timings.get("validate"),
                     timings.get("insert"), round(dur, 3), rows_read, loaded, rejected, skipped,
                     round(rows_read / dur, 1) if dur and rows_read else None, status,
                     error, batch_id))


def ingest_file(path, triggered_by: str = "manual") -> dict:
    path = Path(path)
    key = detect_source(path.name)
    label = _refresh_label(path.name)
    if key is None:
        raise ValueError(f"Unrecognised source file: {path.name}")
    db.init_metadata()
    batch_id = _start_log(path, key, label, triggered_by)
    t0 = time.perf_counter()
    timings, rows_read, loaded, rejected_n, skipped_n = {}, 0, 0, 0, 0
    try:
        t = time.perf_counter()
        if path.suffix.lower() in (".xlsx", ".xls"):
            raw = pd.read_excel(path, dtype=str, keep_default_na=False)
        else:
            raw = pd.read_csv(path, dtype=str, keep_default_na=False)
        rows_read = len(raw)
        timings["read"] = round(time.perf_counter() - t, 3)

        t = time.perf_counter()
        df, reasons = coerce_types(raw, key)
        reasons = business_rules(df, key, reasons)
        db.ensure_staging_table(key, list(df.columns))
        pk = SOURCES[key]["pk"]
        # idempotency: rows whose key is already in staging are skipped, not rejected
        keys = df[pk].astype(str).agg("|".join, axis=1)
        valid_keys = keys[reasons == ""]
        skip = keys.isin(db.existing_keys(key, valid_keys)) & (reasons == "")
        skipped_n = int(skip.sum())
        bad = reasons != ""
        good_df, bad_df = df[~bad & ~skip].copy(), df[bad]
        timings["validate"] = round(time.perf_counter() - t, 3)

        t = time.perf_counter()
        good_df["load_batch_id"] = batch_id
        good_df["source_file"] = path.name
        loaded = db.copy_dataframe(good_df, f"stg.{SOURCES[key]['table']}")
        rejected_n = len(bad_df)
        db.log_rejects(batch_id, key, "batch", raw.loc[bad_df.index].rename(
            columns=dict(zip(raw.columns, df.columns))), reasons)
        timings["insert"] = round(time.perf_counter() - t, 3)

        status = "SUCCESS" if rejected_n == 0 else "PARTIAL"
        _finish_log(batch_id, t0, timings, rows_read, loaded, rejected_n, status, skipped=skipped_n)
        return dict(batch_id=batch_id, source=key, file=path.name, rows_read=rows_read,
                    rows_loaded=loaded, rows_rejected=rejected_n, rows_skipped=skipped_n, status=status,
                    seconds=round(time.perf_counter() - t0, 2))
    except Exception as exc:  # noqa: BLE001 - logged and re-raised
        _finish_log(batch_id, t0, timings, rows_read, loaded, rejected_n, "FAILED",
                    f"{type(exc).__name__}: {exc}"[:1000], skipped_n)
        traceback.print_exc()
        raise
