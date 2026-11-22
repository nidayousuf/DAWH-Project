"""Storage & staging layer metrics -> meta.storage_metrics.

  * Table Row Count      : SELECT COUNT(*) for every staging, star and aggregate table
  * Storage Footprint MB : pg_total_relation_size() for Postgres tables, os.path.getsize()
                           for the DuckDB file and the Postgres data directory
  * Date Coverage Span   : MAX(date) - MIN(date) and number of distinct year-months
"""
import argparse
import os
from pathlib import Path

import duckdb

from common import config, db

STAGING_DATES = {
    "master_data": ("effective_from_date", "Master data"),
    "manufacturing": ("order_date", "Manufacturing"),
    "manufacturer_delivery": ("dispatch_date", "Manufacturer delivery"),
    "warehouse_snapshot": ("snapshot_date", "Warehouse"),
    "pharmacy_sales": ("sales_date", "Pharmacy sales"),
    "hospital_demand": ("demand_month", "Hospital demand"),
    "iot_readings": ("event_timestamp_utc", "IoT telemetry"),
}
MART_DATES = {"fact_sales": "date_key", "fact_demand": "date_key"}


def capture(label="manual"):
    rows = []
    with db.pg_conn() as c, c.cursor() as cur:
        for table, (dcol, domain) in STAGING_DATES.items():
            cur.execute("SELECT to_regclass(%s)", (f"stg.{table}",))
            if cur.fetchone()[0] is None:
                continue
            cur.execute(f"""SELECT count(*), pg_total_relation_size('stg.{table}')/1048576.0,
                    min({dcol})::date, max({dcol})::date,
                    count(DISTINCT date_trunc('month', {dcol})) FROM stg.{table}""")
            n, mb, dmin, dmax, months = cur.fetchone()
            span = (dmax - dmin).days if dmin and dmax else None
            rows.append((label, "staging", f"stg.{table}", domain, n, mb, dmin, dmax, span, months))
        cur.execute("""SELECT c.relname, pg_total_relation_size(c.oid)/1048576.0
                       FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname='sdi' AND c.relkind IN ('r','p') AND NOT c.relispartition""")
        pg_sizes = dict(cur.fetchall())
        # partitioned parents report 0 bytes; add their partitions
        for parent in MART_DATES:
            cur.execute("""SELECT COALESCE(sum(pg_total_relation_size(inhrelid)),0)/1048576.0
                           FROM pg_inherits WHERE inhparent = to_regclass(%s)""", (f"sdi.{parent}",))
            pg_sizes[parent] = float(cur.fetchone()[0])
        cur.execute("SELECT pg_database_size(current_database())/1048576.0")
        db_mb = float(cur.fetchone()[0])

    if config.DUCKDB_PATH.exists():
        con = duckdb.connect(str(config.DUCKDB_PATH), read_only=True)
        tables = [r[0] for r in con.execute("""SELECT table_name FROM information_schema.tables
                                               WHERE table_schema='sdi'""").fetchall()]
        for t in tables:
            n = con.execute(f"SELECT count(*) FROM sdi.{t}").fetchone()[0]
            dmin = dmax = months = span = None
            if t in MART_DATES:
                k = MART_DATES[t]
                dmin, dmax, months = con.execute(f"""SELECT strptime(min({k})::VARCHAR,'%Y%m%d')::DATE,
                        strptime(max({k})::VARCHAR,'%Y%m%d')::DATE, count(DISTINCT {k}//100)
                        FROM sdi.{t}""").fetchone()
                span = (dmax - dmin).days
            kind = "fact" if t.startswith("fact") else "aggregate" if t.startswith("agg") else "dimension"
            rows.append((label, f"mart_{kind}", f"sdi.{t}", "SDI mart", n,
                         pg_sizes.get(t), dmin, dmax, span, months))
        con.close()
        rows.append((label, "file", "DuckDB file (OLAP mart)", "SDI mart", None,
                     os.path.getsize(config.DUCKDB_PATH) / 1048576, None, None, None, None))
    rows.append((label, "file", "Postgres database (ODS + meta + sdi)", "All", None, db_mb,
                 None, None, None, None))
    with db.pg_conn() as c, c.cursor() as cur:
        cur.executemany("""INSERT INTO meta.storage_metrics (refresh_label, layer, object_name,
               source_domain, row_count, size_mb, min_date, max_date, date_span_days,
               distinct_months) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="manual")
    for r in capture(ap.parse_args().label):
        print(f"{r[1]:15s} {r[2]:40s} rows={r[4]!s:>9}  mb={float(r[5] or 0):8.2f}  {r[6]}..{r[7]}")
