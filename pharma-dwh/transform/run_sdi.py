"""Builds / refreshes the Sales & Demand Intelligence (SDI) mart.

  Postgres stg.*  --(incremental sync by load_batch_id)-->  DuckDB raw.*
  DuckDB raw.*    --(SQL files in transform/sql/sdi)----->  DuckDB sdi.* star schema
  DuckDB sdi.*    --(publish)---------------------------->  Postgres sdi.* (for Power BI)

Usage:  python -m transform.run_sdi --label refresh_1
"""
import argparse
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from common import config, db
from common.schema import SOURCES

SQL_DIR = Path(__file__).parent / "sql" / "sdi"
SDI_SOURCES = ["master", "sales", "demand"]

DUCK_TYPES = {"dates": "DATE", "timestamps": "TIMESTAMP", "ints": "BIGINT", "floats": "DOUBLE"}

STAR_TABLES = ["dim_date", "dim_location", "dim_medicine", "dim_hospital", "dim_pharmacy",
               "fact_sales", "fact_demand", "agg_sales_month", "agg_demand_vs_sales",
               "agg_medicine_velocity"]
FACTS = {"fact_sales": ("sales_key", "date_key"),
         "fact_demand": ("demand_key", "date_key")}


def connect():
    con = duckdb.connect(str(config.DUCKDB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS sdi;")
    return con


def _pg_columns(table):
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema='stg' AND table_name=%s ORDER BY ordinal_position""",
                    (table,))
        return [r[0] for r in cur.fetchall()]


def sync_raw(con, key) -> int:
    """Copies only the staging batches DuckDB has not seen yet (incremental extract)."""
    spec = SOURCES[key]
    table = spec["table"]
    cols = [c for c in _pg_columns(table)
            if c not in ("source_created_at", "db_inserted_at", "ingested_at")]
    exists = con.execute("""SELECT count(*) FROM information_schema.tables
                            WHERE table_schema='raw' AND table_name=?""", [table]).fetchone()[0]
    last = con.execute(f"SELECT COALESCE(max(load_batch_id),0) FROM raw.{table}").fetchone()[0] \
        if exists else 0
    types = {}
    for c in cols:
        t = "VARCHAR"
        for group, dtype in DUCK_TYPES.items():
            if c in spec[group]:
                t = dtype
        if c == "load_batch_id":
            t = "BIGINT"
        types[c] = t
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
    collist = ", ".join(f'"{c}"' for c in cols)
    with db.pg_conn() as c, c.cursor() as cur, open(tmp_path, "w") as fh:
        cur.copy_expert(f"COPY (SELECT {collist} FROM stg.{table} WHERE load_batch_id > {last}) "
                        f"TO STDOUT WITH (FORMAT csv, HEADER true)", fh)
    type_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in types.items()) + "}"
    read = f"read_csv('{tmp_path}', header=true, columns={type_sql}, timestampformat='%Y-%m-%d %H:%M:%S')"
    if not exists:
        con.execute(f"CREATE TABLE raw.{table} AS SELECT * FROM {read}")
    else:
        con.execute(f"INSERT INTO raw.{table} SELECT * FROM {read}")
    n = con.execute(f"SELECT count(*) FROM raw.{table} WHERE load_batch_id > {last}").fetchone()[0]
    os.unlink(tmp_path)
    return n


def _count(con, table):
    try:
        return con.execute(f"SELECT count(*) FROM sdi.{table}").fetchone()[0]
    except duckdb.CatalogException:
        return 0


def _log(label, obj, started, before, after, status="SUCCESS"):
    fin = datetime.now(timezone.utc)
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO meta.mart_refresh_log (mart, refresh_label, started_at,
                finished_at, duration_seconds, object_name, rows_before, rows_after,
                rows_inserted, status) VALUES ('SDI',%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (label, started, fin, (fin - started).total_seconds(), obj, before, after,
                     after - before, status))


STEPS = {"dimensions": ("01", "02", "03", "04", "05"),
         "facts": ("10", "11"),
         "aggregates": ("20",)}


def build(con, label, step=None):
    """Runs the SQL files of one step (dimensions / facts / aggregates) or all of them."""
    prefixes = STEPS[step] if step else tuple(p for v in STEPS.values() for p in v)
    files = [f for f in sorted(SQL_DIR.glob("*.sql")) if f.name[:2] in prefixes]
    tables = [t for t in STAR_TABLES
              if step is None
              or (step == "dimensions" and t.startswith("dim_"))
              or (step == "facts" and t.startswith("fact_"))
              or (step == "aggregates" and t.startswith("agg_"))]
    before = {t: _count(con, t) for t in tables}
    build_start = datetime.now(timezone.utc)
    for sql_file in files:
        t = time.perf_counter()
        con.execute(sql_file.read_text())
        print(f"  {sql_file.name:32s} {time.perf_counter() - t:6.2f}s")
    for tname in tables:
        _log(label, f"sdi.{tname}", build_start, before[tname], _count(con, tname))


def quality_checks(con):
    """Post-load data-quality gates. Raises (fails the Airflow task) if any check fails."""
    checks = {
        "FactSales rows == staged sales rows":
            """SELECT (SELECT count(*) FROM sdi.fact_sales) = (SELECT count(DISTINCT sales_transaction_id) FROM raw.pharmacy_sales)""",
        "FactDemand rows == staged demand rows":
            """SELECT (SELECT count(*) FROM sdi.fact_demand) = (SELECT count(DISTINCT hospital_demand_record_id) FROM raw.hospital_demand)""",
        "net sales match staging (to the cent)":
            """SELECT abs((SELECT sum(net_sales_usd) FROM sdi.fact_sales) - (SELECT sum(net_sales_amount_usd) FROM raw.pharmacy_sales)) < 0.05""",
        "no missing dimension keys in FactSales":
            """SELECT count(*) = 0 FROM sdi.fact_sales WHERE med_key < 0 OR location_key < 0 OR hosp_key < 0 OR pharm_key < 0""",
        "no missing dimension keys in FactDemand":
            """SELECT count(*) = 0 FROM sdi.fact_demand WHERE med_key < 0 OR hosp_key < 0""",
        "exactly one current version per medicine":
            """SELECT max(n) = 1 FROM (SELECT count(*) n FROM sdi.dim_medicine WHERE is_current='Y' GROUP BY med_id)""",
        "summary table total == fact total":
            """SELECT abs((SELECT sum(net_sales_usd) FROM sdi.agg_sales_month) - (SELECT sum(net_sales_usd) FROM sdi.fact_sales)) < 0.05""",
    }
    failed = []
    for name, sql in checks.items():
        ok = bool(con.execute(sql).fetchone()[0])
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        if not ok:
            failed.append(name)
    if failed:
        raise RuntimeError(f"Data-quality checks failed: {failed}")
    return checks


PG_TYPE = {"INTEGER": "INTEGER", "BIGINT": "BIGINT", "VARCHAR": "TEXT", "DATE": "DATE",
           "TIMESTAMP": "TIMESTAMP", "DOUBLE": "DOUBLE PRECISION", "HUGEINT": "NUMERIC",
           "BOOLEAN": "BOOLEAN", "FLOAT": "REAL"}


def _pg_ddl(con, table):
    cols = con.execute(f"DESCRIBE sdi.{table}").fetchall()
    parts = []
    for name, dtype, *_ in cols:
        pg = PG_TYPE.get(dtype, "NUMERIC" if dtype.startswith("DECIMAL") else "TEXT")
        if dtype.startswith("DECIMAL"):
            pg = dtype.replace("DECIMAL", "NUMERIC")
        parts.append(f'"{name}" {pg}')
    return ", ".join(parts)


def publish(con):
    """Copies the star schema to Postgres schema sdi, which Power BI reads natively.
    Dimensions/aggregates are replaced; facts are appended (new surrogate keys only) into
    yearly RANGE partitions."""
    with db.pg_conn() as c, c.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS sdi")
    for table in STAR_TABLES:
        t0 = time.perf_counter()
        is_fact = table in FACTS
        with db.pg_conn() as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"sdi.{table}",))
            exists = cur.fetchone()[0] is not None
            if is_fact and exists:
                key = FACTS[table][0]
                cur.execute(f"SELECT COALESCE(max({key}),0) FROM sdi.{table}")
                where = f"WHERE {key} > {cur.fetchone()[0]}"
            else:
                where = ""
                if exists:
                    cur.execute(f"DROP TABLE sdi.{table} CASCADE")
                ddl = _pg_ddl(con, table)
                if is_fact:
                    part_col = FACTS[table][1]
                    cur.execute(f"CREATE TABLE sdi.{table} ({ddl}) PARTITION BY RANGE ({part_col})")
                    for y in range(2023, 2031):
                        cur.execute(f"CREATE TABLE sdi.{table}_y{y} PARTITION OF sdi.{table} "
                                    f"FOR VALUES FROM ({y}0101) TO ({y + 1}0101)")
                    cur.execute(f"CREATE TABLE sdi.{table}_other PARTITION OF sdi.{table} DEFAULT")
                else:
                    cur.execute(f"CREATE TABLE sdi.{table} ({ddl})")
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            path = tmp.name
        con.execute(f"COPY (SELECT * FROM sdi.{table} {where}) TO '{path}' (HEADER, DELIMITER ',')")
        with db.pg_conn() as c, c.cursor() as cur, open(path) as fh:
            cur.copy_expert(f"COPY sdi.{table} FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
        os.unlink(path)
        print(f"  published sdi.{table:30s} {time.perf_counter() - t0:6.2f}s")
    _pg_indexes()


def _pg_indexes():
    idx = {
        "fact_sales": ["date_key", "med_key", "location_key", "hosp_key", "pharm_key", "sales_txn_id"],
        "fact_demand": ["date_key", "med_key", "hosp_key"],
        "dim_medicine": ["med_id"], "dim_pharmacy": ["pharm_id"], "dim_hospital": ["hosp_id"],
        "agg_sales_month": ["month_start_date", "med_key"],
        "agg_demand_vs_sales": ["month_key", "hosp_key"],
    }
    with db.pg_conn() as c, c.cursor() as cur:
        for t, cols in idx.items():
            for col in cols:
                cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{t}_{col} ON sdi.{t} ({col})")
        cur.execute("ANALYZE")


def sync_all(con):
    for key in SDI_SOURCES:
        n = sync_raw(con, key)
        print(f"  synced {n:>8,} new staging rows from {key}")


def run(label="manual", publish_to_pg=True):
    db.init_metadata()
    con = connect()
    t0 = time.perf_counter()
    sync_all(con)
    build(con, label)
    quality_checks(con)
    if publish_to_pg:
        publish(con)
    con.close()
    print(f"SDI refresh '{label}' done in {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="manual")
    ap.add_argument("--no-publish", action="store_true")
    a = ap.parse_args()
    run(a.label, not a.no_publish)
