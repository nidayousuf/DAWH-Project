"""Wipes everything so the demo can start from zero: Postgres stg/meta/sdi schemas, the DuckDB
file, generator state and the landing/archive/stream folders. Seed CSVs are kept."""
import shutil

from common import config, db

with db.pg_conn() as c, c.cursor() as cur:
    cur.execute("DROP SCHEMA IF EXISTS stg CASCADE; DROP SCHEMA IF EXISTS meta CASCADE; "
                "DROP SCHEMA IF EXISTS sdi CASCADE;")
db.init_metadata()
for p in config.WAREHOUSE_DIR.glob("*.duckdb*"):
    p.unlink()
config.GENERATOR_STATE.unlink(missing_ok=True)
for d in (config.LANDING_DIR, config.ARCHIVE_DIR, config.REJECTED_DIR,
          config.DATA_DIR / "stream", config.DATA_DIR / "generator_reports"):
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True, exist_ok=True)
print("Reset complete.")
