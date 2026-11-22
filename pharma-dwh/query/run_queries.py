"""Analytical query layer runner. Executes every catalogue query (base + optimised), measures
Query Execution Time with a time.perf_counter() wrapper, counts executions and failures, and
logs everything to meta.query_log for the Query Performance dashboard.

Usage: python -m query.run_queries --label refresh_1 [--repeat 3] [--inject-error]
"""
import argparse
import time

import duckdb

from common import config, db
from query.catalog import BROKEN_QUERY, QUERIES


def timed(con, sql):
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    return (time.perf_counter() - t0) * 1000, len(rows)


def run(label="manual", repeat=3, inject_error=False):
    con = duckdb.connect(str(config.DUCKDB_PATH), read_only=True)
    catalogue = QUERIES + ([BROKEN_QUERY] if inject_error else [])
    log = []
    for q in catalogue:
        for variant in ("base_star", "optimized_aggregate"):
            sql = q["base"] if variant == "base_star" else q.get("optimized")
            if not sql:
                continue
            for _ in range(repeat):
                try:
                    ms, n = timed(con, sql)
                    log.append((label, q["widget"], q["name"], variant, "duckdb", round(ms, 3),
                                n, "SUCCESS", None))
                except Exception as exc:  # noqa: BLE001 - counted for Query Error Rate
                    log.append((label, q["widget"], q["name"], variant, "duckdb", None, 0,
                                "FAILED", str(exc)[:500]))
    con.close()
    with db.pg_conn() as c, c.cursor() as cur:
        cur.executemany("""INSERT INTO meta.query_log (refresh_label, widget, query_name,
               query_variant, engine, duration_ms, rows_returned, status, error_message)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", log)
    ok = sum(1 for r in log if r[7] == "SUCCESS")
    print(f"{len(log)} executions, {len(log) - ok} failed "
          f"({100 * (len(log) - ok) / max(len(log), 1):.1f}% error rate)")
    return log


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="manual")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--inject-error", action="store_true")
    a = ap.parse_args()
    for r in run(a.label, a.repeat, a.inject_error)[::a.repeat]:
        print(f"{r[2]:32s} {r[3]:20s} {r[5] or 0:9.2f} ms  {r[6]:>6} rows  {r[7]}")
