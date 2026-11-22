"""Generates the Grafana dashboard JSON files (provisioned automatically at start-up).
Run: python grafana/build_dashboards.py"""
import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
DS = {"type": "grafana-postgresql-datasource", "uid": "pharma-pg"}
_id = [0]

LABEL_FILTER = "refresh_label IN (${refresh_label:sqlstring})"


def target(sql, fmt="table", ref="A"):
    return {"refId": ref, "datasource": DS, "rawQuery": True, "editorMode": "code",
            "format": fmt, "rawSql": sql}


def panel(ptype, title, sql, x, y, w, h, desc="", unit=None, options=None, fmt="table",
          overrides=None, decimals=None, thresholds=None, extra_targets=None, field_cfg=None):
    _id[0] += 1
    defaults = {"color": {"mode": "palette-classic"}}
    if unit:
        defaults["unit"] = unit
    if decimals is not None:
        defaults["decimals"] = decimals
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        defaults["color"] = {"mode": "thresholds"}
    if field_cfg:
        defaults.update(field_cfg)
    targets = [target(sql, fmt)] + (extra_targets or [])
    return {"id": _id[0], "type": ptype, "title": title, "description": desc,
            "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": targets, "options": options or {},
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []}}


def stat(title, sql, x, y, w=4, h=4, unit=None, desc="", decimals=None, thresholds=None):
    return panel("stat", title, sql, x, y, w, h, desc, unit,
                 {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                  "colorMode": "background" if thresholds else "value", "graphMode": "none",
                  "textMode": "value_and_name" if False else "auto"},
                 decimals=decimals, thresholds=thresholds)


def bar(title, sql, x, y, w, h, horizontal=False, unit=None, desc="", stacking="none",
        xfield=None):
    opts = {"orientation": "horizontal" if horizontal else "vertical", "showValue": "auto",
            "stacking": stacking, "legend": {"displayMode": "list", "placement": "bottom",
                                              "showLegend": True},
            "xTickLabelRotation": 0 if horizontal else -30, "barWidth": 0.8, "groupWidth": 0.7}
    if xfield:
        opts["xField"] = xfield
    return panel("barchart", title, sql, x, y, w, h, desc, unit, opts)


def ts(title, sql, x, y, w, h, unit=None, desc=""):
    return panel("timeseries", title, sql, x, y, w, h, desc, unit,
                 {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                  "tooltip": {"mode": "multi"}}, fmt="time_series",
                 field_cfg={"custom": {"drawStyle": "line", "lineWidth": 2, "pointSize": 4,
                                       "showPoints": "always", "fillOpacity": 10}})


def table(title, sql, x, y, w, h, desc=""):
    return panel("table", title, sql, x, y, w, h, desc, None,
                 {"showHeader": True, "cellHeight": "sm"})


def pie(title, sql, x, y, w, h, desc="", unit=None):
    return panel("piechart", title, sql, x, y, w, h, desc, unit,
                 {"pieType": "donut", "reduceOptions": {"calcs": ["lastNotNull"], "values": True},
                  "legend": {"displayMode": "table", "placement": "right", "values": ["percent", "value"]},
                  "displayLabels": ["percent"]})


def text(content, x, y, w, h):
    _id[0] += 1
    return {"id": _id[0], "type": "text", "title": "", "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"mode": "markdown", "content": content}}


def label_var(table_name):
    return {"name": "refresh_label", "label": "Refresh", "type": "query", "datasource": DS,
            "query": f"SELECT DISTINCT refresh_label FROM meta.{table_name} WHERE refresh_label IS NOT NULL ORDER BY 1",
            "refresh": 2, "multi": True, "includeAll": True, "current": {"text": "All", "value": "$__all"},
            "definition": "", "sort": 1}


def dashboard(uid, title, panels, variables=None, refresh="10s", desc=""):
    return {"uid": uid, "title": title, "description": desc, "tags": ["pharma_dwh", "data-engineering"],
            "timezone": "browser", "schemaVersion": 39, "version": 1, "refresh": refresh,
            "time": {"from": "now-24h", "to": "now"}, "editable": True,
            "templating": {"list": variables or []}, "panels": panels,
            "links": [{"title": "All Pharma DWH dashboards", "type": "dashboards", "tags": ["pharma_dwh"],
                       "asDropdown": True}]}


GREEN_RED = [{"color": "red", "value": None}, {"color": "orange", "value": 90},
             {"color": "green", "value": 99}]

# --------------------------------------------------------------------------- 1 BATCH
_id[0] = 0
B = "meta.batch_ingestion_log"
batch = dashboard("dwh-batch", "1 - Batch Ingestion (Excel/CSV -> Staging)", [
    text("**Batch ingestion** - every file dropped in `data/landing` is picked up by the "
         "file-watcher, posted to the ingestion API, validated and COPY-loaded into Postgres "
         "staging. One row per load in `meta.batch_ingestion_log`.", 0, 0, 24, 2),
    stat("Batch Success Rate", f"SELECT 100.0*count(*) FILTER (WHERE status IN ('SUCCESS','PARTIAL'))/NULLIF(count(*),0) FROM {B} WHERE {LABEL_FILTER} AND status<>'RUNNING'",
         0, 2, 4, 4, "percent", "Successful runs / total attempted runs x 100 (PARTIAL = finished, some rows rejected)", 1, GREEN_RED),
    stat("Batches run", f"SELECT count(*) FROM {B} WHERE {LABEL_FILTER}", 4, 2, 4, 4),
    stat("Rows processed", f"SELECT sum(rows_read) FROM {B} WHERE {LABEL_FILTER}", 8, 2, 4, 4, "short"),
    stat("Rows loaded", f"SELECT sum(rows_loaded) FROM {B} WHERE {LABEL_FILTER}", 12, 2, 4, 4, "short"),
    stat("Rows rejected", f"SELECT sum(rows_rejected) FROM {B} WHERE {LABEL_FILTER}", 16, 2, 4, 4, "short",
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]),
    stat("Total load time", f"SELECT sum(duration_seconds) FROM {B} WHERE {LABEL_FILTER}", 20, 2, 4, 4, "s", decimals=1),
    bar("Batch Load Duration (s) per source file", f"""SELECT regexp_replace(file_name,'(__.*)?\\.csv$','') || ' [' || refresh_label || ']' AS file,
            read_seconds AS "read", validate_seconds AS "validate", insert_seconds AS "insert"
         FROM {B} WHERE {LABEL_FILTER} AND status<>'RUNNING' ORDER BY duration_seconds DESC""",
        0, 6, 12, 11, True, "s", "Timestamp_end - Timestamp_start per file, split into read / validate / insert phases",
        "normal"),
    bar("Rows Processed per Batch (len(df))", f"""SELECT regexp_replace(file_name,'(__.*)?\\.csv$','') || ' [' || refresh_label || ']' AS file,
            rows_loaded AS loaded, rows_rejected AS rejected, rows_skipped AS skipped
         FROM {B} WHERE {LABEL_FILTER} AND status<>'RUNNING' ORDER BY rows_read DESC""",
        12, 6, 12, 11, True, "short", "Rows read from each file, split into loaded / rejected / skipped (already staged)", "normal"),
    bar("Throughput (rows/second) per load", f"""SELECT source_key || ' [' || refresh_label || ']' AS load, rows_per_second
         FROM {B} WHERE {LABEL_FILTER} AND status<>'RUNNING' ORDER BY batch_id""", 0, 17, 12, 9, False, "short"),
    pie("Rejected rows by validation rule", f"""SELECT reason, count(*) FROM meta.rejected_records
         WHERE channel='batch' AND batch_id IN (SELECT batch_id FROM {B} WHERE {LABEL_FILTER}) GROUP BY 1""",
        12, 17, 12, 9, "price > 0, valid dates, non-null SKU, duplicates ..."),
    table("Batch load log", f"""SELECT batch_id, refresh_label, file_name, status, rows_read, rows_loaded,
            rows_rejected, rows_skipped, duration_seconds, rows_per_second, triggered_by, started_at, error_message
         FROM {B} WHERE {LABEL_FILTER} ORDER BY batch_id DESC""", 0, 26, 24, 10),
], [label_var("batch_ingestion_log")])

# --------------------------------------------------------------------------- 2 STREAM
_id[0] = 0
S = "meta.stream_metrics"
SF = "run_label IN (${run_label:sqlstring})"
run_var = {"name": "run_label", "label": "Stream run", "type": "query", "datasource": DS,
           "query": f"SELECT DISTINCT run_label FROM {S} ORDER BY 1", "refresh": 2, "multi": True,
           "includeAll": True, "current": {"text": "All", "value": "$__all"}, "sort": 1}
stream = dashboard("dwh-stream", "2 - Streaming Ingestion (IoT micro-batches)", [
    text("**Streaming ingestion** - the stream simulator emits sensor readings in micro-batches "
         "to `POST /ingest/stream`. Latency = DB insert timestamp - source row creation "
         "timestamp. One row per micro-batch window in `meta.stream_metrics`.", 0, 0, 24, 2),
    stat("Stream Throughput (events/s)", f"""SELECT sum(events_loaded)/NULLIF(extract(epoch FROM max(window_end)-min(window_start)),0)
         FROM {S} WHERE {SF}""", 0, 2, 5, 4, "short", "Total records ingested / total stream duration (s)", 1),
    stat("Events ingested", f"SELECT sum(events_loaded) FROM {S} WHERE {SF}", 5, 2, 5, 4, "short"),
    stat("Avg end-to-end latency", f"SELECT avg(avg_latency_sec) FROM {S} WHERE {SF}", 10, 2, 5, 4, "s", decimals=3,
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 2}, {"color": "red", "value": 5}]),
    stat("Malformed / dropped", f"SELECT sum(events_malformed) FROM {S} WHERE {SF}", 15, 2, 4, 4, "short",
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]),
    stat("Drop rate", f"SELECT 100.0*sum(events_malformed)/NULLIF(sum(events_received),0) FROM {S} WHERE {SF}",
         19, 2, 5, 4, "percent", decimals=2),
    ts("Stream Throughput (events/second) over the run", f"""SELECT window_end AS time, run_label AS metric, throughput_eps
         FROM {S} WHERE {SF} AND $__timeFilter(window_end) ORDER BY 1""", 0, 6, 24, 9, "short",
       "Flat line = stable pipeline; dips = back-pressure"),
    ts("End-to-End Latency (s): average and p95 per window", f"""SELECT window_end AS time, avg_latency_sec AS "avg latency", p95_latency_sec AS "p95 latency"
         FROM {S} WHERE {SF} AND $__timeFilter(window_end) ORDER BY 1""", 0, 15, 16, 9, "s"),
    bar("Malformed records per run", f"""SELECT run_label, sum(events_malformed) AS malformed, sum(events_loaded) AS loaded
         FROM {S} WHERE {SF} GROUP BY 1 ORDER BY 1""", 16, 15, 8, 9, False, "short"),
    table("Rejected stream events (dead-letter)", """SELECT rejected_at, record_key, reason FROM meta.rejected_records
         WHERE channel='stream' ORDER BY reject_id DESC LIMIT 200""", 0, 24, 24, 8),
], [run_var], refresh="5s")
stream["time"] = {"from": "now-1h", "to": "now"}

# --------------------------------------------------------------------------- 3 STORAGE
_id[0] = 0
M = "meta.storage_metrics"
LAST = f"captured_at = (SELECT max(captured_at) FROM {M})"
storage = dashboard("dwh-storage", "3 - Storage & Staging Layer", [
    text("**Storage** - captured after every refresh: `COUNT(*)` per table, physical size "
         "(`pg_total_relation_size`, `os.path.getsize`) and date coverage (MAX - MIN date, "
         "distinct year-months).", 0, 0, 24, 2),
    stat("Rows in staging (ODS)", f"SELECT sum(row_count) FROM {M} WHERE {LAST} AND layer='staging'", 0, 2, 5, 4, "short"),
    stat("Rows in SDI star schema", f"SELECT sum(row_count) FROM {M} WHERE {LAST} AND layer LIKE 'mart%'", 5, 2, 5, 4, "short"),
    stat("Postgres footprint", f"SELECT size_mb FROM {M} WHERE {LAST} AND object_name LIKE 'Postgres database%'", 10, 2, 5, 4, "decmbytes", decimals=1),
    stat("DuckDB mart file", f"SELECT size_mb FROM {M} WHERE {LAST} AND object_name LIKE 'DuckDB%'", 15, 2, 4, 4, "decmbytes", decimals=1),
    stat("Sales date coverage (days)", f"SELECT date_span_days FROM {M} WHERE {LAST} AND object_name='stg.pharmacy_sales'", 19, 2, 5, 4, "short",
         "MAX(sales_date) - MIN(sales_date); 2023-2025 history = ~1,065 days, grows with each refresh"),
    pie("Storage breakdown - % of staged rows by source domain", f"""SELECT source_domain, row_count FROM {M}
         WHERE {LAST} AND layer='staging'""", 0, 6, 12, 10),
    bar("Storage footprint (MB) by source domain", f"""SELECT source_domain, size_mb FROM {M}
         WHERE {LAST} AND layer='staging' ORDER BY size_mb DESC""", 12, 6, 12, 10, True, "decmbytes"),
    bar("Table row count growth across refreshes (staging)", f"""SELECT refresh_label, sum(row_count) FILTER (WHERE source_domain='Pharmacy sales') AS "Pharmacy sales",
            sum(row_count) FILTER (WHERE source_domain='Warehouse') AS "Warehouse",
            sum(row_count) FILTER (WHERE source_domain='IoT telemetry') AS "IoT telemetry",
            sum(row_count) FILTER (WHERE source_domain='Hospital demand') AS "Hospital demand"
         FROM {M} m WHERE layer='staging' AND captured_at = (SELECT max(captured_at) FROM {M} x WHERE x.refresh_label=m.refresh_label)
         GROUP BY 1 ORDER BY min(captured_at)""", 0, 16, 12, 9, False, "short"),
    bar("SDI mart rows by table", f"""SELECT object_name, row_count FROM {M} WHERE {LAST} AND layer LIKE 'mart%' ORDER BY row_count DESC""",
        12, 16, 12, 9, True, "short"),
    table("Date Coverage Span check (2023 -> latest refresh)", f"""SELECT object_name, min_date, max_date, date_span_days,
            distinct_months, CASE WHEN min_date <= '2023-02-01' AND distinct_months >= 36 THEN 'OK - full 2023-2025 coverage' ELSE 'CHECK' END AS coverage
         FROM {M} WHERE {LAST} AND min_date IS NOT NULL ORDER BY object_name""", 0, 25, 24, 9),
], refresh="30s")

# --------------------------------------------------------------------------- 4 QUERY
_id[0] = 0
Q = "meta.query_log"
query = dashboard("dwh-query", "4 - Analytical Query Layer & Optimisation", [
    text("**Query layer** - every dashboard widget's SQL is run against the star schema "
         "(*base_star*) and against the pre-aggregated summary tables (*optimized_aggregate*), "
         "timed with a `time.perf_counter()` wrapper and logged to `meta.query_log`.", 0, 0, 24, 2),
    stat("Queries executed", f"SELECT count(*) FROM {Q} WHERE {LABEL_FILTER}", 0, 2, 5, 4, "short"),
    stat("Query Error Rate", f"SELECT 100.0*count(*) FILTER (WHERE status='FAILED')/NULLIF(count(*),0) FROM {Q} WHERE {LABEL_FILTER}",
         5, 2, 5, 4, "percent", "Failed / total executed x 100 (includes one deliberate negative test)", 2,
         [{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 5}]),
    stat("Avg execution time", f"SELECT avg(duration_ms) FROM {Q} WHERE {LABEL_FILTER} AND status='SUCCESS'", 10, 2, 5, 4, "ms", decimals=2),
    stat("p95 execution time", f"SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) FROM {Q} WHERE {LABEL_FILTER} AND status='SUCCESS'",
         15, 2, 4, 4, "ms", decimals=2,
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 500}, {"color": "red", "value": 1000}]),
    stat("Speed-up from aggregates", f"""SELECT avg(duration_ms) FILTER (WHERE query_variant='base_star')/NULLIF(avg(duration_ms) FILTER (WHERE query_variant='optimized_aggregate'),0)
         FROM {Q} WHERE {LABEL_FILTER} AND status='SUCCESS' AND query_name IN (SELECT query_name FROM {Q} WHERE query_variant='optimized_aggregate')""",
         19, 2, 5, 4, "x", decimals=1),
    bar("Average Query Execution Time (ms) per dashboard widget", f"""SELECT query_name,
            avg(duration_ms) FILTER (WHERE query_variant='base_star') AS "star schema",
            avg(duration_ms) FILTER (WHERE query_variant='optimized_aggregate') AS "pre-aggregated"
         FROM {Q} WHERE {LABEL_FILTER} AND status='SUCCESS' GROUP BY 1 ORDER BY 1""", 0, 6, 24, 11, False, "ms"),
    bar("Daily Active Query Count", f"""SELECT to_char(executed_at,'YYYY-MM-DD') AS day, count(*) FILTER (WHERE status='SUCCESS') AS succeeded,
            count(*) FILTER (WHERE status='FAILED') AS failed FROM {Q} WHERE {LABEL_FILTER} GROUP BY 1 ORDER BY 1""",
        0, 17, 8, 9, False, "short", stacking="normal"),
    bar("Execution time by refresh (data volume effect)", f"""SELECT refresh_label, avg(duration_ms) AS avg_ms,
            max(duration_ms) AS max_ms FROM {Q} WHERE status='SUCCESS' GROUP BY 1 ORDER BY 1""", 8, 17, 8, 9, False, "ms"),
    table("Failed queries", f"""SELECT executed_at, query_name, widget, error_message FROM {Q}
         WHERE status='FAILED' AND {LABEL_FILTER} ORDER BY executed_at DESC""", 16, 17, 8, 9),
], [label_var("query_log")], refresh="30s")

# --------------------------------------------------------------------------- 0 OVERVIEW
_id[0] = 0
overview = dashboard("dwh-overview", "0 - DE Observability (overview)", [
    text("## Pharma Cold-Chain DWH - Data Engineering Observability\n"
         "Pipeline: **Generator -> Landing -> File-watcher -> Ingestion API -> Postgres ODS -> "
         "DuckDB SDI star schema -> Postgres `sdi` -> Power BI**, orchestrated by Airflow.\n\n"
         "Open the four detail dashboards from the *All Pharma DWH dashboards* menu (top right).", 0, 0, 24, 4),
    stat("Batch success rate", f"SELECT 100.0*count(*) FILTER (WHERE status IN ('SUCCESS','PARTIAL'))/NULLIF(count(*),0) FROM {B} WHERE status<>'RUNNING'",
         0, 4, 6, 4, "percent", decimals=1, thresholds=GREEN_RED),
    stat("Stream throughput (eps)", f"SELECT sum(events_loaded)/NULLIF(extract(epoch FROM max(window_end)-min(window_start)),0) FROM {S}",
         6, 4, 6, 4, "short", decimals=1),
    stat("Rows in ODS", f"SELECT sum(row_count) FROM {M} WHERE {LAST} AND layer='staging'", 12, 4, 6, 4, "short"),
    stat("Avg query time", f"SELECT avg(duration_ms) FROM {Q} WHERE status='SUCCESS'", 18, 4, 6, 4, "ms", decimals=2),
    table("Mart refresh history", """SELECT refresh_label, object_name, rows_before, rows_after, rows_inserted, status, finished_at
         FROM meta.mart_refresh_log WHERE object_name LIKE 'sdi.fact%' OR object_name='BI refresh signal'
         ORDER BY refresh_id DESC""", 0, 8, 24, 10),
], refresh="10s")

OUT.mkdir(exist_ok=True)
for name, d in [("00_de_observability", overview), ("01_batch_ingestion", batch),
                ("02_stream_ingestion", stream), ("03_storage", storage), ("04_query_layer", query)]:
    (OUT / f"{name}.json").write_text(json.dumps(d, indent=2))
    print("wrote", name)
