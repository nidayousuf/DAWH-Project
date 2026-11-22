-- Pharma DWH: Postgres operational data store (staging) + pipeline metadata
-- Runs once when the Postgres container is created (docker-entrypoint-initdb.d).

CREATE SCHEMA IF NOT EXISTS stg;    -- ODS / staging: raw-but-validated copies of the 7 sources
CREATE SCHEMA IF NOT EXISTS meta;   -- pipeline observability (feeds the 4 DE dashboards)
CREATE SCHEMA IF NOT EXISTS sdi;    -- published SDI star schema (copied from DuckDB for Power BI)

-- 1) Batch ingestion log: one row per file load attempt -------------------------------
CREATE TABLE IF NOT EXISTS meta.batch_ingestion_log (
    batch_id          BIGSERIAL PRIMARY KEY,
    source_key        TEXT,
    staging_table     TEXT,
    file_name         TEXT NOT NULL,
    file_size_mb      NUMERIC(12,3),
    refresh_label     TEXT,              -- 'historical', 'refresh_1', 'refresh_2' ...
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    read_seconds      NUMERIC(10,3),
    validate_seconds  NUMERIC(10,3),
    insert_seconds    NUMERIC(10,3),
    duration_seconds  NUMERIC(10,3),     -- Timestamp_end - Timestamp_start
    rows_read         BIGINT,            -- len(df)
    rows_loaded       BIGINT,
    rows_rejected     BIGINT,
    rows_skipped      BIGINT,            -- already in staging (idempotent re-delivery)
    rows_per_second   NUMERIC(14,1),
    status            TEXT,              -- SUCCESS / PARTIAL / FAILED
    error_message     TEXT,
    triggered_by      TEXT               -- file-watcher / airflow / manual
);

-- 2) Rows rejected by validation (dead-letter table) ----------------------------------
CREATE TABLE IF NOT EXISTS meta.rejected_records (
    reject_id     BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT,
    source_key    TEXT,
    channel       TEXT,                  -- batch / stream
    record_key    TEXT,
    reason        TEXT,
    raw_record    JSONB,
    rejected_at   TIMESTAMPTZ DEFAULT now()
);

-- 3) Streaming ingestion metrics: one row per micro-batch window ----------------------
CREATE TABLE IF NOT EXISTS meta.stream_metrics (
    window_id         BIGSERIAL PRIMARY KEY,
    run_label         TEXT,
    window_start      TIMESTAMPTZ,
    window_end        TIMESTAMPTZ,
    events_received   INT,
    events_loaded     INT,
    events_malformed  INT,
    throughput_eps    NUMERIC(12,2),     -- records ingested / seconds since previous window
    capacity_eps      NUMERIC(12,2),     -- records / (commit - window_start): processing speed
    avg_latency_sec   NUMERIC(12,3),     -- db_insert_ts - source_row_created_ts
    p95_latency_sec   NUMERIC(12,3),
    max_latency_sec   NUMERIC(12,3)
);

-- 4) Storage metrics snapshot: one row per table per capture --------------------------
CREATE TABLE IF NOT EXISTS meta.storage_metrics (
    capture_id      BIGSERIAL,
    captured_at     TIMESTAMPTZ DEFAULT now(),
    refresh_label   TEXT,
    layer           TEXT,                -- staging / mart / file
    object_name     TEXT,
    source_domain   TEXT,
    row_count       BIGINT,
    size_mb         NUMERIC(14,3),
    min_date        DATE,
    max_date        DATE,
    date_span_days  INT,
    distinct_months INT
);

-- 5) Query log: one row per analytical query execution --------------------------------
CREATE TABLE IF NOT EXISTS meta.query_log (
    query_log_id    BIGSERIAL PRIMARY KEY,
    executed_at     TIMESTAMPTZ DEFAULT now(),
    refresh_label   TEXT,
    widget          TEXT,                -- dashboard visual / KPI card the query feeds
    query_name      TEXT,
    query_variant   TEXT,                -- 'base_star' vs 'optimized_aggregate'
    engine          TEXT,
    duration_ms     NUMERIC(12,3),
    rows_returned   INT,
    status          TEXT,                -- SUCCESS / FAILED
    error_message   TEXT
);

-- 6) Mart refresh log -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.mart_refresh_log (
    refresh_id      BIGSERIAL PRIMARY KEY,
    mart            TEXT,
    refresh_label   TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    duration_seconds NUMERIC(10,3),
    object_name     TEXT,
    rows_before     BIGINT,
    rows_after      BIGINT,
    rows_inserted   BIGINT,
    status          TEXT
);

ALTER TABLE meta.batch_ingestion_log ADD COLUMN IF NOT EXISTS rows_skipped BIGINT;
ALTER TABLE meta.stream_metrics ADD COLUMN IF NOT EXISTS capacity_eps NUMERIC(12,2);
CREATE INDEX IF NOT EXISTS ix_batch_log_started ON meta.batch_ingestion_log(started_at);
CREATE INDEX IF NOT EXISTS ix_query_log_exec ON meta.query_log(executed_at);
CREATE INDEX IF NOT EXISTS ix_stream_win ON meta.stream_metrics(window_start);
