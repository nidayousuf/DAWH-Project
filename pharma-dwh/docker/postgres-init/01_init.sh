#!/bin/bash
# Runs once, when the Postgres volume is first created.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
  CREATE DATABASE airflow;
  CREATE DATABASE pharma_dwh;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d pharma_dwh -f /sql/00_init.sql
