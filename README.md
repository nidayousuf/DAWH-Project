# Pharma Cold-Chain DWH – Phase 1(a): Sales & Demand Intelligence (SDI) Data Mart

Data Analytics and Warehousing (Fall 2026) – an automated DWH + BI pipeline for a pharmaceutical cold-chain supply chain, orchestrated with Apache Airflow.

## Group members

| Name | ERP ID |
|---|---|
| Nida Yousuf | [31986] |
| Sobia Rizwan | [33901] |
| Hassan Siddiqui | [33904] |

## Demo video

▶️ **[Watch the Phase 1(a) demo video](https://drive.google.com/file/d/1WUAO9YsNblkinA-0MGRtf_X5U6U59g2U/view?usp=sharing)**

## Deliverables

| Deliverable | File |
|---|---|
| Project report | [`deliverables/Phase.1.Report.pdf`](deliverables/Phase.1.Report.pdf) |
| SDI Excel (DWH design) | [`deliverables/SDI.Excel.xlsx`](deliverables/SDI.Excel.xlsx) |
| Pipeline diagram | [`deliverables/SDI_Pipeline.png`](deliverables/SDI_Pipeline.png) |
| All code (Airflow etc.) | [`pharma-dwh/`](pharma-dwh/) |

## The problem we solve

Medicines are sold through retail pharmacies, hospital pharmacies and online, and hospitals order medicines every month. The data sits in 7 separate files, so it is hard to answer: Are sales going up? Which pharmacy and medicine sell the most? Do hospitals get the medicines they ask for?

We built the **SDI data mart**: a fact constellation with **2 facts** (FactSales, FactDemand) and **5 dimensions** (Date, Medicine, Hospital, Pharmacy, Location), loaded and refreshed automatically.

## Pipeline

![SDI pipeline](deliverables/SDI_Pipeline.png)

Data sources → File-watcher + FastAPI ingestion (batch and stream, with validation) → PostgreSQL staging → DuckDB star schema → published to PostgreSQL `sdi` schema. Grafana monitors ingestion, streaming, storage and queries. Apache Airflow runs every step (DAGs `sdi_00` to `sdi_05`).

## How to run

1. Install **Docker Desktop**.
2. In the `pharma-dwh` folder run: `docker compose up -d --build`
3. Open Airflow at http://localhost:8080 (admin / admin) and trigger `sdi_00_master_pipeline` with **refresh = 0** (historical load), then **refresh = 1** and **refresh = 2** (new data).
4. Open Grafana at http://localhost:3000 (admin / admin) to see the data engineering dashboards.

To start again from zero: `docker compose exec airflow-scheduler python -m scripts.reset`
