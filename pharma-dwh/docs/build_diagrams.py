"""Renders the pipeline architecture diagram (Graphviz). Run: python docs/build_diagrams.py"""
import subprocess
from pathlib import Path

OUT = Path(__file__).parent

FACT = "#F4B183"
DIM = "#DEEBF7"
JUNK = "#E2F0D9"
HDR_DIM = "#2F5597"
HDR_FACT = "#C55A11"
HDR_JUNK = "#548235"


def esc(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def table(name, cols, hdr, body, note=""):
    rows = "".join(
        f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="10">{esc(c)}</FONT></TD></TR>'
        for c in cols)
    sub = f'<TR><TD BGCOLOR="{hdr}"><FONT COLOR="white" POINT-SIZE="9"><I>{esc(note)}</I></FONT></TD></TR>' if note else ""
    return (f'<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="3" BGCOLOR="{body}">'
            f'<TR><TD BGCOLOR="{hdr}"><FONT COLOR="white" POINT-SIZE="12"><B>{name}</B></FONT></TD></TR>'
            f'{sub}{rows}</TABLE>>')


# The star schema diagram (docs/sdi_star_schema.png) is drawn by docs/star_schema.py

# ------------------------------------------------------------------ pipeline architecture
arch = r'''digraph P {
graph [rankdir=LR, fontname="Helvetica", dpi=150, pad=0.3, nodesep=0.3, ranksep=0.45,
       label="Automated DWH + BI pipeline (Phase 1: SDI mart) - orchestrated by Apache Airflow", labelloc=t, fontsize=18];
node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, fillcolor="#DEEBF7", color="#2F5597"];
edge [color="#404040", fontname="Helvetica", fontsize=9];
subgraph cluster_src { label="Sources"; style="rounded,dashed"; color="#7F7F7F";
  seed [label="7 OLTP extracts\n2023-2025 (seed)", fillcolor="#F2F2F2"];
  gen [label="Data generator\n(oversampling)\nDAG 1", fillcolor="#FFF2CC"]; }
subgraph cluster_ing { label="Ingestion"; style="rounded,dashed"; color="#7F7F7F";
  land [label="Landing folder\n(atomic file drops)", fillcolor="#F2F2F2"];
  watch [label="File-watcher\n(watchdog)"];
  api [label="Ingestion API\n(FastAPI)\nvalidate + COPY"];
  stream [label="Stream simulator\nIoT micro-batches\nDAG 3", fillcolor="#FFF2CC"]; }
subgraph cluster_store { label="Storage & staging"; style="rounded,dashed"; color="#7F7F7F";
  ods [label="Postgres ODS\nstg.* (7 tables)\n+ meta.* metrics", fillcolor="#E2F0D9", color="#548235"]; }
subgraph cluster_tr { label="Transformation & modelling (DAG 4)"; style="rounded,dashed"; color="#7F7F7F";
  duck [label="DuckDB OLAP\nraw.* -> sdi.* star\n5 dims (SCD 1/2) + 2 facts\nincremental + quality checks", fillcolor="#FBE5D6", color="#C55A11"]; }
subgraph cluster_q { label="Query & BI (DAG 5)"; style="rounded,dashed"; color="#7F7F7F";
  agg [label="Summary tables +\npartitions + indexes\ntimed OLAP queries", fillcolor="#FBE5D6", color="#C55A11"];
  pgsdi [label="Postgres sdi.*\n(published star)", fillcolor="#E2F0D9", color="#548235"];
  pbi [label="Power BI\nSDI business dashboards", fillcolor="#FFE699", color="#BF9000"];
  graf [label="Grafana\n4 DE dashboards", fillcolor="#FFE699", color="#BF9000"]; }
seed -> land [label="initial load"]; gen -> land [label="6 files / refresh"];
land -> watch -> api -> ods [label="  batch"]; gen -> stream [label="IoT file"]; stream -> api [label="POST /ingest/stream"];
ods -> duck [label="new batches only"]; duck -> agg; duck -> pgsdi [label="publish"]; pgsdi -> pbi;
ods -> graf [label="meta.*", style=dashed];
}'''
(OUT / "pipeline_architecture.dot").write_text(arch)
subprocess.run(["dot", "-Tpng", str(OUT / "pipeline_architecture.dot"), "-o", str(OUT / "pipeline_architecture.png")], check=True)
print("diagrams written")
