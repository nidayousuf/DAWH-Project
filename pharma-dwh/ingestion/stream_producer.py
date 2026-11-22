"""IoT stream simulator: replays generated sensor readings as a live event stream.

Each micro-batch is stamped with source_created_at = now (the moment the "sensor" emits it)
and POSTed to /ingest/stream, which commits it and computes End-to-End Latency, Stream
Throughput and Malformed/Dropped counts per window into meta.stream_metrics.

Usage: python -m ingestion.stream_producer --eps 400 --batch 100 [--max-events 20000]
"""
import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from common import config

STREAM_DIR = config.DATA_DIR / "stream"
DONE = STREAM_DIR / "_streamed.json"


def pending_files():
    done = set(json.loads(DONE.read_text())) if DONE.exists() else set()
    return [p for p in sorted(STREAM_DIR.glob("07_iot*.csv")) if p.name not in done], done


def run(eps=400, batch=100, max_events=None, label=None):
    files, done = pending_files()
    if not files:
        print("no new IoT stream files")
        return 0
    sent = 0
    for path in files:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        if max_events:
            df = df.head(max_events)
        run_label = label or path.name.split("__")[1]
        interval = batch / eps
        for start in range(0, len(df), batch):
            t0 = time.perf_counter()
            chunk = df.iloc[start:start + batch].copy()
            now = datetime.now(timezone.utc)
            chunk["source_created_at"] = now.isoformat()
            payload = {"run_label": run_label, "window_start": now.isoformat(),
                       "events": chunk.to_dict("records")}
            try:
                r = requests.post(f"{config.INGEST_API_URL}/ingest/stream", json=payload, timeout=60)
                r.raise_for_status()
                sent += len(chunk)
            except requests.RequestException as exc:
                print(f"window failed: {exc}")
            time.sleep(max(0.0, interval - (time.perf_counter() - t0)))
            if start // batch % 20 == 0:
                print(f"{path.name}: {sent:,} events sent")
        done.add(path.name)
        DONE.write_text(json.dumps(sorted(done)))
    return sent


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--eps", type=float, default=400, help="target events per second")
    ap.add_argument("--batch", type=int, default=100, help="events per micro-batch")
    ap.add_argument("--max-events", type=int, default=None)
    a = ap.parse_args()
    print(f"streamed {run(a.eps, a.batch, a.max_events):,} events")
