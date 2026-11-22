"""Automated file-watcher: watches data/landing and sends every new file to the ingestion API.

  * waits until the file size stops changing (no half-written files),
  * processes files in name order (01_master before 05_sales, so dimensions arrive first),
  * moves the file to data/archive on success or data/rejected on failure.

Run:  python -m ingestion.watcher
"""
import queue
import shutil
import threading
import time
from pathlib import Path

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver  # works on Docker bind mounts

from common import config

VALID = (".csv", ".xlsx")
q: "queue.Queue[Path]" = queue.Queue()
seen: set[str] = set()


def _stable(path: Path, wait=1.0, tries=60) -> bool:
    last = -1
    for _ in range(tries):
        if not path.exists():
            return False
        size = path.stat().st_size
        if size == last and size > 0:
            return True
        last = size
        time.sleep(wait)
    return False


class Handler(FileSystemEventHandler):
    def _enqueue(self, p):
        path = Path(p)
        if path.suffix.lower() in VALID and path.name not in seen:
            seen.add(path.name)
            q.put(path)

    def on_created(self, event):
        if not event.is_directory:
            self._enqueue(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._enqueue(event.dest_path)


def worker():
    while True:
        path = q.get()
        time.sleep(0.5)
        # pick up any siblings that arrived together and process them in name order
        batch = [path]
        while not q.empty():
            batch.append(q.get())
        for p in sorted(batch, key=lambda x: x.name):
            if not _stable(p):
                print(f"[watcher] {p.name} disappeared or never stabilised")
                continue
            print(f"[watcher] ingesting {p.name}")
            try:
                r = requests.post(f"{config.INGEST_API_URL}/ingest/batch",
                                  json={"path": str(p), "triggered_by": "file-watcher"},
                                  timeout=900)
                ok = r.status_code == 200
                print(f"[watcher] {p.name}: {r.status_code} {r.text[:200]}")
            except requests.RequestException as exc:
                ok = False
                print(f"[watcher] API error for {p.name}: {exc}")
            dest = config.ARCHIVE_DIR if ok else config.REJECTED_DIR
            shutil.move(str(p), dest / p.name)


def main():
    for _ in range(60):   # wait for the API container
        try:
            requests.get(f"{config.INGEST_API_URL}/health", timeout=2)
            break
        except requests.RequestException:
            time.sleep(2)
    threading.Thread(target=worker, daemon=True).start()
    handler = Handler()
    for p in sorted(config.LANDING_DIR.iterdir()):   # files that arrived while we were down
        handler._enqueue(p)
    obs = PollingObserver(timeout=2)
    obs.schedule(handler, str(config.LANDING_DIR), recursive=False)
    obs.start()
    print(f"[watcher] watching {config.LANDING_DIR}")
    try:
        while True:
            time.sleep(1)
    finally:
        obs.stop()
        obs.join()


if __name__ == "__main__":
    main()
