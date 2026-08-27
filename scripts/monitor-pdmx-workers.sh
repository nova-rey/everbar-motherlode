#!/usr/bin/env bash
# Durable, read-only PDMX worker liveness heartbeat.  Completion receipts,
# not this monitor, remain the authority for shard ownership and resumability.
set -euo pipefail

root=${MOTHERLODE_ROOT:?MOTHERLODE_ROOT is required}
interval_seconds=${MONITOR_INTERVAL_SECONDS:-60}
slots=${PDMX_WORKER_SLOTS:-4}
heartbeat="$root/progress/monitor-heartbeat.json"

while true; do
  ROOT="$root" SLOTS="$slots" python3 - "$heartbeat" <<'PY'
import json
import os
import sys
import time
from pathlib import Path

root = Path(os.environ["ROOT"])
workers = []
for slot in range(int(os.environ["SLOTS"])):
    pid_path = root / "state" / f"pdmx-chunk-worker-{slot}.pid"
    pid = None
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text().strip())
        except ValueError:
            pass
    alive = pid is not None
    if alive:
        try:
            os.kill(pid, 0)
        except OSError:
            alive = False
    workers.append({"slot": slot, "pid": pid, "alive": alive})
payload = {"state": "RUNNING" if all(w["alive"] for w in workers) else "DEGRADED", "updated_unix": time.time(), "workers": workers}
target = Path(sys.argv[1])
target.parent.mkdir(parents=True, exist_ok=True)
temporary = target.with_suffix(target.suffix + ".tmp")
temporary.write_text(json.dumps(payload, sort_keys=True) + "\n")
temporary.replace(target)
PY
  sleep "$interval_seconds"
done
