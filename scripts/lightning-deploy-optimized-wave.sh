#!/usr/bin/env bash
# Perform the source-tree handoff only after the currently active chunk child
# processes exit. Arguments are old PDMX worker-parent PIDs followed by the
# old wave-controller PID. Completed receipts make the replacement resumable.
set -euo pipefail

root=${MOTHERLODE_ROOT:?MOTHERLODE_ROOT is required}
repo=${MOTHERLODE_REPO:?MOTHERLODE_REPO is required}
next=${MOTHERLODE_NEXT_REPO:?MOTHERLODE_NEXT_REPO is required}
backup=${MOTHERLODE_BACKUP_REPO:?MOTHERLODE_BACKUP_REPO is required}
shift 0
if (($# != 4)); then
  echo "usage: $0 <pdmx-parent-0> <pdmx-parent-1> <pdmx-parent-2> <queue-parent>" >&2
  exit 2
fi
parents=("$1" "$2" "$3")
queue_parent=$4

# Stop only the schedulers, never their in-flight child chunks.
for parent in "${parents[@]}" "$queue_parent"; do kill -STOP "$parent"; done
for parent in "${parents[@]}"; do
  while pgrep -P "$parent" >/dev/null 2>&1; do sleep 15; done
done

# No worker process is executing Motherlode source at this point. Retain the
# prior tree as a rollback artifact and retain the already-built venv.
mv "$repo/.venv" "$next/.venv"
mv "$repo" "$backup"
mv "$next" "$repo"
for parent in "${parents[@]}" "$queue_parent"; do kill -TERM "$parent" 2>/dev/null || true; done

export MOTHERLODE_ROOT="$root" MOTHERLODE_REPO="$repo"
export EVERBAR_CHECKOUT=${EVERBAR_CHECKOUT:?EVERBAR_CHECKOUT is required}
for slot in 0 1 2; do
  nohup "$repo/scripts/motherlode-chunk-worker.sh" pdmx "$slot" > "$root/logs/pdmx-chunk-worker-${slot}.log" 2>&1 &
  echo $! > "$root/state/pdmx-chunk-worker-${slot}.pid"
done
# Keep the host at four useful Brick 3 processes while the pre-existing
# MAESTRO/Aria worker is still alive. The slot-3 wrapper is itself the durable
# PID the wave controller waits on, then execs the fourth PDMX worker.
other_work_pid=${MOTHERLODE_CONCURRENT_WORK_PID:-}
if test -n "$other_work_pid" && kill -0 "$other_work_pid" 2>/dev/null; then
  nohup bash -c 'while kill -0 "$1" 2>/dev/null; do sleep 30; done; exec "$2" pdmx 3' bash \
    "$other_work_pid" "$repo/scripts/motherlode-chunk-worker.sh" > "$root/logs/pdmx-chunk-worker-3.log" 2>&1 &
else
  nohup "$repo/scripts/motherlode-chunk-worker.sh" pdmx 3 > "$root/logs/pdmx-chunk-worker-3.log" 2>&1 &
fi
echo $! > "$root/state/pdmx-chunk-worker-3.pid"
nohup "$repo/scripts/queue-gigamidi-after-pdmx.sh" > "$root/logs/queue-gigamidi-after-pdmx-chunks.log" 2>&1 &
echo $! > "$root/state/queue-gigamidi-after-pdmx-chunks.pid"

python3 - <<'PY'
import json
import os
import time
from pathlib import Path

root = Path(os.environ["MOTHERLODE_ROOT"])
(root / "progress" / "optimization-deploy-3122b80.json").write_text(json.dumps({
    "state": "DEPLOYED",
    "repo_sha": "3122b80ef8e1c62bc8d1481e6ff879daf72b54f8",
    "deployed_at": time.time(),
}, sort_keys=True) + "\n")
PY
