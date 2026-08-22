#!/usr/bin/env bash
# Cheap, detached health watchdog for the persistent Motherlode runner.
#
# It never restarts, deletes, or alters corpus work.  A fault is delivered to
# the configured Codex thread through `codex queue`, which creates an explicit
# request to inspect and repair the production runner.
set -euo pipefail

config_path=${WATCHDOG_CONFIG:-"$HOME/.config/everbar-motherlode/watchdog.env"}
if [[ ! -r "$config_path" ]]; then
  echo "watchdog configuration is unavailable: $config_path" >&2
  exit 2
fi
# The configuration file is created locally with mode 0600 and contains only
# non-secret routing and path values.
# shellcheck disable=SC1090
source "$config_path"

: "${CODEX_THREAD:?CODEX_THREAD is required}"
: "${MOTHERLODE_SSH:?MOTHERLODE_SSH is required}"
: "${MOTHERLODE_ROOT:?MOTHERLODE_ROOT is required}"

state_dir=${WATCHDOG_STATE_DIR:-"$HOME/.local/state/everbar-motherlode-watchdog"}
max_progress_age=${MAX_PROGRESS_AGE_SECONDS:-900}
no_progress_grace=${NO_PROGRESS_GRACE_SECONDS:-7200}
alert_cooldown=${ALERT_COOLDOWN_SECONDS:-14400}
mkdir -p "$state_dir"
exec 9>"$state_dir/watchdog.lock"
flock -n 9 || exit 0

now=$(date +%s)
probe=$(cat <<'PY'
import json, os, time
from pathlib import Path

root = Path(os.environ["MOTHERLODE_ROOT"])
names = (
    "pdmx-chunk-worker-0.pid", "pdmx-chunk-worker-1.pid",
    "pdmx-chunk-worker-2.pid", "pdmx-chunk-worker-3.pid",
    "gigamidi-chunk-worker-0.pid", "gigamidi-chunk-worker-1.pid",
    "gigamidi-chunk-worker-2.pid", "gigamidi-chunk-worker-3.pid",
    "queue-gigamidi-after-pdmx-chunks.pid", "monitor-pdmx-giga.pid",
)
pids = {}
for name in names:
    path = root / "state" / name
    if not path.exists():
        pids[name] = False
        continue
    try:
        os.kill(int(path.read_text().strip()), 0)
        pids[name] = True
    except (OSError, ValueError):
        pids[name] = False

progress_path = root / "progress" / "current.json"
progress = {}
if progress_path.exists():
    try:
        progress = json.loads(progress_path.read_text())
    except json.JSONDecodeError:
        progress = {"state": "INVALID_JSON"}

converted = progress.get("live_converted_by_dataset") or {}
print(json.dumps({
    "pids": pids,
    "progress_exists": progress_path.exists(),
    "progress_age_seconds": time.time() - progress_path.stat().st_mtime if progress_path.exists() else None,
    "state": progress.get("state"),
    "stage": progress.get("current_stage"),
    "converted_streams": sum(int(value) for value in converted.values()),
    "converted_by_dataset": converted,
}, sort_keys=True))
PY
)

remote_root=$(printf '%q' "$MOTHERLODE_ROOT")
if ! raw=$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$MOTHERLODE_SSH" "MOTHERLODE_ROOT=$remote_root python3 - <<'PY'
$probe
PY" 2>/dev/null); then
  raw='{"probe_error":"ssh_unavailable"}'
fi

assessment=$(RAW_PROBE="$raw" WATCHDOG_STATE="$state_dir/state.json" NOW="$now" MAX_PROGRESS_AGE="$max_progress_age" NO_PROGRESS_GRACE="$no_progress_grace" python3 - <<'PY'
import json, os
from pathlib import Path

now = int(os.environ["NOW"])
try:
    probe = json.loads(os.environ["RAW_PROBE"])
except json.JSONDecodeError:
    probe = {"probe_error": "invalid_probe_json"}
path = Path(os.environ["WATCHDOG_STATE"])
try:
    previous = json.loads(path.read_text())
except (OSError, json.JSONDecodeError):
    previous = {}

faults = []
if probe.get("probe_error"):
    faults.append(str(probe["probe_error"]))
else:
    pids = probe.get("pids") or {}
    workers = any(alive for name, alive in pids.items() if name.startswith(("pdmx-", "gigamidi-")))
    if not pids.get("queue-gigamidi-after-pdmx-chunks.pid"):
        faults.append("wave_controller_dead")
    if not pids.get("monitor-pdmx-giga.pid"):
        faults.append("progress_monitor_dead")
    if not workers:
        faults.append("no_dataset_worker_alive")
    if not probe.get("progress_exists"):
        faults.append("progress_receipt_missing")
    elif probe.get("progress_age_seconds", float("inf")) > int(os.environ["MAX_PROGRESS_AGE"]):
        faults.append("progress_receipt_stale")
    if probe.get("state") not in ("RUNNING", "PARTIAL"):
        faults.append("unexpected_pipeline_state:" + str(probe.get("state")))

count = int(probe.get("converted_streams") or 0)
last_count = previous.get("last_count")
last_progress_at = previous.get("last_progress_at", now)
if not faults and last_count is not None:
    if count > int(last_count):
        last_progress_at = now
    elif now - int(last_progress_at) > int(os.environ["NO_PROGRESS_GRACE"]):
        faults.append("no_conversion_progress")
elif not faults:
    last_progress_at = now

fingerprint = ",".join(sorted(faults))
alert = bool(faults) and (
    previous.get("active_fault") != fingerprint
    or now - int(previous.get("last_alert_at", 0)) >= int(os.environ.get("ALERT_COOLDOWN", "14400"))
)
state = {
    "checked_at": now, "last_count": count, "last_progress_at": last_progress_at,
    "active_fault": fingerprint or None,
    "last_alert_at": now if alert else previous.get("last_alert_at", 0),
    "probe": probe,
}
temporary = path.with_suffix(".tmp")
temporary.write_text(json.dumps(state, sort_keys=True) + "\n")
temporary.replace(path)
print(json.dumps({"alert": alert, "faults": faults, "probe": probe}, sort_keys=True))
PY
)

if [[ $(python3 -c 'import json,sys; print("1" if json.load(sys.stdin)["alert"] else "0")' <<<"$assessment") != 1 ]]; then
  exit 0
fi

summary=$(python3 -c 'import json,sys; x=json.load(sys.stdin); print(", ".join(x["faults"]))' <<<"$assessment")
message="[AUTOMATED MOTHERLODE WATCHDOG] Health fault: ${summary}. Please inspect the Lightning studio and the persistent Motherlode root, repair/resume the detached corpus workers without replaying completed shards, then report the outcome."
if [[ ${WATCHDOG_DRY_RUN:-0} == 1 ]]; then
  echo "watchdog alert: $message"
  exit 0
fi
codex queue --thread "$CODEX_THREAD" --message "$message"
