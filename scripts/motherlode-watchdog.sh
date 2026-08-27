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

# User services do not reliably inherit an interactive ssh-agent.  Prefer the
# configured Lightning key so a valid running studio is not mislabeled as an
# SSH outage merely because the watchdog lacks SSH_AUTH_SOCK.
ssh_identity=${MOTHERLODE_SSH_IDENTITY:-"$HOME/.lightning/lightning_rsa"}
ssh_options=(
  -o BatchMode=yes
  -o ConnectTimeout=15
  -o ConnectionAttempts=3
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=4
)
if [[ -r "$ssh_identity" ]]; then
  ssh_options+=( -i "$ssh_identity" -o IdentitiesOnly=yes )
fi

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
pid_numbers = {}
for name in names:
    path = root / "state" / name
    if not path.exists():
        pids[name] = False
        pid_numbers[name] = None
        continue
    try:
        number = int(path.read_text().strip())
        os.kill(number, 0)
        pids[name] = True
        pid_numbers[name] = number
    except (OSError, ValueError):
        pids[name] = False
        pid_numbers[name] = None

progress_path = root / "progress" / "current.json"
progress = {}
if progress_path.exists():
    try:
        progress = json.loads(progress_path.read_text())
    except json.JSONDecodeError:
        progress = {"state": "INVALID_JSON"}

converted = progress.get("live_converted_by_dataset") or {}
# Chunk workers write V1 derivative MIDIs continuously but only publish the
# aggregate progress receipt at a durable chunk boundary.  The pre-Brick-3
# directories are intentionally flat, so their directory mtime is a cheap
# durable movement witness: it advances whenever a candidate is atomically
# created, without recursively scanning the corpus every watchdog interval.
derivative_mtimes = {}
for dataset in ("pdmx", "gigamidi"):
    directory = root / "derived" / dataset / "prebrick3"
    try:
        derivative_mtimes[dataset] = directory.stat().st_mtime
    except OSError:
        derivative_mtimes[dataset] = None
latest_derivative_mtime = max(
    (value for value in derivative_mtimes.values() if value is not None),
    default=None,
)
print(json.dumps({
    "pids": pids,
    "pid_numbers": pid_numbers,
    "progress_exists": progress_path.exists(),
    "progress_age_seconds": time.time() - progress_path.stat().st_mtime if progress_path.exists() else None,
    "state": progress.get("state"),
    "stage": progress.get("current_stage"),
    "converted_streams": sum(int(value) for value in converted.values()),
    "converted_by_dataset": converted,
    "derivative_mtimes": derivative_mtimes,
    "latest_derivative_mtime": latest_derivative_mtime,
}, sort_keys=True))
PY
)

remote_root=$(printf '%q' "$MOTHERLODE_ROOT")
raw=''
for attempt in 1 2 3; do
  if raw=$(ssh "${ssh_options[@]}" "$MOTHERLODE_SSH" "MOTHERLODE_ROOT=$remote_root python3 - <<'PY'
$probe
PY" 2>/dev/null); then
    break
  fi
  raw=''
  [[ $attempt == 3 ]] || sleep 5
done
if [[ -z "$raw" ]]; then raw='{"probe_error":"ssh_unavailable"}'; fi

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
    # The monitor emits aggregate counts only after a durable chunk boundary.
    # A long active Brick 3 chunk can legitimately outlive the receipt freshness
    # budget, so liveness plus the independent two-hour movement guard below is
    # the fail-closed stall signal rather than this observational age alone.
    if probe.get("state") not in ("RUNNING", "PARTIAL"):
        faults.append("unexpected_pipeline_state:" + str(probe.get("state")))

count = int(probe.get("converted_streams") or 0)
last_count = previous.get("last_count")
last_progress_at = previous.get("last_progress_at", now)
run_changed = previous.get("probe", {}).get("pid_numbers") != probe.get("pid_numbers")
latest_derivative_mtime = probe.get("latest_derivative_mtime")
previous_derivative_mtime = previous.get("probe", {}).get("latest_derivative_mtime")
derivative_advanced = (
    latest_derivative_mtime is not None
    and (previous_derivative_mtime is None or float(latest_derivative_mtime) > float(previous_derivative_mtime))
)
derivative_recent = (
    latest_derivative_mtime is not None
    and now - float(latest_derivative_mtime) <= int(os.environ["NO_PROGRESS_GRACE"])
)
if run_changed:
    # A Lightning stop/start replaces every scheduler PID while preserving
    # completed shard receipts.  Start a fresh movement grace period.
    last_progress_at = now
elif not faults and last_count is not None:
    if count > int(last_count) or derivative_advanced or derivative_recent:
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
