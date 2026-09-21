#!/usr/bin/env bash
# Keep a resumable local Motherlode controller alive across ordinary transient
# acquisition/object-store failures.  Each invocation is idempotent: durable
# source, shard, package, and compact-partition receipts are skipped by the
# Python controller.  A COMPLETE terminal receipt ends the loop.
set -u -o pipefail

repo_root=${MOTHERLODE_REPO_ROOT:-/home/rey/everbar-motherlode}
corpus_root=${MOTHERLODE_ROOT:-/home/rey/motherlode-root}
config_path=${MOTHERLODE_CONFIG:-"$repo_root/configs/motherlode-v1.toml"}
plan_path=${MOTHERLODE_AUTOPILOT_PLAN:-"$repo_root/configs/remaining-sources-autopilot.json"}
python_bin=${MOTHERLODE_PYTHON:-"$repo_root/.venv/bin/python"}
progress_path="$corpus_root/progress/remaining-sources-autopilot.json"
log_dir="$corpus_root/logs/remaining-sources-autopilot"
mkdir -p "$log_dir"

attempt=0
while true; do
  attempt=$((attempt + 1))
  started_at=$(date -Is)
  if ( cd "$repo_root" && "$python_bin" -m everbar_motherlode.cli remaining-sources-autopilot \
      --root "$corpus_root" --config "$config_path" --plan "$plan_path" ) >>"$log_dir/controller-retry.log" 2>&1; then
    state=$("$python_bin" - "$progress_path" <<'PY'
import json, pathlib, sys
p=pathlib.Path(sys.argv[1])
print(json.loads(p.read_text()).get("state", "MISSING") if p.exists() else "MISSING")
PY
)
    if [[ "$state" == "COMPLETE" ]]; then
      exit 0
    fi
  fi
  # Bounded exponential backoff prevents an unreachable upstream from
  # consuming local CPU or hammering R2, while removing the need for a manual
  # restart after a recoverable outage.
  delay=$((attempt < 6 ? 30 * (2 ** (attempt - 1)) : 900))
  "$python_bin" - "$progress_path" "$attempt" "$delay" "$started_at" <<'PY'
import json, pathlib, sys, time
p=pathlib.Path(sys.argv[1]); p.parent.mkdir(parents=True, exist_ok=True)
try: body=json.loads(p.read_text())
except Exception: body={}
body.update({"state":"RETRYING", "stage":"RECOVERABLE_FAILURE_BACKOFF",
             "retry_attempt":int(sys.argv[2]), "retry_delay_seconds":int(sys.argv[3]),
             "retry_started_at":sys.argv[4], "updated_at":time.time()})
tmp=p.with_suffix(p.suffix+".tmp"); tmp.write_text(json.dumps(body, indent=2, sort_keys=True)+"\n"); tmp.replace(p)
PY
  sleep "$delay"
done
