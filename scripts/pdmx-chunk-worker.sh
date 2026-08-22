#!/usr/bin/env bash
# Deterministic resumable PDMX worker.  The completion test is deliberately
# tiny and dependency-free: a valid receipt is the only reason to skip work.
set -euo pipefail

slot=${1:?slot is required}
root=${MOTHERLODE_ROOT:?MOTHERLODE_ROOT is required}
repo=${MOTHERLODE_REPO:?MOTHERLODE_REPO is required}
chunks=${PDMX_CHUNKS:-96}

cd "$repo"
export EVERBAR_CHECKOUT=${EVERBAR_CHECKOUT:?EVERBAR_CHECKOUT is required}

for ((index=slot; index<chunks; index+=4)); do
  printf -v label 'pdmx-part-%05d-of-%05d' "$index" "$chunks"
  receipt="$root/progress/shards/$label.json"
  if test -f "$receipt" && python3 - "$receipt" <<'PY'
import json
import sys

raise SystemExit(0 if json.load(open(sys.argv[1])).get("state") == "COMPLETE" else 1)
PY
  then
    continue
  fi
  uv run everbar-motherlode shard --root "$root" --config "$repo/configs/motherlode-v1.toml" \
    --dataset pdmx --partition-index "$index" --partitions "$chunks"
done
