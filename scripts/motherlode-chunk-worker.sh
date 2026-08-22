#!/usr/bin/env bash
# Generic deterministic chunk worker. It is intentionally a thin orchestration
# layer: Motherlode owns all source, conversion, and Brick 3 semantics.
set -euo pipefail

dataset=${1:?dataset is required}
slot=${2:?slot is required}
root=${MOTHERLODE_ROOT:?MOTHERLODE_ROOT is required}
repo=${MOTHERLODE_REPO:?MOTHERLODE_REPO is required}
chunks=${MOTHERLODE_CHUNKS:-96}

cd "$repo"
export EVERBAR_CHECKOUT=${EVERBAR_CHECKOUT:?EVERBAR_CHECKOUT is required}
runner="$repo/.venv/bin/everbar-motherlode"
if test ! -x "$runner"; then
  echo "Motherlode checkout virtualenv command is unavailable: $runner" >&2
  exit 1
fi

for ((index=slot; index<chunks; index+=4)); do
  printf -v label '%s-part-%05d-of-%05d' "$dataset" "$index" "$chunks"
  receipt="$root/progress/shards/$label.json"
  if test -f "$receipt" && python3 - "$receipt" <<'PY'
import json
import sys

raise SystemExit(0 if json.load(open(sys.argv[1])).get("state") == "COMPLETE" else 1)
PY
  then
    continue
  fi
  "$runner" shard --root "$root" --config "$repo/configs/motherlode-v1.toml" \
    --dataset "$dataset" --partition-index "$index" --partitions "$chunks"
done
