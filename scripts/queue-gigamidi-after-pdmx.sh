#!/usr/bin/env bash
# Resume-safe, four-slot wave controller for one Lightning studio.
set -euo pipefail

root=${MOTHERLODE_ROOT:?MOTHERLODE_ROOT is required}
repo=${MOTHERLODE_REPO:?MOTHERLODE_REPO is required}
worker="$repo/scripts/motherlode-chunk-worker.sh"
export MOTHERLODE_ROOT="$root" MOTHERLODE_REPO="$repo"
export EVERBAR_CHECKOUT=${EVERBAR_CHECKOUT:?EVERBAR_CHECKOUT is required}

wait_workers() {
  local dataset=$1 slot pid
  for slot in 0 1 2 3; do
    pid=$(cat "$root/state/${dataset}-chunk-worker-${slot}.pid")
    while kill -0 "$pid" 2>/dev/null; do sleep 30; done
  done
}

wait_workers pdmx

# First GigaMIDI worker alone performs serialized safe nested extraction.
nohup "$worker" gigamidi 0 > "$root/logs/gigamidi-chunk-worker-0.log" 2>&1 & echo $! > "$root/state/gigamidi-chunk-worker-0.pid"
while test ! -f "$root/extracted/gigamidi/.gigamidi-nested-complete"; do
  pid=$(cat "$root/state/gigamidi-chunk-worker-0.pid")
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "GigaMIDI preparation stopped before completion" >&2
    exit 1
  fi
  sleep 30
done
for slot in 1 2 3; do
  nohup "$worker" gigamidi "$slot" > "$root/logs/gigamidi-chunk-worker-${slot}.log" 2>&1 & echo $! > "$root/state/gigamidi-chunk-worker-${slot}.pid"
done
wait_workers gigamidi

cd "$repo"
uv run everbar-motherlode reconcile --root "$root" --config "$repo/configs/motherlode-v1.toml" --resume
uv run everbar-motherlode merge-shards --root "$root" --config "$repo/configs/motherlode-v1.toml" --resume
