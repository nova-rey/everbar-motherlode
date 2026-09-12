#!/usr/bin/env python3
"""Publish pre-existing completed shard states without replaying corpus work.

This is deliberately narrower than ``distributed-shard``: it reads only local
COMPLETE receipts, packages one immutable worker state at a time with the
established stage/publish contract, verifies the remote completion marker, and
then removes only the temporary outbox package.  It never invokes ``shard``.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from everbar_motherlode.core import config, sha, writej
from everbar_motherlode.distributed import (
    _direct_s3_exists,
    _direct_s3_read,
    output_prefix,
    publish_shard,
    stage_shard,
)


def _read_completion(uri: str) -> bytes:
    if uri.startswith("direct-s3://"):
        return _direct_s3_read(uri).encode()
    for attempt in range(3):
        completed = subprocess.run(["rclone", "cat", uri], capture_output=True)
        if completed.returncode == 0:
            return completed.stdout
        if attempt == 2:
            raise RuntimeError(f"could not read remote completion marker: {completed.stderr.decode(errors='replace')[-400:]}")
        time.sleep(2 ** attempt)
    raise AssertionError("unreachable")


def _remote_completion_exists(uri: str) -> bool:
    if uri.startswith("direct-s3://"):
        return _direct_s3_exists(uri)
    completed = subprocess.run(["rclone", "lsf", uri], capture_output=True, text=True)
    if completed.returncode == 0:
        return bool(completed.stdout.strip())
    diagnostic = completed.stderr.lower()
    if any(marker in diagnostic for marker in ("not found", "directory not found", "no such file")):
        return False
    raise RuntimeError(f"could not determine remote completion state: {completed.stderr[-400:]}")


def _completed_receipts(root: Path, dataset_id: str, shard_count: int) -> list[tuple[int, dict]]:
    receipts: list[tuple[int, dict]] = []
    for path in sorted((root / "progress" / "shards").glob(f"{dataset_id}-part-*-of-{shard_count:05d}.json")):
        receipt = json.loads(path.read_text())
        if receipt.get("state") != "COMPLETE":
            continue
        index = receipt.get("partition_index")
        if not isinstance(index, int) or not 0 <= index < shard_count or receipt.get("partitions") != shard_count:
            raise ValueError(f"invalid completed receipt: {path}")
        if not Path(receipt.get("shard_db", "")).is_file():
            raise FileNotFoundError(f"completed receipt lacks its worker database: {path}")
        receipts.append((index, receipt))
    return receipts


def publish_completed(*, root: Path, config_path: Path, output_uri: str, run_id: str, dataset_id: str, shard_count: int) -> dict:
    cfg = config(config_path)
    outbox = root / "outbox"
    outcomes = []
    for index, receipt in _completed_receipts(root, dataset_id, shard_count):
        prefix = output_prefix(run_id, dataset_id, index, shard_count)
        completion_uri = output_uri.rstrip("/") + "/" + prefix + "/completion.json"
        if _remote_completion_exists(completion_uri):
            existing = _read_completion(completion_uri)
            existing_body = json.loads(existing)
            if existing_body.get("state") != "COMPLETE" or existing_body.get("shard_index") != index:
                raise RuntimeError(f"remote completion marker is invalid for shard {index}")
            outcomes.append({"shard_index": index, "state": "ALREADY_DURABLE", "completion_sha256": sha(existing)})
            continue
        stage, manifest = stage_shard(root, cfg, dataset_id, index, shard_count, run_id, {"results": [receipt]})
        try:
            destination = publish_shard(stage, output_uri, manifest)
            remote = _read_completion(completion_uri)
            local = (stage / "completion.json").read_bytes()
            if remote != local:
                raise RuntimeError(f"remote completion receipt bytes differ for shard {index}")
            outcomes.append({"shard_index": index, "state": "PUBLISHED", "destination": destination, "completion_sha256": sha(remote)})
        finally:
            # This directory is a reproducible staging copy. Its removal is
            # permitted only after a byte-identical remote completion receipt.
            if stage.exists() and outcomes and outcomes[-1].get("shard_index") == index and outcomes[-1].get("state") == "PUBLISHED":
                import shutil
                shutil.rmtree(stage)
    report = {"state": "COMPLETE", "run_id": run_id, "dataset_id": dataset_id, "shard_count": shard_count, "outcomes": outcomes, "at": time.time()}
    writej(root / "progress" / f"publish-completed-{dataset_id}-{run_id}.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-uri", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args()
    report = publish_completed(root=args.root, config_path=args.config, output_uri=args.output_uri, run_id=args.run_id, dataset_id=args.dataset, shard_count=args.shard_count)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
