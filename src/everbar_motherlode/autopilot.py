"""Durable, storage-bounded controller for the remaining Motherlode sources.

This is deliberately an orchestration boundary: it invokes the existing
download/extract/shard, publication, verification, and receipt-only streaming
consolidation paths.  It never changes the transformation or Brick-3 policy.
Each source advances serially so that a large archive is never joined by a
second large archive on the constrained local disk; partitions inside a source
run concurrently and remain deterministic/resumable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from .core import config, download, extract, init, registry, writej

POLICY_ID = "motherlode-remaining-sources-autopilot-v1"
TERMINAL_COMPLETE = "COMPLETE"


def _sha_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def classify_sources(sources: list[dict[str, Any]], completed: set[str]) -> list[dict[str, Any]]:
    """Return a complete, explicit inventory without inferring eligibility."""
    rows: list[dict[str, Any]] = []
    for source in sources:
        source_id = source["id"]
        if source_id in completed:
            disposition, reason = "COMPLETED_SKIP", "durably completed before this controller"
        elif source.get("role") != "raw":
            disposition, reason = "GATED", f"role={source.get('role')} is not a raw training source"
        elif source.get("training") != "ALLOWED":
            disposition, reason = "GATED", f"training={source.get('training')}"
        elif source.get("method") in {"manual_gated", "none"}:
            disposition, reason = "GATED", f"acquisition method={source.get('method')} requires explicit external authority"
        else:
            disposition, reason = "QUEUED", "automated raw source with training ALLOWED"
        rows.append({
            "dataset_id": source_id, "disposition": disposition, "reason": reason,
            "estimate_bytes": int(source.get("estimate", 0)), "lane": source.get("lane"),
            "method": source.get("method"), "training": source.get("training"),
        })
    return rows


def safe_to_stage(root: Path, estimate_bytes: int, reserve_bytes: int, multiplier: float) -> tuple[bool, dict[str, int]]:
    """Keep an explicit headroom envelope before a new source begins.

    The estimate covers compressed input.  The multiplier reserves room for
    extraction and short-lived derivation; completed source payloads are
    published and compacted before this controller releases their local trees.
    """
    free = shutil.disk_usage(root).free
    required = int(estimate_bytes * multiplier) + reserve_bytes
    return free >= required, {"free_bytes": free, "required_bytes": required, "reserve_bytes": reserve_bytes}


class Autopilot:
    def __init__(self, *, root: Path, config_path: Path, plan_path: Path):
        self.root = root
        self.config_path = config_path.resolve()
        self.cfg = config(self.config_path)
        self.plan = json.loads(plan_path.read_text())
        self.plan_path = plan_path.resolve()
        self.progress_path = root / "progress" / "remaining-sources-autopilot.json"
        self.log_dir = root / "logs" / "remaining-sources-autopilot"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.workers = int(self.plan["max_partition_workers"])
        self.output_uri = self.plan["output_uri"]
        self.run_prefix = self.plan["run_id_prefix"]
        self.reserve_bytes = int(self.plan["reserve_bytes"])
        self.storage_multiplier = float(self.plan["storage_envelope_multiplier"])
        self.completed = set(self.plan["completed_source_ids"])

    def _write(self, *, state: str, stage: str, **extra: Any) -> dict[str, Any]:
        body = {
            "policy_id": POLICY_ID, "policy_sha256": _sha_json(self.plan), "state": state,
            "stage": stage, "updated_at": time.time(), "root": str(self.root),
            "max_partition_workers": self.workers, "output_uri_scheme": self.output_uri.split(":", 1)[0],
            **extra,
        }
        writej(self.progress_path, body)
        return body

    def inventory(self) -> list[dict[str, Any]]:
        rows = classify_sources(registry(self.cfg), self.completed)
        writej(self.root / "progress" / "remaining-sources-inventory.json", {
            "policy_id": POLICY_ID, "policy_sha256": _sha_json(self.plan),
            "generated_at": time.time(), "rows": rows,
        })
        gates = [row for row in rows if row["disposition"] == "GATED"]
        writej(self.root / "progress" / "remaining-sources-gates.json", {
            "policy_id": POLICY_ID, "generated_at": time.time(), "gates": gates,
        })
        return rows

    def _source(self, dataset_id: str) -> dict[str, Any]:
        return next(source for source in registry(self.cfg) if source["id"] == dataset_id)

    def _run(self, command: list[str], *, log_name: str) -> None:
        log_path = self.log_dir / log_name
        with log_path.open("ab") as log:
            result = subprocess.run(command, cwd=self.config_path.parent.parent, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            raise RuntimeError(f"command failed ({result.returncode}); inspect {log_path}")

    def _worker_command(self, dataset_id: str, index: int, partitions: int) -> list[str]:
        return [sys.executable, "-m", "everbar_motherlode.cli", "shard", "--root", str(self.root),
                "--config", str(self.config_path), "--dataset", dataset_id,
                "--partition-index", str(index), "--partitions", str(partitions)]

    def _launch_partitions(self, dataset_id: str, partitions: int) -> None:
        children: dict[int, subprocess.Popen[bytes]] = {}
        for index in range(partitions):
            receipt = self.root / "progress" / "shards" / f"{dataset_id}-part-{index:05d}-of-{partitions:05d}.json"
            if receipt.exists():
                try:
                    if json.loads(receipt.read_text()).get("state") == "COMPLETE":
                        continue
                except json.JSONDecodeError:
                    pass
            log = (self.log_dir / f"{dataset_id}-part-{index:05d}.log").open("ab")
            children[index] = subprocess.Popen(self._worker_command(dataset_id, index, partitions),
                                                cwd=self.config_path.parent.parent, stdout=log,
                                                stderr=subprocess.STDOUT, start_new_session=True)
            log.close()
        while children:
            active = []
            for index, child in list(children.items()):
                code = child.poll()
                if code is None:
                    active.append(index)
                    continue
                del children[index]
                if code:
                    raise RuntimeError(f"partition {dataset_id}:{index}/{partitions} failed with exit {code}")
            completed = partitions - len(children)
            self._write(state="RUNNING", stage="PARTITIONS", dataset_id=dataset_id, partitions=partitions,
                        completed_partitions=completed, active_partitions=active)
            if children:
                time.sleep(10)

    def _source_complete(self, dataset_id: str, consolidation_id: str) -> bool:
        receipt = self.root / "progress" / "remaining-sources" / f"{dataset_id}.json"
        if not receipt.exists():
            return False
        try:
            body = json.loads(receipt.read_text())
        except json.JSONDecodeError:
            return False
        return body.get("state") == TERMINAL_COMPLETE and body.get("consolidation_id") == consolidation_id

    def _release_source_local_payload(self, dataset_id: str, partitions: int) -> list[str]:
        """Release only reproducible source-local payload after durable receipts.

        Source packages, per-partition completion markers, canonical compact
        partitions, and their checksums have already been verified before this
        is called.  Conversion receipts are intentionally retained: they are
        small, source-independent audit evidence.
        """
        removed: list[str] = []
        candidates = [self.root / "raw" / dataset_id, self.root / "extracted" / dataset_id,
                      self.root / "derived" / dataset_id]
        candidates.extend(self.root / "state" / "shards" / f"{dataset_id}-part-{index:05d}-of-{partitions:05d}"
                          for index in range(partitions))
        for path in candidates:
            if path.exists():
                shutil.rmtree(path)
                removed.append(str(path))
        return removed

    def run(self) -> dict[str, Any]:
        init(self.root, self.cfg)
        rows = self.inventory()
        queued = [row["dataset_id"] for row in rows if row["disposition"] == "QUEUED"]
        self._write(state="RUNNING", stage="INVENTORIED", queued_sources=queued,
                    gated_sources=[row["dataset_id"] for row in rows if row["disposition"] == "GATED"])
        for dataset_id in queued:
            source = self._source(dataset_id)
            run_id = f"{self.run_prefix}-{dataset_id}-v1"
            consolidation_id = f"{self.run_prefix}-{dataset_id}-canonical-v1"
            if self._source_complete(dataset_id, consolidation_id):
                continue
            okay, storage = safe_to_stage(self.root, int(source.get("estimate", 0)), self.reserve_bytes, self.storage_multiplier)
            if not okay:
                return self._write(state="RESOURCE_PAUSED", stage="STORAGE_GUARD", dataset_id=dataset_id,
                                   run_id=run_id, consolidation_id=consolidation_id, storage=storage)
            self._write(state="RUNNING", stage="DOWNLOAD", dataset_id=dataset_id, run_id=run_id,
                        consolidation_id=consolidation_id, storage=storage)
            artifact = download(self.root, source)
            if source.get("metadata_url"):
                download(self.root, {**source, "id": source["id"] + "-metadata", "url": source["metadata_url"]})
            self._write(state="RUNNING", stage="EXTRACT", dataset_id=dataset_id, run_id=run_id)
            extract(self.root, source, artifact)
            self._launch_partitions(dataset_id, self.workers)
            self._write(state="RUNNING", stage="PUBLISH", dataset_id=dataset_id, run_id=run_id,
                        partitions=self.workers)
            self._run([sys.executable, "scripts/publish_completed_shards.py", "--root", str(self.root),
                       "--config", str(self.config_path), "--output-uri", self.output_uri, "--run-id", run_id,
                       "--dataset", dataset_id, "--shard-count", str(self.workers)],
                      log_name=f"{dataset_id}-publish.log")
            self._write(state="RUNNING", stage="VERIFY_PACKAGES", dataset_id=dataset_id, run_id=run_id)
            self._run([sys.executable, "-m", "everbar_motherlode.cli", "verify-distributed-run", "--root", str(self.root),
                       "--config", str(self.config_path), "--dataset", dataset_id, "--shard-count", str(self.workers),
                       "--run-id", run_id, "--output-uri", self.output_uri], log_name=f"{dataset_id}-verify.log")
            self._write(state="RUNNING", stage="RECEIPT_RECONCILIATION_AND_COMPACTION", dataset_id=dataset_id,
                        run_id=run_id, consolidation_id=consolidation_id)
            self._run([sys.executable, "-m", "everbar_motherlode.cli", "stream-consolidate", "--workspace", str(self.root),
                       "--source-uri", self.output_uri, "--output-uri", self.output_uri, "--run-id", run_id,
                       "--dataset", dataset_id, "--shard-count", str(self.workers), "--consolidation-id", consolidation_id],
                      log_name=f"{dataset_id}-consolidate.log")
            removed = self._release_source_local_payload(dataset_id, self.workers)
            receipt = {"state": TERMINAL_COMPLETE, "policy_id": POLICY_ID, "dataset_id": dataset_id,
                       "run_id": run_id, "consolidation_id": consolidation_id, "finished_at": time.time(),
                       "released_local_payload_paths": removed,
                       "durability": "R2 package verification plus receipt-only canonical compaction completed"}
            writej(self.root / "progress" / "remaining-sources" / f"{dataset_id}.json", receipt)
            self._write(state="RUNNING", stage="SOURCE_COMPLETE", dataset_id=dataset_id, run_id=run_id,
                        consolidation_id=consolidation_id, released_local_payload_paths=removed)
        return self._write(state=TERMINAL_COMPLETE, stage="ALL_AUTOMATED_ELIGIBLE_SOURCES_COMPLETE", queued_sources=queued)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    args = parser.parse_args(argv)
    result = Autopilot(root=args.root, config_path=args.config, plan_path=args.plan).run()
    print(json.dumps(result, sort_keys=True))
    return 0 if result["state"] != "RESOURCE_PAUSED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
