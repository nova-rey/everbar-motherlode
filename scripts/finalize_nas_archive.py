#!/usr/bin/env python3
"""Autonomously run the non-overlapping final NAS archive safety gates.

The process waits for all copy-worker receipts, creates a fresh R2 inventory,
requires it to match the migration inventory, rehashes NAS payloads during
coverage reconciliation, and only then invokes the separately gated R2 delete
operation.  It intentionally does not touch iCloud: CloudDocs is a distinct
source that needs its own receipt after its dataless placeholders are handled.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from everbar_motherlode.icloud_migration import inventory_r2
from everbar_motherlode.nas_archive import delete_verified_inventory, inventory_content_sha256, verify_coverage


def write_status(path: Path, **value: object) -> None:
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps({"updated_at": time.time(), **value}, sort_keys=True) + "\n")
    os.replace(temporary, path)


def complete_workers(archive: Path, inventory: Path, workers: int) -> bool:
    root = archive / "r2" / "workers" / inventory.name
    for index in range(workers):
        try:
            state = json.loads((root / f"worker-{index:02d}-of-{workers:02d}.json").read_text()).get("state")
        except (FileNotFoundError, json.JSONDecodeError):
            return False
        if state != "COMPLETE":
            return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--migration-inventory", type=Path, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--poll-seconds", type=int, default=120)
    parser.add_argument("--confirm-r2-deletion", action="store_true")
    args = parser.parse_args()
    receipt_dir = args.archive_root / "receipts"
    receipt_dir.mkdir(parents=True, exist_ok=True)
    status = receipt_dir / "nas-archive-finalizer-latest.json"
    while not complete_workers(args.archive_root, args.migration_inventory, args.workers):
        write_status(status, state="WAITING_FOR_COPY", migration_inventory=str(args.migration_inventory), workers=args.workers)
        time.sleep(args.poll_seconds)
    write_status(status, state="BUILDING_FINAL_INVENTORY", migration_inventory=str(args.migration_inventory))
    final = args.archive_root / "inventory" / f"r2-final-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl.gz"
    inventory_r2(final, ["everbar-motherlode-input", "everbar-motherlode-output"])
    migration_digest = inventory_content_sha256(args.migration_inventory)
    final_digest = inventory_content_sha256(final)
    if migration_digest != final_digest:
        write_status(status, state="INVENTORY_MISMATCH", migration_inventory=str(args.migration_inventory), final_inventory=str(final), migration_digest=migration_digest, final_digest=final_digest)
        return 2
    write_status(status, state="VERIFYING_NAS_PAYLOAD_HASHES", final_inventory=str(final), inventory_digest=final_digest)
    coverage = verify_coverage(inventory=final, archive_root=args.archive_root, verify_payload_hashes=True)
    if coverage["state"] != "COMPLETE":
        write_status(status, state="COVERAGE_INCOMPLETE", final_inventory=str(final), coverage=coverage)
        return 3
    coverage_path = receipt_dir / f"coverage-{final.name}.json"
    if not args.confirm_r2_deletion:
        write_status(status, state="R2_DELETE_READY", final_inventory=str(final), coverage_receipt=str(coverage_path), coverage=coverage)
        return 0
    write_status(status, state="DELETING_VERIFIED_R2", final_inventory=str(final), coverage_receipt=str(coverage_path))
    deletion = delete_verified_inventory(inventory=final, archive_root=args.archive_root, coverage_receipt=coverage_path)
    write_status(status, state="R2_EMPTY_ICLOUD_PENDING", final_inventory=str(final), coverage=coverage, deletion=deletion)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
