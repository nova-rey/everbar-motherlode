#!/usr/bin/env python3
"""Copy one deterministic R2 inventory partition into a mounted NAS archive.

This command is deliberately copy-only.  A separate coverage verifier and an
explicit deletion command are required before any R2 deletion can be allowed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from everbar_motherlode.nas_archive import delete_verified_inventory, migrate_worker, verify_coverage


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--worker-index", type=int, default=0)
    parser.add_argument("--worker-count", type=int, default=1)
    parser.add_argument("--pack-mib", type=int, default=256)
    parser.add_argument("--small-under-mib", type=int, default=1)
    parser.add_argument("--chunk-mib", type=int, default=256)
    parser.add_argument("--pack-fetch-workers", type=int, default=4)
    parser.add_argument("--verify-coverage", action="store_true")
    parser.add_argument("--verify-payload-hashes", action="store_true")
    parser.add_argument("--delete-verified", action="store_true")
    parser.add_argument("--coverage-receipt", type=Path)
    parser.add_argument("--confirm-r2-deletion")
    args = parser.parse_args()
    if args.verify_coverage:
        result = verify_coverage(inventory=args.inventory, archive_root=args.archive_root, verify_payload_hashes=args.verify_payload_hashes)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["state"] == "COMPLETE" else 1
    if args.delete_verified:
        if args.confirm_r2_deletion != "DELETE_VERIFIED_R2" or args.coverage_receipt is None:
            parser.error("R2 deletion requires --coverage-receipt and --confirm-r2-deletion DELETE_VERIFIED_R2")
        result = delete_verified_inventory(inventory=args.inventory, archive_root=args.archive_root, coverage_receipt=args.coverage_receipt)
        print(json.dumps(result, sort_keys=True))
        return 0
    result = migrate_worker(
        inventory=args.inventory,
        archive_root=args.archive_root,
        worker_index=args.worker_index,
        worker_count=args.worker_count,
        pack_bytes=args.pack_mib * 1024 * 1024,
        small_object_bytes=args.small_under_mib * 1024 * 1024,
        chunk_bytes=args.chunk_mib * 1024 * 1024,
        pack_fetch_workers=args.pack_fetch_workers,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
