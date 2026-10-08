#!/usr/bin/env python3
"""Copy one deterministic R2 inventory partition into a mounted NAS archive.

This command is deliberately copy-only.  A separate coverage verifier and an
explicit deletion command are required before any R2 deletion can be allowed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from everbar_motherlode.nas_archive import migrate_worker


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--worker-index", type=int, required=True)
    parser.add_argument("--worker-count", type=int, required=True)
    parser.add_argument("--pack-mib", type=int, default=256)
    parser.add_argument("--small-under-mib", type=int, default=1)
    parser.add_argument("--chunk-mib", type=int, default=256)
    args = parser.parse_args()
    result = migrate_worker(
        inventory=args.inventory,
        archive_root=args.archive_root,
        worker_index=args.worker_index,
        worker_count=args.worker_count,
        pack_bytes=args.pack_mib * 1024 * 1024,
        small_object_bytes=args.small_under_mib * 1024 * 1024,
        chunk_bytes=args.chunk_mib * 1024 * 1024,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
