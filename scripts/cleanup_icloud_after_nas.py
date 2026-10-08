#!/usr/bin/env python3
"""Free verified duplicate CloudDocs migration placeholders after NAS/R2 closure."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path


def digest(rows: list[dict]) -> str:
    canonical = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in sorted(rows, key=lambda row: row["path"]))
    return hashlib.sha256(canonical.encode()).hexdigest()


def remote_rows(host: str, known_hosts: Path) -> list[dict]:
    command = 'root="$HOME/Library/Mobile Documents/com~apple~CloudDocs/Motherlode-R2-Migration"; test -d "$root" || exit 44; cd "$root"; LC_ALL=C find . -type f -exec stat -f "%z %N" {} \\;'
    result = subprocess.run(["ssh", "-o", f"UserKnownHostsFile={known_hosts}", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host, command], text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"cannot inventory iCloud migration source: {result.stderr[-300:]}")
    return [{"size": int(line.split(" ", 1)[0]), "path": line.split(" ", 1)[1]} for line in result.stdout.splitlines() if " " in line]


def write_status(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--icloud-inventory", type=Path, required=True)
    parser.add_argument("--mac-host", required=True)
    parser.add_argument("--known-hosts", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=int, default=300)
    args = parser.parse_args()
    status = args.archive_root / "receipts" / "icloud-cleanup-latest.json"
    deletion = args.archive_root / "receipts" / "r2-deletion" / "terminal.json"
    expected = [json.loads(line) for line in args.icloud_inventory.read_text().splitlines() if line.strip()]
    expected_digest = digest(expected)
    while not deletion.exists() or json.loads(deletion.read_text()).get("state") != "COMPLETE":
        write_status(status, {"state": "WAITING_FOR_R2_EMPTY", "updated_at": time.time()})
        time.sleep(args.poll_seconds)
    try:
        observed = remote_rows(args.mac_host, args.known_hosts)
    except RuntimeError as exc:
        write_status(status, {"state": "ICLOUD_UNREACHABLE", "error": str(exc), "updated_at": time.time()})
        return 2
    if digest(observed) != expected_digest:
        write_status(status, {"state": "ICLOUD_INVENTORY_MISMATCH", "expected_digest": expected_digest, "observed_digest": digest(observed), "updated_at": time.time()})
        return 3
    cleanup = 'root="$HOME/Library/Mobile Documents/com~apple~CloudDocs/Motherlode-R2-Migration"; test -d "$root"; find "$root" -mindepth 1 -depth -delete; rmdir "$root"; test ! -e "$root"'
    result = subprocess.run(["ssh", "-o", f"UserKnownHostsFile={args.known_hosts}", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", args.mac_host, cleanup], text=True, capture_output=True)
    if result.returncode:
        write_status(status, {"state": "ICLOUD_DELETE_FAILED", "error": result.stderr[-500:], "updated_at": time.time()})
        return 4
    write_status(status, {"state": "COMPLETE", "source_files": len(expected), "source_logical_bytes": sum(row["size"] for row in expected), "inventory_digest": expected_digest, "updated_at": time.time()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
