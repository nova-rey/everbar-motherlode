#!/usr/bin/env python3
"""Keep a bounded NAS archive worker fleet healthy and receipt its progress.

It never deletes R2 or iCloud data.  A worker that exits without a COMPLETE
receipt is restarted from its atomic partial payload; completed workers are
left alone.  The watcher is intended for a local detached session, not a
cloud VM with irreplaceable state.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path


def _tmux_alive(session: str) -> bool:
    return subprocess.run(["tmux", "has-session", "-t", session], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def _read(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _atomic(path: Path, value: dict) -> None:
    temp = path.with_name(path.name + ".partial")
    temp.write_text(json.dumps(value, sort_keys=True) + "\n")
    os.replace(temp, path)


def _launch(*, session: str, command: str) -> None:
    subprocess.run(["tmux", "new-session", "-d", "-s", session, command], check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--rclone-config", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()
    if args.workers < 1 or args.poll_seconds < 5:
        parser.error("workers must be positive and poll-seconds at least 5")
    worker_dir = args.archive_root / "r2" / "workers" / args.inventory.name
    log_dir = args.archive_root / "logs" / "watcher-workers"
    status_path = args.archive_root / "receipts" / "nas-archive-watcher-latest.json"
    log_dir.mkdir(parents=True, exist_ok=True)
    while True:
        complete = running = restarted = 0
        failures: list[dict] = []
        for index in range(args.workers):
            receipt = _read(worker_dir / f"worker-{index:02d}-of-{args.workers:02d}.json")
            if receipt and receipt.get("state") == "COMPLETE":
                complete += 1
                continue
            session = f"motherlode-r2-nas-output32-{index}" if args.workers == 32 else f"motherlode-r2-nas-worker-{index}"
            if _tmux_alive(session):
                running += 1
                continue
            log = log_dir / f"worker-{index:02d}.log"
            command = (
                f"cd {args.repo} && exec env RCLONE_CONFIG={args.rclone_config} "
                f".venv/bin/python scripts/migrate_r2_to_nas.py --inventory {args.inventory} "
                f"--archive-root {args.archive_root} --worker-index {index} --worker-count {args.workers} "
                f">> {log} 2>&1"
            )
            try:
                _launch(session=session, command=command)
                restarted += 1
            except subprocess.CalledProcessError as exc:
                failures.append({"worker": index, "error": str(exc)})
        state = "COMPLETE" if complete == args.workers else "RUNNING"
        _atomic(status_path, {
            "state": state, "inventory": str(args.inventory), "workers": args.workers,
            "complete_workers": complete, "running_workers": running,
            "restarted_workers": restarted, "failures": failures, "updated_at": time.time(),
        })
        if state == "COMPLETE":
            return 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
