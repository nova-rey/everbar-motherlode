#!/usr/bin/env python3
"""Relay R2 object bytes through this host to an iCloud Drive Mac, resumably.

Credentials are intentionally never read here: the R2 stream command executes
on the designated source host.  Each completed chunk is SHA-256 checked on
both sides, explicitly evicted with ``brctl``, and receipted before the next
chunk begins.  Deletion is opt-in and requires a complete object manifest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

from everbar_motherlode.icloud_migration import MIGRATION_SCHEMA, STREAM_EVENT_PREFIX, iter_inventory


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, indent=2) + "\n"


def _ssh(host: str, command: str, *, known_hosts: Path | None = None, stdin=None, capture: bool = True) -> subprocess.Popen:
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
    if known_hosts:
        argv += ["-o", f"UserKnownHostsFile={known_hosts}"]
    argv += [host, command]
    return subprocess.Popen(argv, stdin=stdin, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE,
                            text=False)


def _mac_write_command(remote_path: str) -> str:
    """Return the Mac-side atomic write/hash/upload/evict protocol.

    `brctl evict` is the only supported proof that CloudDocs has accepted the
    complete file, but large files may need a short upload window before the
    first eviction succeeds.  Retrying *only the eviction* avoids retransmitting
    a verified local chunk and keeps the Mac below its small physical disk.
    """
    parent = shlex.quote(str(Path(remote_path).parent)); target = shlex.quote(remote_path)
    return (
        f"set -euo pipefail; mkdir -p {parent}; tmp={target}.partial; cat > \"$tmp\"; mv \"$tmp\" {target}; "
        f"digest=$(shasum -a 256 {target} | awk '{{print $1}}'); evicted=0; "
        f"for delay in 2 4 8 16 30 30 30 30 30 30; do if brctl evict {target}; then evicted=1; break; fi; sleep \"$delay\"; done; "
        f"test \"$evicted\" = 1; flags=$(ls -lO {target}); printf '%s\\t%s\\n' \"$digest\" \"$flags\""
    )


def _remote_write_and_evict(host: str, known_hosts: Path, destination: str, local_path: Path, remote_path: str) -> dict:
    """Copy a small receipt/manifest atomically, hash it remotely, then evict it."""
    command = _mac_write_command(remote_path)
    with local_path.open("rb") as source:
        proc = _ssh(host, command, known_hosts=known_hosts, stdin=source)
        stdout, stderr = proc.communicate()
    if proc.returncode:
        raise RuntimeError(f"Mac receipt upload failed: {stderr.decode(errors='replace')[-500:]}")
    digest, _, flags = stdout.decode().strip().partition("\t")
    local_digest = hashlib.sha256(local_path.read_bytes()).hexdigest()
    if digest != local_digest or "dataless" not in flags:
        raise RuntimeError("Mac receipt did not hash and evict as required")
    return {"sha256": digest, "evicted_flags": flags}


def _receive_chunk(host: str, known_hosts: Path, destination: str, remote_path: str, producer_stdout, size: int) -> dict:
    command = _mac_write_command(remote_path)
    sink = _ssh(host, command, known_hosts=known_hosts, stdin=subprocess.PIPE)
    remaining = size
    while remaining:
        block = producer_stdout.read(min(8 * 1024 * 1024, remaining))
        if not block:
            sink.stdin.close(); sink.wait()
            raise RuntimeError("source stream ended before declared chunk boundary")
        sink.stdin.write(block); remaining -= len(block)
    sink.stdin.close()
    # ``communicate`` attempts to flush ``stdin`` even after a caller closed
    # it, which raises locally and can obscure an otherwise successful remote
    # iCloud receipt.  Wait and drain the two finite result streams directly.
    sink.wait()
    stdout, stderr = sink.stdout.read(), sink.stderr.read()
    if sink.returncode:
        raise RuntimeError(f"Mac chunk upload failed: {stderr.decode(errors='replace')[-500:]}")
    digest, _, flags = stdout.decode().strip().partition("\t")
    if len(digest) != 64 or "dataless" not in flags:
        raise RuntimeError("Mac chunk did not report SHA-256 and dataless eviction")
    return {"sha256": digest, "evicted_flags": flags}


def _read_events(stream, events: list[dict], errors: list[str]) -> None:
    for raw in iter(stream.readline, b""):
        line = raw.decode("utf-8", errors="replace").rstrip()
        if line.startswith(STREAM_EVENT_PREFIX):
            try: events.append(json.loads(line[len(STREAM_EVENT_PREFIX):]))
            except json.JSONDecodeError as exc: errors.append(f"invalid source event: {exc}")
        elif line:
            errors.append(line[-500:])


def _receipt_path(work_root: Path, object_id: str, chunk_index: int) -> Path:
    return work_root / "objects" / object_id / "receipts" / f"chunk-{chunk_index:08d}.json"


def _existing_chunk_receipt(path: Path) -> dict | None:
    try:
        receipt = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    return receipt if receipt.get("state") == "COMPLETE" and len(receipt.get("sha256", "")) == 64 else None


def migrate_one(record: dict, args: argparse.Namespace) -> dict:
    oid = record["object_id"]
    object_root = args.work_root / "objects" / oid
    object_root.mkdir(parents=True, exist_ok=True)
    final_receipt = object_root / "object-receipt.json"
    if final_receipt.exists():
        existing = json.loads(final_receipt.read_text())
        if existing.get("state") == "COMPLETE" and existing.get("inventory") == record:
            return {"state": "SKIPPED_COMPLETE", "object_id": oid}
    source_command = (
        f"cd {shlex.quote(args.remote_repo)} && RCLONE_CONFIG={shlex.quote(args.remote_rclone_config)} "
        f"{shlex.quote(args.remote_python)} -m everbar_motherlode.cli r2-icloud-source-stream "
        f"--bucket {shlex.quote(record['bucket'])} --key {shlex.quote(record['key'])} --chunk-bytes {args.chunk_bytes}"
    )
    producer = _ssh(args.source_host, source_command, stdin=None, capture=True)
    events: list[dict] = []; errors: list[str] = []
    reader = threading.Thread(target=_read_events, args=(producer.stderr, events, errors), daemon=True); reader.start()
    chunk_count = (record["size"] + args.chunk_bytes - 1) // args.chunk_bytes
    receipts = []
    destination_object = f"{args.icloud_destination.rstrip('/')}/objects/{oid}"
    for index in range(chunk_count):
        size = min(args.chunk_bytes, record["size"] - index * args.chunk_bytes)
        local_receipt = _receipt_path(args.work_root, oid, index)
        previous = _existing_chunk_receipt(local_receipt)
        if previous is not None:
            # Still consume source bytes so its final whole-object digest covers
            # exactly the inventory object without retaining a local payload.
            remaining = size
            while remaining:
                block = producer.stdout.read(min(8 * 1024 * 1024, remaining))
                if not block: raise RuntimeError("source stream ended while skipping verified chunk")
                remaining -= len(block)
            receipts.append(previous); continue
        target = f"{destination_object}/chunks/{index:08d}.bin"
        mac = _receive_chunk(args.mac_host, args.mac_known_hosts, args.icloud_destination, target, producer.stdout, size)
        receipt = {"schema": MIGRATION_SCHEMA, "state": "COMPLETE", "object_id": oid, "chunk_index": index,
                   "offset": index * args.chunk_bytes, "size": size, "sha256": mac["sha256"],
                   "mac_evicted_flags": mac["evicted_flags"], "created_at": time.time()}
        local_receipt.parent.mkdir(parents=True, exist_ok=True); local_receipt.write_text(_json(receipt))
        remote_receipt = f"{destination_object}/receipts/chunk-{index:08d}.json"
        _remote_write_and_evict(args.mac_host, args.mac_known_hosts, args.icloud_destination, local_receipt, remote_receipt)
        receipts.append(receipt)
    producer.stdout.close(); rc = producer.wait(); reader.join(timeout=10)
    if rc or errors:
        raise RuntimeError(f"source stream failed rc={rc}: {'; '.join(errors[-3:])}")
    chunk_events = sorted((event for event in events if event.get("event") == "CHUNK"), key=lambda x: x["index"])
    terminal = next((event for event in events if event.get("event") == "OBJECT"), None)
    if terminal is None or terminal["size"] != record["size"] or terminal.get("etag") != record["etag"]:
        raise RuntimeError("source terminal receipt does not match inventory")
    if len(chunk_events) != chunk_count or len(receipts) != chunk_count:
        raise RuntimeError("source chunk receipt count does not match inventory")
    for event, receipt in zip(chunk_events, receipts):
        if event["index"] != receipt["chunk_index"] or event["size"] != receipt["size"] or event["sha256"] != receipt["sha256"]:
            raise RuntimeError("source and Mac chunk hashes differ; object retained in R2")
    manifest = {"schema": MIGRATION_SCHEMA, "state": "COMPLETE", "inventory": record, "source": terminal,
                "chunks": receipts, "manifest_sha256": None, "completed_at": time.time()}
    manifest["manifest_sha256"] = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    final_receipt.write_text(_json(manifest))
    _remote_write_and_evict(args.mac_host, args.mac_known_hosts, args.icloud_destination, final_receipt,
                            f"{destination_object}/object-manifest.json")
    if args.delete_verified:
        delete_command = (
            f"cd {shlex.quote(args.remote_repo)} && RCLONE_CONFIG={shlex.quote(args.remote_rclone_config)} "
            f"{shlex.quote(args.remote_python)} -m everbar_motherlode.cli r2-icloud-delete-verified "
            f"--bucket {shlex.quote(record['bucket'])} --key {shlex.quote(record['key'])} "
            f"--expected-size {record['size']} --expected-etag {shlex.quote(record['etag'])}"
        )
        deleted = _ssh(args.source_host, delete_command, capture=True); stdout, stderr = deleted.communicate()
        if deleted.returncode: raise RuntimeError(f"source deletion refused: {stderr.decode(errors='replace')[-500:]}")
        # The object manifest remains immutable after its iCloud proof.  A
        # separate deletion receipt prevents a post-verification mutation from
        # changing the manifest hash that authorized deletion.
        deletion_receipt = object_root / "source-deletion-receipt.json"
        deletion_receipt.write_text(_json(json.loads(stdout)))
        _remote_write_and_evict(args.mac_host, args.mac_known_hosts, args.icloud_destination, deletion_receipt,
                                f"{destination_object}/source-deletion-receipt.json")
    return {"state": "COMPLETE", "object_id": oid, "size": record["size"], "deleted": bool(args.delete_verified)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--source-host", required=True); parser.add_argument("--remote-repo", required=True)
    parser.add_argument("--remote-python", required=True); parser.add_argument("--remote-rclone-config", required=True)
    parser.add_argument("--mac-host", required=True); parser.add_argument("--mac-known-hosts", type=Path, required=True)
    parser.add_argument("--icloud-destination", required=True); parser.add_argument("--chunk-mib", type=int, default=512)
    parser.add_argument("--delete-verified", action="store_true")
    parser.add_argument("--limit", type=int); parser.add_argument("--object-id")
    args = parser.parse_args(argv); args.chunk_bytes = args.chunk_mib * 1024 * 1024
    args.work_root.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in iter_inventory(args.inventory):
        if args.object_id and record["object_id"] != args.object_id: continue
        print(json.dumps(migrate_one(record, args), sort_keys=True), flush=True); count += 1
        if args.limit and count >= args.limit: break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
