"""Resumable, receipt-backed R2 archive writer for a mounted NAS.

The archive intentionally does *not* delete any source objects.  It stores
small R2 objects in self-describing packs and larger objects separately so a
filesystem does not need to hold millions of tiny files.  Every completed
payload has a source-metadata check, a SHA-256 receipt, and an atomic rename.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Iterable

from .icloud_migration import PACK_MAGIC, _canonical, iter_inventory, pack_frame_size, stream_r2_object, stream_r2_pack


NAS_ARCHIVE_SCHEMA = "everbar-motherlode.r2-nas-archive/v1"


def _pack_id(records: list[dict]) -> str:
    return hashlib.sha256(_canonical({"schema": NAS_ARCHIVE_SCHEMA, "objects": [row["object_id"] for row in records]})).hexdigest()


def _sha256_file(path: Path, block_bytes: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(block_bytes), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_bytes(_canonical(value) + b"\n")
    os.replace(temporary, path)


def _read_complete_receipt(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    return value if value.get("state") == "COMPLETE" and value.get("schema") == NAS_ARCHIVE_SCHEMA else None


def selected_records(inventory: Path, worker_index: int, worker_count: int) -> Iterable[dict]:
    """Yield a deterministic worker partition without retaining the inventory."""
    if worker_count < 1 or not 0 <= worker_index < worker_count:
        raise ValueError("invalid worker partition")
    for record in iter_inventory(inventory):
        if int(record["object_id"][:16], 16) % worker_count == worker_index:
            yield record


def iter_small_packs(records: Iterable[dict], *, pack_bytes: int, small_object_bytes: int, max_records: int = 10_000) -> Iterable[list[dict]]:
    """Group selected small records into bounded, deterministic framed packs."""
    if pack_bytes < 1024 * 1024 or small_object_bytes < 0 or max_records < 1:
        raise ValueError("invalid pack bounds")
    group: list[dict] = []
    used = len(PACK_MAGIC)
    for record in records:
        if int(record["size"]) > small_object_bytes:
            continue
        frame = pack_frame_size(record)
        if frame + len(PACK_MAGIC) > pack_bytes:
            continue
        if group and (used + frame > pack_bytes or len(group) >= max_records):
            yield group
            group = []
            used = len(PACK_MAGIC)
        group.append(record)
        used += frame
    if group:
        yield group


def _copy_pack(records: list[dict], archive_root: Path, pack_bytes: int) -> dict:
    pack_id = _pack_id(records)
    root = archive_root / "r2" / "packs" / pack_id
    payload = root / "pack.bin"
    receipt_path = root / "receipt.json"
    expected = len(PACK_MAGIC) + sum(pack_frame_size(row) for row in records)
    prior = _read_complete_receipt(receipt_path)
    if prior and payload.exists() and payload.stat().st_size == expected and _sha256_file(payload) == prior.get("sha256"):
        return {"state": "SKIPPED_COMPLETE", "kind": "pack", "pack_id": pack_id, "objects": len(records), "bytes": expected}
    root.mkdir(parents=True, exist_ok=True)
    temporary = payload.with_name(payload.name + ".partial")
    with temporary.open("wb") as handle:
        # ``stream_r2_pack`` takes the pack *capacity* (and deliberately
        # rejects capacities below 1 MiB); a final small pack may itself be
        # only a few hundred bytes.
        terminal = stream_r2_pack(records, pack_bytes, output=handle)
        handle.flush()
        os.fsync(handle.fileno())
    digest = _sha256_file(temporary)
    if temporary.stat().st_size != expected or terminal["pack_size"] != expected or digest != terminal["sha256"]:
        temporary.unlink(missing_ok=True)
        raise RuntimeError("NAS pack hash/size mismatch; source retained")
    os.replace(temporary, payload)
    receipt = {
        "schema": NAS_ARCHIVE_SCHEMA,
        "state": "COMPLETE",
        "kind": "pack",
        "pack_id": pack_id,
        "bytes": expected,
        "sha256": digest,
        "records": records,
        "source_events": terminal["records"],
        "completed_at": time.time(),
    }
    _write_json_atomic(receipt_path, receipt)
    return {"state": "COMPLETE", "kind": "pack", "pack_id": pack_id, "objects": len(records), "bytes": expected}


def _copy_object(record: dict, archive_root: Path, chunk_bytes: int) -> dict:
    root = archive_root / "r2" / "objects" / record["object_id"]
    payload = root / "object.bin"
    receipt_path = root / "receipt.json"
    prior = _read_complete_receipt(receipt_path)
    if prior and payload.exists() and payload.stat().st_size == int(record["size"]) and _sha256_file(payload) == prior.get("sha256"):
        return {"state": "SKIPPED_COMPLETE", "kind": "object", "object_id": record["object_id"], "objects": 1, "bytes": int(record["size"])}
    root.mkdir(parents=True, exist_ok=True)
    temporary = payload.with_name(payload.name + ".partial")
    with temporary.open("wb") as handle:
        terminal = stream_r2_object(record["bucket"], record["key"], chunk_bytes, output=handle)
        handle.flush()
        os.fsync(handle.fileno())
    digest = _sha256_file(temporary)
    if temporary.stat().st_size != int(record["size"]) or digest != terminal["stream_sha256"] or terminal["etag"] != record["etag"]:
        temporary.unlink(missing_ok=True)
        raise RuntimeError("NAS object hash/metadata mismatch; source retained")
    os.replace(temporary, payload)
    receipt = {
        "schema": NAS_ARCHIVE_SCHEMA,
        "state": "COMPLETE",
        "kind": "object",
        "object": record,
        "bytes": int(record["size"]),
        "sha256": digest,
        "source_stream": terminal,
        "completed_at": time.time(),
    }
    _write_json_atomic(receipt_path, receipt)
    return {"state": "COMPLETE", "kind": "object", "object_id": record["object_id"], "objects": 1, "bytes": int(record["size"])}


def migrate_worker(*, inventory: Path, archive_root: Path, worker_index: int, worker_count: int, pack_bytes: int = 256 * 1024 * 1024, small_object_bytes: int = 1024 * 1024, chunk_bytes: int = 256 * 1024 * 1024) -> dict:
    """Copy one deterministic partition of inventory to NAS; never delete R2."""
    # Progress is scoped by immutable inventory filename: input and output
    # lanes may run concurrently with the same worker indexes.
    worker_root = archive_root / "r2" / "workers" / inventory.name
    progress_path = worker_root / f"worker-{worker_index:02d}-of-{worker_count:02d}.json"
    started = time.time()
    totals = {"objects": 0, "bytes": 0, "units": 0, "skipped_units": 0}
    for group in iter_small_packs(
        selected_records(inventory, worker_index, worker_count),
        pack_bytes=pack_bytes,
        small_object_bytes=small_object_bytes,
    ):
        result = _copy_pack(group, archive_root, pack_bytes)
        totals["objects"] += result["objects"]; totals["bytes"] += result["bytes"]; totals["units"] += 1
        totals["skipped_units"] += result["state"] == "SKIPPED_COMPLETE"
        _write_json_atomic(progress_path, {"schema": NAS_ARCHIVE_SCHEMA, "state": "RUNNING", "worker_index": worker_index, "worker_count": worker_count, "inventory": str(inventory), "totals": totals, "updated_at": time.time()})
    # A second streaming pass avoids materialising millions of inventory rows
    # in RAM merely to process the large-object lane.
    for record in selected_records(inventory, worker_index, worker_count):
        if int(record["size"]) <= small_object_bytes:
            continue
        result = _copy_object(record, archive_root, chunk_bytes)
        totals["objects"] += result["objects"]; totals["bytes"] += result["bytes"]; totals["units"] += 1
        totals["skipped_units"] += result["state"] == "SKIPPED_COMPLETE"
        _write_json_atomic(progress_path, {"schema": NAS_ARCHIVE_SCHEMA, "state": "RUNNING", "worker_index": worker_index, "worker_count": worker_count, "inventory": str(inventory), "totals": totals, "updated_at": time.time()})
    result = {"schema": NAS_ARCHIVE_SCHEMA, "state": "COMPLETE", "worker_index": worker_index, "worker_count": worker_count, "inventory": str(inventory), "totals": totals, "started_at": started, "completed_at": time.time()}
    _write_json_atomic(progress_path, result)
    return result
