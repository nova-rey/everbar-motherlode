"""Resumable, receipt-backed R2 archive writer for a mounted NAS.

The archive intentionally does *not* delete any source objects.  It stores
small R2 objects in self-describing packs and larger objects separately so a
filesystem does not need to hold millions of tiny files.  Every completed
payload has a source-metadata check, a SHA-256 receipt, and an atomic rename.
"""
from __future__ import annotations

import hashlib
import contextlib
import json
import os
import sqlite3
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
    with temporary.open("wb") as handle, open(os.devnull, "w") as quiet:
        # ``stream_r2_pack`` takes the pack *capacity* (and deliberately
        # rejects capacities below 1 MiB); a final small pack may itself be
        # only a few hundred bytes.  Per-record event logs are retained in the
        # receipt, not duplicated into multi-gigabyte worker logs.
        with contextlib.redirect_stderr(quiet):
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
    with temporary.open("wb") as handle, open(os.devnull, "w") as quiet:
        with contextlib.redirect_stderr(quiet):
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


def verify_coverage(*, inventory: Path, archive_root: Path, verify_payload_hashes: bool = False) -> dict:
    """Prove an inventory has NAS receipt coverage without deleting R2.

    SQLite keeps the reconciliation bounded: the inventory can contain tens of
    millions of objects, while the process retains only one receipt at a time.
    """
    database = archive_root / "staging" / f"coverage-{inventory.name}.sqlite"
    database.parent.mkdir(parents=True, exist_ok=True)
    if database.exists():
        database.unlink()
    connection = sqlite3.connect(database)
    try:
        connection.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE expected (object_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL);
            CREATE TABLE covered (object_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL, payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL);
        """)
        digest = hashlib.sha256(); expected_count = expected_bytes = 0
        batch: list[tuple[str, str, str, int, str]] = []
        for row in iter_inventory(inventory):
            encoded = _canonical(row) + b"\n"
            digest.update(encoded); expected_count += 1; expected_bytes += int(row["size"])
            batch.append((row["object_id"], row["bucket"], row["key"], int(row["size"]), row["etag"]))
            if len(batch) >= 10_000:
                connection.executemany("INSERT INTO expected VALUES (?, ?, ?, ?, ?)", batch); connection.commit(); batch.clear()
        if batch:
            connection.executemany("INSERT INTO expected VALUES (?, ?, ?, ?, ?)", batch); connection.commit()

        receipt_paths = sorted((archive_root / "r2" / "packs").glob("*/receipt.json")) + sorted((archive_root / "r2" / "objects").glob("*/receipt.json"))
        covered_rows = 0; bad_payloads: list[str] = []
        for receipt_path in receipt_paths:
            receipt = _read_complete_receipt(receipt_path)
            if receipt is None:
                continue
            payload = receipt_path.parent / ("pack.bin" if receipt.get("kind") == "pack" else "object.bin")
            declared_bytes = int(receipt.get("bytes", -1))
            declared_hash = receipt.get("sha256", "")
            valid_payload = payload.exists() and payload.stat().st_size == declared_bytes
            if valid_payload and verify_payload_hashes:
                valid_payload = _sha256_file(payload) == declared_hash
            if not valid_payload:
                bad_payloads.append(str(receipt_path.relative_to(archive_root)))
                continue
            records = receipt.get("records") if receipt.get("kind") == "pack" else [receipt.get("object")]
            values = [(row["object_id"], row["bucket"], row["key"], int(row["size"]), row["etag"], str(payload.relative_to(archive_root)), declared_hash) for row in records if row]
            connection.executemany("INSERT OR IGNORE INTO covered VALUES (?, ?, ?, ?, ?, ?, ?)", values)
            covered_rows += len(values)
            if covered_rows % 100_000 < len(values):
                connection.commit()
        connection.commit()
        matched = connection.execute("""
            SELECT COUNT(*) FROM expected e JOIN covered c USING (object_id)
             WHERE e.bucket=c.bucket AND e.key=c.key AND e.size=c.size AND e.etag=c.etag
        """).fetchone()[0]
        missing = connection.execute("SELECT COUNT(*) FROM expected e LEFT JOIN covered c USING (object_id) WHERE c.object_id IS NULL").fetchone()[0]
        mismatched = connection.execute("""
            SELECT COUNT(*) FROM expected e JOIN covered c USING (object_id)
             WHERE NOT (e.bucket=c.bucket AND e.key=c.key AND e.size=c.size AND e.etag=c.etag)
        """).fetchone()[0]
        extra = connection.execute("SELECT COUNT(*) FROM covered c LEFT JOIN expected e USING (object_id) WHERE e.object_id IS NULL").fetchone()[0]
        result = {
            "schema": NAS_ARCHIVE_SCHEMA,
            "state": "COMPLETE" if not (missing or mismatched or bad_payloads) else "INCOMPLETE",
            "inventory": str(inventory),
            "inventory_content_sha256": digest.hexdigest(),
            "expected_objects": expected_count,
            "expected_bytes": expected_bytes,
            "matched_objects": matched,
            "missing_objects": missing,
            "mismatched_objects": mismatched,
            "extra_objects": extra,
            "bad_payload_receipts": bad_payloads[:100],
            "bad_payload_count": len(bad_payloads),
            "verify_payload_hashes": verify_payload_hashes,
            "database": str(database),
            "completed_at": time.time(),
        }
        _write_json_atomic(archive_root / "receipts" / f"coverage-{inventory.name}.json", result)
        return result
    finally:
        connection.close()
