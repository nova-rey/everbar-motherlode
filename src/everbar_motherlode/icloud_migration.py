"""Receipt-backed R2-to-iCloud evacuation primitives.

This module deliberately keeps object-store credentials at the source host.  A
source stream writes object bytes to stdout and emits only hashes/lengths to
stderr; the coordinator can therefore relay bounded chunks directly to an
iCloud Drive host without retaining corpus payloads locally.
"""
from __future__ import annotations

import hashlib
import gzip
import json
import sys
import time
from pathlib import Path
from typing import BinaryIO, Iterable

from .distributed import _direct_s3_client


STREAM_EVENT_PREFIX = "ICLOUD_R2_STREAM "
MIGRATION_SCHEMA = "everbar-motherlode.r2-icloud-migration/v1"
PACK_MAGIC = b"EMLIPK01"


def object_id(bucket: str, key: str) -> str:
    """Return a filesystem-safe, stable identity without exposing a key in paths."""
    if not bucket or not key:
        raise ValueError("bucket and key are required")
    return hashlib.sha256((bucket + "\0" + key).encode("utf-8")).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _is_gzip_inventory(path: Path) -> bool:
    """Recognize a completed ``.gz`` inventory and its ``.gz.partial`` peer."""
    return path.name.endswith(".gz") or path.name.endswith(".gz.partial")


def _read_partial_inventory(path: Path) -> tuple[hashlib._Hash, dict[str, dict[str, int]], dict[str, str]]:
    """Recover a complete-line JSONL prefix after interruption.

    S3's ``StartAfter`` is exclusive, so the final durable key per bucket is a
    safe resume cursor.  The partial is truncated to the final newline before
    appending, preventing a killed writer's incomplete line from becoming an
    inventory record.
    """
    digest = hashlib.sha256(); counts: dict[str, dict[str, int]] = {}; last: dict[str, str] = {}
    if not path.exists():
        return digest, counts, last
    if _is_gzip_inventory(path):
        # Every pagination page is written as one complete gzip member.  A
        # terminated final append can therefore leave only an unreadable tail;
        # the prior members and their newline-delimited records remain a safe
        # resume prefix.  Do not rewrite a multi-gigabyte checkpoint merely to
        # discard that tail.
        try:
            handle = gzip.open(path, "rb")
            lines = iter(handle.readline, b"")
            for number, line in enumerate(lines, 1):
                if not line.endswith(b"\n"):
                    continue
                record = json.loads(line)
                if record.get("schema") != MIGRATION_SCHEMA:
                    raise ValueError(f"partial inventory line {number} has unexpected schema")
                bucket = record["bucket"]
                row = counts.setdefault(bucket, {"objects": 0, "bytes": 0})
                row["objects"] += 1; row["bytes"] += int(record["size"])
                last[bucket] = record["key"]
                digest.update(line)
        except (EOFError, gzip.BadGzipFile):
            # A killed final page is never authoritative; the complete prefix
            # above is enough for S3 StartAfter resumption.
            pass
        finally:
            try: handle.close()
            except (UnboundLocalError, EOFError, gzip.BadGzipFile): pass
        return digest, counts, last
    data = path.read_bytes()
    newline = data.rfind(b"\n")
    if newline < 0:
        path.write_bytes(b""); return digest, counts, last
    data = data[:newline + 1]
    path.write_bytes(data)
    for number, line in enumerate(data.splitlines(), 1):
        record = json.loads(line)
        if record.get("schema") != MIGRATION_SCHEMA:
            raise ValueError(f"partial inventory line {number} has unexpected schema")
        bucket = record["bucket"]
        row = counts.setdefault(bucket, {"objects": 0, "bytes": 0})
        row["objects"] += 1; row["bytes"] += int(record["size"])
        last[bucket] = record["key"]
        digest.update(line + b"\n")
    return digest, counts, last


def inventory_r2(output: Path, buckets: Iterable[str] | None = None, *, resume: bool = False) -> dict:
    """Write a deterministic, paginated object inventory without loading it all.

    This runs only on the credential-holding source host.  Inventory records
    contain provider metadata, never access credentials.  The atomic summary
    is a prerequisite for any migration or deletion operation.
    """
    client = _direct_s3_client("direct-s3://evacuate/inventory")
    selected = sorted(set(buckets or (row["Name"] for row in client.list_buckets().get("Buckets", []))))
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".partial")
    if output.exists():
        raise FileExistsError(f"completed inventory already exists: {output}")
    if temporary.exists() and not resume:
        raise FileExistsError(f"partial inventory exists; rerun with resume: {temporary}")
    digest, counts, cursors = _read_partial_inventory(temporary) if resume else (hashlib.sha256(), {}, {})
    for bucket in selected:
        count = counts.get(bucket, {}).get("objects", 0)
        total = counts.get(bucket, {}).get("bytes", 0)
        for page in client.get_paginator("list_objects_v2").paginate(**({"Bucket": bucket, **({"StartAfter": cursors[bucket]} if bucket in cursors else {})})):
            page_bytes = bytearray()
            for item in page.get("Contents", []):
                record = {
                    "schema": MIGRATION_SCHEMA,
                    "bucket": bucket,
                    "key": item["Key"],
                    "object_id": object_id(bucket, item["Key"]),
                    "size": int(item["Size"]),
                    "etag": str(item.get("ETag", "")).strip('"'),
                    "last_modified": item.get("LastModified").isoformat() if item.get("LastModified") else None,
                }
                encoded = _canonical(record) + b"\n"
                page_bytes.extend(encoded); digest.update(encoded)
                count += 1; total += record["size"]
                cursors[bucket] = record["key"]
            if page_bytes:
                if _is_gzip_inventory(temporary):
                    # A separate closed member per page gives a bounded
                    # crash-consistent prefix without an extra staging file.
                    with temporary.open("ab") as raw:
                        with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
                            compressed.write(page_bytes)
                else:
                    with temporary.open("ab") as handle:
                        handle.write(page_bytes); handle.flush()
        counts[bucket] = {"objects": count, "bytes": total}
    temporary.replace(output)
    summary = {
        "schema": MIGRATION_SCHEMA,
        "state": "COMPLETE",
        "inventory_path": str(output),
        "inventory_sha256": digest.hexdigest(),
        "buckets": counts,
        "object_count": sum(row["objects"] for row in counts.values()),
        "total_bytes": sum(row["bytes"] for row in counts.values()),
        "created_at": time.time(),
    }
    summary_path = output.with_suffix(output.suffix + ".summary.json")
    summary_path.write_bytes(_canonical(summary) + b"\n")
    return summary


def iter_inventory(path: Path) -> Iterable[dict]:
    """Read only schema-valid inventory records in deterministic file order."""
    opener = gzip.open if _is_gzip_inventory(path) else Path.open
    with opener(path, "rt", encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            record = json.loads(line)
            if record.get("schema") != MIGRATION_SCHEMA:
                raise ValueError(f"inventory line {number} has unexpected schema")
            if record.get("object_id") != object_id(record.get("bucket", ""), record.get("key", "")):
                raise ValueError(f"inventory line {number} has invalid object identity")
            if int(record.get("size", -1)) < 0:
                raise ValueError(f"inventory line {number} has invalid size")
            yield record


def _read_exact(body: object, expected: int) -> bytes:
    pieces: list[bytes] = []; remaining = expected
    while remaining:
        block = body.read(min(8 * 1024 * 1024, remaining))
        if not block:
            break
        pieces.append(block); remaining -= len(block)
    data = b"".join(pieces)
    if len(data) != expected:
        raise RuntimeError(f"R2 response truncated: expected {expected}, received {len(data)}")
    return data


def hash_r2_object(bucket: str, key: str, *, range_bytes: int = 64 * 1024 * 1024, retries: int = 3) -> dict:
    """Hash an immutable object through bounded, retryable S3 ranges only."""
    if range_bytes < 1024 * 1024 or retries < 1:
        raise ValueError("invalid hash range or retry count")
    client = _direct_s3_client("direct-s3://evacuate/hash")
    head = client.head_object(Bucket=bucket, Key=key)
    total = int(head["ContentLength"]); digest = hashlib.sha256()
    for start in range(0, total, range_bytes):
        end = min(total, start + range_bytes) - 1
        last_error: Exception | None = None
        for _ in range(retries):
            try:
                data = _read_exact(client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")["Body"], end - start + 1)
                digest.update(data); break
            except Exception as exc:  # a fresh range request is the retry boundary
                last_error = exc
        else:
            raise RuntimeError(f"R2 hash range {start}-{end} failed after {retries} attempts") from last_error
    return {"event": "OBJECT_HASH", "bucket": bucket, "key": key, "object_id": object_id(bucket, key),
            "size": total, "sha256": digest.hexdigest(), "etag": str(head.get("ETag", "")).strip('"')}


def _pack_header(record: dict, sha256: str) -> bytes:
    """Encode a self-describing object frame header with a fixed-size hash."""
    if len(sha256) != 64:
        raise ValueError("pack record SHA-256 must be exactly 64 hexadecimal characters")
    return _canonical({"schema": MIGRATION_SCHEMA, "bucket": record["bucket"], "key": record["key"],
                       "object_id": record["object_id"], "size": int(record["size"]),
                       "etag": record["etag"], "sha256": sha256})


def pack_frame_size(record: dict) -> int:
    """Return the exact byte cost of a framed small-object pack member."""
    if record.get("object_id") != object_id(record.get("bucket", ""), record.get("key", "")):
        raise ValueError("pack record has invalid object identity")
    size = int(record.get("size", -1))
    if size < 0:
        raise ValueError("pack record has invalid size")
    return 4 + len(_pack_header(record, "0" * 64)) + size


def stream_r2_pack(records: Iterable[dict], max_bytes: int, output: BinaryIO | None = None) -> dict:
    """Stream a bounded self-describing pack of small immutable R2 objects.

    A pack avoids making millions of iCloud filesystem entries for tiny
    receipt/worker objects.  It contains canonical length-prefixed headers and
    object bytes; each header is bound to its source SHA-256 and the terminal
    event binds the complete pack.  The caller is still required to retain the
    inventory and a separate evicted pack manifest before deletion is allowed.
    """
    rows = list(records)
    if not rows or max_bytes < 1024 * 1024:
        raise ValueError("a pack needs records and at least 1 MiB capacity")
    expected = len(PACK_MAGIC) + sum(pack_frame_size(row) for row in rows)
    if expected > max_bytes:
        raise ValueError(f"pack exceeds bounded capacity: {expected} > {max_bytes}")
    client = _direct_s3_client("direct-s3://evacuate/pack")
    sink = output or sys.stdout.buffer
    digest = hashlib.sha256(); digest.update(PACK_MAGIC); sink.write(PACK_MAGIC)
    offset = len(PACK_MAGIC); emitted = []
    for index, row in enumerate(rows):
        head = client.head_object(Bucket=row["bucket"], Key=row["key"])
        if int(head["ContentLength"]) != int(row["size"]) or str(head.get("ETag", "")).strip('"') != row["etag"]:
            raise RuntimeError("source object metadata changed since inventory; pack refused")
        last_error: Exception | None = None
        for _ in range(3):
            try:
                data = _read_exact(client.get_object(Bucket=row["bucket"], Key=row["key"])["Body"], int(row["size"]))
                break
            except Exception as exc:
                last_error = exc
        else:
            raise RuntimeError(f"source pack record {index} could not be read") from last_error
        record_hash = hashlib.sha256(data).hexdigest(); header = _pack_header(row, record_hash)
        frame = len(header).to_bytes(4, "big") + header + data
        sink.write(frame); digest.update(frame)
        event = {"event": "PACK_RECORD", "index": index, "offset": offset, "frame_size": len(frame),
                 "object_id": row["object_id"], "size": int(row["size"]), "sha256": record_hash}
        emitted.append(event); offset += len(frame)
        print(STREAM_EVENT_PREFIX + json.dumps(event, sort_keys=True), file=sys.stderr, flush=True)
    terminal = {"event": "PACK", "record_count": len(rows), "pack_size": offset,
                "sha256": digest.hexdigest(), "records": emitted}
    print(STREAM_EVENT_PREFIX + json.dumps(terminal, sort_keys=True), file=sys.stderr, flush=True)
    return terminal


def stream_r2_object(bucket: str, key: str, chunk_bytes: int, output: BinaryIO | None = None, *, start_offset: int = 0) -> dict:
    """Stream an R2 object once, reporting chunk and full-object hashes on stderr.

    The raw byte channel is stdout only.  This makes the producer safe to use
    in a pipe to a different host while retaining a source-side integrity
    record.  The caller must treat nonzero exit status or a missing terminal
    event as a failed object and never delete its source.
    """
    if chunk_bytes < 1024 * 1024:
        raise ValueError("chunk_bytes must be at least 1 MiB")
    client = _direct_s3_client("direct-s3://evacuate/stream")
    head = client.head_object(Bucket=bucket, Key=key)
    declared_size = int(head["ContentLength"])
    if start_offset < 0 or start_offset > declared_size or start_offset % chunk_bytes:
        raise ValueError("start_offset must be a chunk-aligned position within the object")
    response = client.get_object(Bucket=bucket, Key=key, **({"Range": f"bytes={start_offset}-"} if start_offset else {}))
    expected_streamed = declared_size - start_offset
    body = response["Body"]
    sink = output or sys.stdout.buffer
    complete = hashlib.sha256()
    chunk = hashlib.sha256()
    index = start_offset // chunk_bytes; offset = start_offset; chunk_size = 0
    while True:
        block = body.read(min(8 * 1024 * 1024, chunk_bytes - chunk_size))
        if not block:
            break
        sink.write(block)
        complete.update(block); chunk.update(block)
        offset += len(block); chunk_size += len(block)
        if chunk_size == chunk_bytes:
            event = {"event": "CHUNK", "index": index, "offset": offset - chunk_size,
                     "size": chunk_size, "sha256": chunk.hexdigest()}
            print(STREAM_EVENT_PREFIX + json.dumps(event, sort_keys=True), file=sys.stderr, flush=True)
            index += 1; chunk_size = 0; chunk = hashlib.sha256()
    if chunk_size:
        event = {"event": "CHUNK", "index": index, "offset": offset - chunk_size,
                 "size": chunk_size, "sha256": chunk.hexdigest()}
        print(STREAM_EVENT_PREFIX + json.dumps(event, sort_keys=True), file=sys.stderr, flush=True)
        index += 1
    if offset - start_offset != expected_streamed:
        raise RuntimeError(f"source object changed or was truncated: declared {declared_size}, read through {offset}")
    terminal = {"event": "OBJECT", "bucket": bucket, "key": key, "object_id": object_id(bucket, key),
                "object_size": declared_size, "start_offset": start_offset, "streamed_size": offset - start_offset,
                "stream_sha256": complete.hexdigest(), "chunk_count": index,
                "etag": str(head.get("ETag", "")).strip('"')}
    print(STREAM_EVENT_PREFIX + json.dumps(terminal, sort_keys=True), file=sys.stderr, flush=True)
    return terminal


def delete_verified_r2_object(bucket: str, key: str, expected_size: int, expected_etag: str) -> dict:
    """Delete only an unchanged object identified by a completed inventory record."""
    client = _direct_s3_client("direct-s3://evacuate/delete")
    head = client.head_object(Bucket=bucket, Key=key)
    actual_size = int(head["ContentLength"])
    actual_etag = str(head.get("ETag", "")).strip('"')
    if actual_size != expected_size or actual_etag != expected_etag:
        raise RuntimeError("source object metadata changed since inventory; deletion refused")
    client.delete_object(Bucket=bucket, Key=key)
    return {"schema": MIGRATION_SCHEMA, "state": "DELETED", "bucket": bucket, "key": key,
            "object_id": object_id(bucket, key), "size": actual_size, "etag": actual_etag,
            "deleted_at": time.time()}
