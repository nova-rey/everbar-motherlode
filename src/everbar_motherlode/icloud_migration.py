"""Receipt-backed R2-to-iCloud evacuation primitives.

This module deliberately keeps object-store credentials at the source host.  A
source stream writes object bytes to stdout and emits only hashes/lengths to
stderr; the coordinator can therefore relay bounded chunks directly to an
iCloud Drive host without retaining corpus payloads locally.
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path
from typing import BinaryIO, Iterable

from .distributed import _direct_s3_client


STREAM_EVENT_PREFIX = "ICLOUD_R2_STREAM "
MIGRATION_SCHEMA = "everbar-motherlode.r2-icloud-migration/v1"


def object_id(bucket: str, key: str) -> str:
    """Return a filesystem-safe, stable identity without exposing a key in paths."""
    if not bucket or not key:
        raise ValueError("bucket and key are required")
    return hashlib.sha256((bucket + "\0" + key).encode("utf-8")).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def inventory_r2(output: Path, buckets: Iterable[str] | None = None) -> dict:
    """Write a deterministic, paginated object inventory without loading it all.

    This runs only on the credential-holding source host.  Inventory records
    contain provider metadata, never access credentials.  The atomic summary
    is a prerequisite for any migration or deletion operation.
    """
    client = _direct_s3_client("direct-s3://evacuate/inventory")
    selected = sorted(set(buckets or (row["Name"] for row in client.list_buckets().get("Buckets", []))))
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".partial")
    digest = hashlib.sha256()
    counts: dict[str, dict[str, int]] = {}
    with temporary.open("wb") as handle:
        for bucket in selected:
            count = total = 0
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
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
                    handle.write(encoded); digest.update(encoded)
                    count += 1; total += record["size"]
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
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            record = json.loads(line)
            if record.get("schema") != MIGRATION_SCHEMA:
                raise ValueError(f"inventory line {number} has unexpected schema")
            if record.get("object_id") != object_id(record.get("bucket", ""), record.get("key", "")):
                raise ValueError(f"inventory line {number} has invalid object identity")
            if int(record.get("size", -1)) < 0:
                raise ValueError(f"inventory line {number} has invalid size")
            yield record


def stream_r2_object(bucket: str, key: str, chunk_bytes: int, output: BinaryIO | None = None) -> dict:
    """Stream an R2 object once, reporting chunk and full-object hashes on stderr.

    The raw byte channel is stdout only.  This makes the producer safe to use
    in a pipe to a different host while retaining a source-side integrity
    record.  The caller must treat nonzero exit status or a missing terminal
    event as a failed object and never delete its source.
    """
    if chunk_bytes < 1024 * 1024:
        raise ValueError("chunk_bytes must be at least 1 MiB")
    client = _direct_s3_client("direct-s3://evacuate/stream")
    response = client.get_object(Bucket=bucket, Key=key)
    declared_size = int(response.get("ContentLength", -1))
    body = response["Body"]
    sink = output or sys.stdout.buffer
    complete = hashlib.sha256()
    chunk = hashlib.sha256()
    index = offset = chunk_size = 0
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
    if offset != declared_size:
        raise RuntimeError(f"source object changed or was truncated: declared {declared_size}, read {offset}")
    terminal = {"event": "OBJECT", "bucket": bucket, "key": key, "object_id": object_id(bucket, key),
                "size": offset, "sha256": complete.hexdigest(), "chunk_count": index,
                "etag": str(response.get("ETag", "")).strip('"')}
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
