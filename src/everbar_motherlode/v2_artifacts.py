"""Small, deterministic helpers for V2 derived sidecar artifacts.

The canonical Motherlode database is an input authority for these artifacts,
never an output target.  The helpers here deliberately keep paths out of
semantic hashes so the same sidecar can be rebuilt in another checkout.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import struct
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import quote


ARTIFACT_SCHEMA = "everbar-motherlode.v2-sidecar-artifacts/v1"


def open_canonical_read_only(path: str | Path) -> sqlite3.Connection:
    """Open an existing canonical SQLite database without write capability."""
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"canonical state database is unavailable: {source}")
    # mode=ro prevents SQLite from creating or journaling the input.  The
    # second guard also rejects accidental writes through this connection.
    uri = f"file:{quote(str(source.resolve()), safe='/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("pragma query_only=on")
    return conn


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def semantic_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256_bytes(payload)


def write_json(path: str | Path, value: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def write_jsonl(path: str | Path, rows: Iterable[Mapping[str, Any]]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
        for row in rows
    )
    target.write_text(payload, encoding="utf-8")
    return sha256_bytes(payload.encode("utf-8"))


def write_int64_npy(path: str | Path, values: Iterable[int]) -> str:
    """Write a dependency-free, deterministic one-dimensional int64 NPY."""
    data = list(values)
    header = "{'descr': '<i8', 'fortran_order': False, 'shape': (%d,), }" % len(data)
    # NPY v1 requires the header, including its newline, to end on a 64-byte
    # boundary from the beginning of the file.
    prefix = 10
    padding = 64 - ((prefix + len(header) + 1) % 64)
    header_bytes = (header + " " * padding + "\n").encode("ascii")
    payload = b"\x93NUMPY\x01\x00" + struct.pack("<H", len(header_bytes)) + header_bytes
    payload += struct.pack("<%dq" % len(data), *[int(value) for value in data]) if data else b""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return sha256_bytes(payload)


def write_artifact_manifest(
    output_dir: str | Path,
    *,
    schema: str,
    files: Mapping[str, str | Path],
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Write a content-addressed manifest for already-written sidecar files."""
    root = Path(output_dir)
    entries: dict[str, dict[str, Any]] = {}
    for logical_name, raw_path in sorted(files.items()):
        path = Path(raw_path)
        if not path.is_file():
            raise FileNotFoundError(f"artifact is missing: {path}")
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError(f"artifact must be below output directory: {path}") from exc
        entries[logical_name] = {
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    manifest: dict[str, Any] = {"schema": schema, "files": entries}
    if metadata:
        manifest.update(dict(metadata))
    manifest["manifest_sha256"] = semantic_hash(manifest)
    write_json(root / "manifest.json", manifest)
    return manifest


def canonical_identity(path: str | Path) -> dict[str, Any]:
    """Return a path-independent identity for a canonical SQLite input."""
    source = Path(path)
    return {"schema": "sqlite-file/v1", "bytes": source.stat().st_size, "sha256": sha256_file(source)}
