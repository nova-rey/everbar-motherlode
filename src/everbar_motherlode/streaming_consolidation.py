"""Bounded, resumable consolidation of immutable distributed shard packages.

This is deliberately *not* ``core.merge_shards``.  The historical merge makes
one central SQLite database and therefore needs room for every shard's receipt
JSON.  A streaming consolidation opens exactly one immutable shard database at
a time, projects only its accepted Brick-3 receipts into a compact canonical
partition, uploads that partition, and then removes its temporary copies.

The source packages are never modified.  Raw MIDI is never opened and Brick 3
is never invoked here.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .core import sha, writej
from .distributed import _direct_s3_exists, _direct_s3_key, _direct_s3_read, _direct_s3_write
from .feature_base import ensure_feature_schema, materialize_canonical_stream


POLICY_ID = "streaming-canonical-consolidation-v1"


class PackageVerificationError(RuntimeError):
    """A source package is incomplete, inconsistent, or unsafe to consume."""


def _json_hash(value: Any) -> str:
    return sha(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _uri_child(root: str, *parts: str) -> str:
    return root.rstrip("/") + "/" + "/".join(part.strip("/") for part in parts)


def _read_bytes(uri: str) -> bytes:
    if uri.startswith("direct-s3://"):
        client, bucket, key = _direct_s3_key(uri)
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if uri.startswith("file://"):
        return Path(urlparse(uri).path).read_bytes()
    return subprocess.run(["rclone", "cat", uri], check=True, capture_output=True).stdout


def _read_json(uri: str) -> dict[str, Any]:
    return json.loads(_read_bytes(uri))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_sha256(uri: str) -> str:
    """Hash a remote object in bounded memory; package databases are huge."""
    digest = hashlib.sha256()
    if uri.startswith("direct-s3://"):
        client, bucket, key = _direct_s3_key(uri)
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        while chunk := body.read(8 * 1024 * 1024):
            digest.update(chunk)
        return digest.hexdigest()
    if uri.startswith("file://"):
        return _file_sha256(Path(urlparse(uri).path))
    process = subprocess.Popen(["rclone", "cat", uri], stdout=subprocess.PIPE)
    assert process.stdout is not None
    while chunk := process.stdout.read(8 * 1024 * 1024):
        digest.update(chunk)
    if process.wait() != 0:
        raise RuntimeError(f"rclone could not read uploaded artifact: {uri}")
    return digest.hexdigest()


def _exists(uri: str) -> bool:
    if uri.startswith("direct-s3://"):
        return _direct_s3_exists(uri)
    if uri.startswith("file://"):
        return Path(urlparse(uri).path).is_file()
    probe = subprocess.run(["rclone", "lsf", uri], capture_output=True, text=True)
    return probe.returncode == 0 and bool(probe.stdout.strip())


def _copy_from(uri: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".downloading")
    temporary.unlink(missing_ok=True)
    if uri.startswith("direct-s3://"):
        client, bucket, key = _direct_s3_key(uri)
        client.download_file(bucket, key, str(temporary))
    elif uri.startswith("file://"):
        shutil.copy2(Path(urlparse(uri).path), temporary)
    else:
        subprocess.run(["rclone", "copyto", uri, str(temporary)], check=True)
    temporary.replace(destination)


def _copy_to(source: Path, uri: str) -> None:
    if uri.startswith("direct-s3://"):
        _direct_s3_write(uri, source)
    elif uri.startswith("file://"):
        destination = Path(urlparse(uri).path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".uploading")
        shutil.copy2(source, temporary)
        temporary.replace(destination)
    else:
        subprocess.run(["rclone", "copyto", str(source), uri], check=True)


def _source_prefix(source_uri: str, run_id: str, dataset_id: str, shard_index: int, shard_count: int) -> str:
    return _uri_child(source_uri, "runs", run_id, dataset_id, f"shard-{shard_index:05d}-of-{shard_count:05d}")


def _output_prefix(output_uri: str, consolidation_id: str, dataset_id: str, shard_index: int, shard_count: int) -> str:
    return _uri_child(output_uri, "consolidations", consolidation_id, dataset_id, f"shard-{shard_index:05d}-of-{shard_count:05d}")


def _validate_source_package(source_uri: str, run_id: str, dataset_id: str, shard_index: int, shard_count: int) -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    """Validate the durable marker and immutable item ledger before download."""
    prefix = _source_prefix(source_uri, run_id, dataset_id, shard_index, shard_count)
    completion = _read_json(_uri_child(prefix, "completion.json"))
    manifest = _read_json(_uri_child(prefix, "manifest.json"))
    ids_body = _read_json(_uri_child(prefix, "item-ids.json"))
    receipt = _read_json(_uri_child(prefix, "shard-receipt.json"))
    ids = ids_body.get("item_ids")
    if not isinstance(ids, list) or any(not isinstance(item, str) for item in ids):
        raise PackageVerificationError(f"shard {shard_index}: item ledger is malformed")
    for body_name, body in (("completion", completion), ("manifest", manifest)):
        if body.get("state") != "COMPLETE" or body.get("run_id") != run_id or body.get("dataset_id") != dataset_id:
            raise PackageVerificationError(f"shard {shard_index}: invalid {body_name} identity")
        if body.get("shard_index") != shard_index or body.get("shard_count") != shard_count:
            raise PackageVerificationError(f"shard {shard_index}: invalid {body_name} partition identity")
        if body.get("item_count") != len(ids) or body.get("item_ids_sha256") != sha("\n".join(ids)):
            raise PackageVerificationError(f"shard {shard_index}: {body_name} does not bind its item ledger")
    if completion != manifest:
        raise PackageVerificationError(f"shard {shard_index}: completion marker and manifest differ")
    if receipt.get("state") != "COMPLETE" or receipt.get("dataset_id") != dataset_id:
        raise PackageVerificationError(f"shard {shard_index}: worker receipt is not complete")
    return completion, ids, receipt


def _verify_sqlite(path: Path, expected_ids: list[str], shard_index: int) -> None:
    source = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if source.execute("pragma integrity_check").fetchone()[0] != "ok":
            raise PackageVerificationError(f"shard {shard_index}: SQLite integrity check failed")
        actual = [row[0] for row in source.execute("select id from items order by id")]
    finally:
        source.close()
    if actual != sorted(expected_ids):
        raise PackageVerificationError(f"shard {shard_index}: SQLite item IDs do not match immutable package ledger")


def _index_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    create table if not exists canonical_hashes(
      canonical_score_sha256 text primary key,
      kept_stream_id text not null,
      kept_shard_id text not null,
      occurrence_count integer not null
    );
    create table if not exists streamed_shards(
      shard_id text primary key,
      source_completion_sha256 text not null,
      source_shard_db_sha256 text not null,
      compact_sha256 text,
      state text not null,
      updated_at real not null
    );
    """)


def _compact_schema(conn: sqlite3.Connection) -> None:
    ensure_feature_schema(conn)
    conn.executescript("""
    create table canonical_dedupe(
      stream_id text primary key,
      canonical_score_sha256 text not null,
      status text not null check(status in ('UNIQUE','DUPLICATE')),
      kept_stream_id text not null,
      kept_shard_id text not null
    );
    create table stream_provenance(
      stream_id text primary key,
      dataset_id text not null,
      canonical_score_sha256 text not null,
      brick3_receipt_sha256 text not null,
      source_piece_id text,
      source_track_id text,
      sibling_track_ids_json text not null,
      programs_json text not null,
      is_drum integer not null,
      source_track_name text,
      source_native_role text,
      source_timing_json text not null,
      canonical_status text not null check(canonical_status in ('UNIQUE','DUPLICATE')),
      kept_stream_id text not null,
      kept_shard_id text not null
    );
    """)


def _copy_source_provenance(source: sqlite3.Connection, compact: sqlite3.Connection) -> None:
    for table, columns in (
        ("source_pieces", "source_piece_id,dataset_id,dataset_version,source_artifact_id,source_relative_path,source_raw_sha256,source_timing_json,detail_json"),
        ("source_tracks", "source_track_id,source_piece_id,track_index,source_track_name,programs_json,channels_json,is_drum,has_notes,source_native_role,timing_json"),
    ):
        rows = source.execute(f"select {columns} from {table}").fetchall()
        if rows:
            placeholders = ",".join("?" for _ in columns.split(","))
            compact.executemany(f"insert or replace into {table} values({placeholders})", rows)


def _project_partition(source_db: Path, compact_db: Path, index: sqlite3.Connection, dataset_id: str, shard_id: str) -> dict[str, int]:
    """Materialize receipt-backed canonical rows, retaining one score per hash."""
    source = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    compact = sqlite3.connect(compact_db)
    try:
        _compact_schema(compact)
        _copy_source_provenance(source, compact)
        accepted = unique = duplicates = malformed = 0
        for stream_id, item_dataset, detail_json in source.execute("select id,dataset_id,detail from items where state='BRICK3_COMPLETE' order by id"):
            try:
                detail = json.loads(detail_json)
                if detail.get("brick3") != "ACCEPT":
                    continue
                receipt = detail.get("receipt") or {}
                canonical = receipt.get("canonical") or {}
                canonical_hash = canonical.get("event_sha256")
                if not canonical_hash:
                    raise ValueError("accepted receipt has no canonical event hash")
                accepted += 1
                previous = index.execute("select kept_stream_id,kept_shard_id from canonical_hashes where canonical_score_sha256=?", (canonical_hash,)).fetchone()
                if previous is None:
                    kept_stream, kept_shard, status = stream_id, shard_id, "UNIQUE"
                    index.execute("insert into canonical_hashes values(?,?,?,?)", (canonical_hash, kept_stream, kept_shard, 1))
                    unique += 1
                else:
                    kept_stream, kept_shard, status = previous[0], previous[1], "DUPLICATE"
                    index.execute("update canonical_hashes set occurrence_count=occurrence_count+1 where canonical_score_sha256=?", (canonical_hash,))
                    duplicates += 1
                provenance = detail.get("provenance") or {}
                compact.execute("insert into stream_provenance values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    stream_id, item_dataset, canonical_hash, receipt.get("receipt_sha256", ""),
                    provenance.get("source_piece_id"), provenance.get("source_track_id"),
                    json.dumps(provenance.get("sibling_track_ids", []), sort_keys=True),
                    json.dumps(provenance.get("programs", []), sort_keys=True), int(bool(provenance.get("is_drum", False))),
                    provenance.get("source_track_name"), provenance.get("source_native_role"),
                    json.dumps(provenance.get("source_timing", {}), sort_keys=True), status, kept_stream, kept_shard,
                ))
                compact.execute("insert into canonical_dedupe values(?,?,?,?,?)", (stream_id, canonical_hash, status, kept_stream, kept_shard))
                if status == "UNIQUE":
                    if not materialize_canonical_stream(compact, stream_id=stream_id, dataset_id=dataset_id, detail=detail):
                        raise ValueError("accepted receipt cannot materialize canonical stream")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                # An accepted receipt that cannot project would make an apparently
                # complete partition lossy, so fail closed rather than skip it.
                raise PackageVerificationError(f"{shard_id}: invalid accepted Brick-3 receipt for {stream_id}: {exc}") from exc
        compact.commit()
        return {"accepted_streams": accepted, "unique_canonical_streams": unique, "canonical_duplicates": duplicates, "malformed": malformed}
    finally:
        compact.close()
        source.close()


def _download_index_if_needed(index_path: Path, output_uri: str, consolidation_id: str) -> None:
    if index_path.exists():
        return
    remote = _uri_child(output_uri, "consolidations", consolidation_id, "canonical-index.sqlite")
    if _exists(remote):
        _copy_from(remote, index_path)


def _upload_and_verify(source: Path, output_uri: str, *parts: str, expected_sha256: str | None = None) -> None:
    destination = _uri_child(output_uri, *parts)
    _copy_to(source, destination)
    if expected_sha256 is not None and _remote_sha256(destination) != expected_sha256:
        raise PackageVerificationError(f"uploaded artifact hash mismatch: {destination}")


def _existing_completion(output_uri: str, consolidation_id: str, dataset_id: str, shard_index: int, shard_count: int) -> dict[str, Any] | None:
    uri = _uri_child(_output_prefix(output_uri, consolidation_id, dataset_id, shard_index, shard_count), "completion.json")
    return _read_json(uri) if _exists(uri) else None


def stream_consolidate(*, workspace: Path, source_uri: str, output_uri: str, run_id: str, dataset_id: str, shard_count: int, consolidation_id: str) -> dict[str, Any]:
    """Consolidate an exact completed run in deterministic shard-index order.

    The output consists of immutable compact canonical partitions and a small
    durable canonical-hash index.  It intentionally refuses a partial run:
    deterministic cross-shard dedupe needs all expected packages available.
    """
    if shard_count < 1:
        raise ValueError("shard_count must be positive")
    root = workspace / "streaming-consolidations" / consolidation_id
    work = root / "work"
    index_path = root / "canonical-index.sqlite"
    root.mkdir(parents=True, exist_ok=True)
    _download_index_if_needed(index_path, output_uri, consolidation_id)
    index = sqlite3.connect(index_path)
    try:
        _index_schema(index)
        # Do not create a half-consolidated view merely because later source
        # packages have not arrived yet.  The R2 recovery staging step is the
        # only place where incomplete run membership is allowed.
        packages = [
            _validate_source_package(source_uri, run_id, dataset_id, shard_index, shard_count)
            for shard_index in range(shard_count)
        ]
        outcomes = []
        for shard_index in range(shard_count):
            shard_id = f"{dataset_id}-part-{shard_index:05d}-of-{shard_count:05d}"
            completion, ids, worker_receipt = packages[shard_index]
            completion_hash = _json_hash(completion)
            previous = _existing_completion(output_uri, consolidation_id, dataset_id, shard_index, shard_count)
            if previous is not None:
                if previous.get("source_completion_sha256") != completion_hash:
                    raise PackageVerificationError(f"{shard_id}: existing compact output binds a different source package")
                state = index.execute("select state from streamed_shards where shard_id=?", (shard_id,)).fetchone()
                if state is None:
                    raise PackageVerificationError(f"{shard_id}: compact completion exists but durable index state is absent")
                outcomes.append({"shard_index": shard_index, "state": "SKIPPED_COMPLETE"})
                continue
            stage = work / shard_id
            if stage.exists():
                shutil.rmtree(stage)
            stage.mkdir(parents=True)
            source_db = stage / "shard.sqlite"
            compact_db = stage / "canonical.sqlite"
            try:
                _copy_from(_uri_child(_source_prefix(source_uri, run_id, dataset_id, shard_index, shard_count), "shard.sqlite"), source_db)
                _verify_sqlite(source_db, ids, shard_index)
                source_hash = _file_sha256(source_db)
                summary = _project_partition(source_db, compact_db, index, dataset_id, shard_id)
                compact_hash = _file_sha256(compact_db)
                manifest = {
                    "state": "COMPLETE", "policy_id": POLICY_ID, "consolidation_id": consolidation_id,
                    "dataset_id": dataset_id, "run_id": run_id, "shard_index": shard_index, "shard_count": shard_count,
                    "source_completion_sha256": completion_hash, "source_shard_db_sha256": source_hash,
                    "source_worker_finished_at": worker_receipt.get("finished_at"), "canonical_sqlite_sha256": compact_hash,
                    "source_item_count": len(ids), "summary": summary, "used_raw_midi": False, "used_brick3": False,
                    "created_at": time.time(),
                }
                manifest_path = stage / "manifest.json"
                writej(manifest_path, manifest)
                index.execute("insert into streamed_shards values(?,?,?,?,?,?) on conflict(shard_id) do update set source_completion_sha256=excluded.source_completion_sha256,source_shard_db_sha256=excluded.source_shard_db_sha256,compact_sha256=excluded.compact_sha256,state=excluded.state,updated_at=excluded.updated_at", (shard_id, completion_hash, source_hash, compact_hash, "PREPARED", time.time()))
                index.commit()
                prefix = _output_prefix(output_uri, consolidation_id, dataset_id, shard_index, shard_count)
                _upload_and_verify(compact_db, output_uri, "consolidations", consolidation_id, dataset_id, f"shard-{shard_index:05d}-of-{shard_count:05d}", "canonical.sqlite", expected_sha256=compact_hash)
                _upload_and_verify(manifest_path, output_uri, "consolidations", consolidation_id, dataset_id, f"shard-{shard_index:05d}-of-{shard_count:05d}", "manifest.json", expected_sha256=sha(manifest_path.read_bytes()))
                _upload_and_verify(index_path, output_uri, "consolidations", consolidation_id, "canonical-index.sqlite", expected_sha256=_file_sha256(index_path))
                completion_path = stage / "completion.json"
                writej(completion_path, manifest)
                _upload_and_verify(completion_path, output_uri, "consolidations", consolidation_id, dataset_id, f"shard-{shard_index:05d}-of-{shard_count:05d}", "completion.json", expected_sha256=sha(completion_path.read_bytes()))
                index.execute("update streamed_shards set state='COMPLETE',updated_at=? where shard_id=?", (time.time(), shard_id))
                index.commit()
                _upload_and_verify(index_path, output_uri, "consolidations", consolidation_id, "canonical-index.sqlite", expected_sha256=_file_sha256(index_path))
                outcomes.append({"shard_index": shard_index, "state": "COMPLETE", **summary})
            finally:
                # Keep a failed stage for diagnosis/resume.  On success the source
                # DB and compact database disappear only after remote hash checks.
                if outcomes and outcomes[-1].get("shard_index") == shard_index and outcomes[-1]["state"] == "COMPLETE":
                    shutil.rmtree(stage)
        report = {
            "state": "COMPLETE", "policy_id": POLICY_ID, "consolidation_id": consolidation_id,
            "run_id": run_id, "dataset_id": dataset_id, "shard_count": shard_count,
            "outcomes": outcomes, "canonical_index_sha256": _file_sha256(index_path),
            "used_raw_midi": False, "used_brick3": False, "finished_at": time.time(),
        }
        receipt_path = root / "completion.json"
        writej(receipt_path, report)
        _upload_and_verify(receipt_path, output_uri, "consolidations", consolidation_id, "completion.json", expected_sha256=sha(receipt_path.read_bytes()))
        return report
    finally:
        index.close()
