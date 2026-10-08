import io
import json
from pathlib import Path

from everbar_motherlode.icloud_migration import object_id
from everbar_motherlode import nas_archive
from everbar_motherlode.nas_archive import NAS_ARCHIVE_SCHEMA, _write_json_atomic, delete_verified_inventory, iter_small_packs, selected_records, verify_coverage


def _record(index: int, size: int) -> dict:
    key = f"key-{index}"
    return {
        "schema": "everbar-motherlode.r2-icloud-migration/v1",
        "bucket": "b",
        "key": key,
        "object_id": object_id("b", key),
        "size": size,
        "etag": str(index),
        "last_modified": None,
    }


def test_selected_records_is_disjoint_and_complete(tmp_path: Path):
    rows = [_record(index, 10) for index in range(40)]
    inventory = tmp_path / "inventory.jsonl"
    inventory.write_text("".join(json.dumps(row) + "\n" for row in rows))
    partitions = [list(selected_records(inventory, index, 4)) for index in range(4)]
    ids = [row["object_id"] for partition in partitions for row in partition]
    assert len(ids) == len(set(ids)) == len(rows)


def test_small_pack_planning_keeps_bounds_and_skips_large():
    rows = [_record(0, 400_000), _record(1, 400_000), _record(2, 3_000_000), _record(3, 400_000)]
    packs = list(iter_small_packs(rows, pack_bytes=1024 * 1024, small_object_bytes=1024 * 1024))
    packed = [row["key"] for pack in packs for row in pack]
    assert packed == ["key-0", "key-1", "key-3"]
    assert NAS_ARCHIVE_SCHEMA.endswith("/v1")


def test_coverage_requires_exact_inventory_receipt(tmp_path: Path):
    row = _record(0, 3)
    inventory = tmp_path / "inventory.jsonl"
    inventory.write_text(json.dumps(row, sort_keys=True) + "\n")
    archive = tmp_path / "archive"
    pack = archive / "r2" / "packs" / "p"
    pack.mkdir(parents=True)
    (pack / "pack.bin").write_bytes(b"abc")
    import hashlib
    _write_json_atomic(pack / "receipt.json", {
        "schema": NAS_ARCHIVE_SCHEMA, "state": "COMPLETE", "kind": "pack",
        "bytes": 3, "sha256": hashlib.sha256(b"abc").hexdigest(), "records": [row],
    })
    verified = verify_coverage(inventory=inventory, archive_root=archive, verify_payload_hashes=True)
    assert verified["state"] == "COMPLETE"
    (pack / "receipt.json").unlink()
    missing = verify_coverage(inventory=inventory, archive_root=archive)
    assert missing["state"] == "INCOMPLETE" and missing["missing_objects"] == 1


def test_parallel_pack_preserves_record_order_and_hashes(monkeypatch):
    payloads = {"key-0": b"a" * 5, "key-1": b"b" * 7, "key-2": b"c" * 11}
    rows = [_record(index, len(payloads[f"key-{index}"])) for index in range(3)]
    class Body:
        def __init__(self, data): self.data = data; self.position = 0
        def read(self, amount):
            result = self.data[self.position:self.position + amount]; self.position += len(result); return result
    class Client:
        def head_object(self, Bucket, Key): return {"ContentLength": len(payloads[Key]), "ETag": f'"{Key[-1]}"'}
        def get_object(self, Bucket, Key): return {"Body": Body(payloads[Key])}
    monkeypatch.setattr(nas_archive, "_direct_s3_client", lambda _: Client())
    # Match the fake ETags to the inventory metadata.
    for row in rows: row["etag"] = row["key"][-1]
    sink = io.BytesIO()
    result = nas_archive.stream_r2_pack_parallel(rows, 1024 * 1024, sink, fetch_workers=2)
    assert result["pack_size"] == len(sink.getvalue())
    assert [event["object_id"] for event in result["records"]] == [row["object_id"] for row in rows]


def test_verified_delete_refuses_unbound_coverage_and_receipts_batches(tmp_path: Path, monkeypatch):
    row = _record(0, 3)
    inventory = tmp_path / "inventory.jsonl"
    inventory.write_text(json.dumps(row, sort_keys=True) + "\n")
    archive = tmp_path / "archive"
    pack = archive / "r2" / "packs" / "p"
    pack.mkdir(parents=True)
    (pack / "pack.bin").write_bytes(b"abc")
    import hashlib
    _write_json_atomic(pack / "receipt.json", {
        "schema": NAS_ARCHIVE_SCHEMA, "state": "COMPLETE", "kind": "pack",
        "bytes": 3, "sha256": hashlib.sha256(b"abc").hexdigest(), "records": [row],
    })
    coverage = verify_coverage(inventory=inventory, archive_root=archive, verify_payload_hashes=True)
    coverage_path = archive / "receipts" / f"coverage-{inventory.name}.json"
    calls = []
    class Client:
        def delete_objects(self, **kwargs): calls.append(kwargs); return {"Deleted": kwargs["Delete"]["Objects"]}
        def list_objects_v2(self, **kwargs): return {"KeyCount": 0}
    monkeypatch.setattr(nas_archive, "_direct_s3_client", lambda _: Client())
    result = delete_verified_inventory(inventory=inventory, archive_root=archive, coverage_receipt=coverage_path)
    assert result["state"] == "COMPLETE" and len(calls) == 1
    assert (archive / "receipts" / "r2-deletion" / "b" / "batch-00000000.json").exists()
    assert coverage["state"] == "COMPLETE"
