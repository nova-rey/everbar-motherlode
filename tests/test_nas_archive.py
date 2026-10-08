import json
from pathlib import Path

from everbar_motherlode.icloud_migration import object_id
from everbar_motherlode.nas_archive import NAS_ARCHIVE_SCHEMA, _write_json_atomic, iter_small_packs, selected_records, verify_coverage


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
