import hashlib
import io
import json
import gzip
from pathlib import Path

import pytest

from everbar_motherlode import icloud_migration as migration
from scripts.migrate_r2_to_icloud import iter_small_object_packs


def test_object_id_is_stable_and_key_safe():
    assert migration.object_id("bucket", "a/path/file.mid") == migration.object_id("bucket", "a/path/file.mid")
    assert len(migration.object_id("bucket", "../../odd/key")) == 64
    with pytest.raises(ValueError):
        migration.object_id("", "key")


def test_inventory_is_paginated_deterministic_and_hashed(tmp_path: Path, monkeypatch):
    class Pager:
        def paginate(self, Bucket):
            assert Bucket == "b"
            return [{"Contents": [{"Key": "z", "Size": 2, "ETag": '"etag-z"'}, {"Key": "a", "Size": 3, "ETag": '"etag-a"'}]}]
    class Client:
        def get_paginator(self, name): assert name == "list_objects_v2"; return Pager()
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    target = tmp_path / "inventory.jsonl"
    summary = migration.inventory_r2(target, ["b"])
    assert summary["object_count"] == 2 and summary["total_bytes"] == 5
    rows = list(migration.iter_inventory(target))
    assert [row["key"] for row in rows] == ["z", "a"]
    assert summary["inventory_sha256"] == hashlib.sha256(target.read_bytes()).hexdigest()


def test_inventory_resumes_after_last_complete_line_without_duplicate(tmp_path: Path, monkeypatch):
    target = tmp_path / "inventory.jsonl"; partial = target.with_suffix(".jsonl.partial")
    first = {"schema": migration.MIGRATION_SCHEMA, "bucket": "b", "key": "a", "object_id": migration.object_id("b", "a"), "size": 2, "etag": "a", "last_modified": None}
    partial.write_text(json.dumps(first, sort_keys=True) + "\n{incomplete")
    class Pager:
        def paginate(self, **kwargs):
            assert kwargs == {"Bucket": "b", "StartAfter": "a"}
            return [{"Contents": [{"Key": "b", "Size": 3, "ETag": '"b"'}]}]
    class Client:
        def get_paginator(self, name): return Pager()
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    summary = migration.inventory_r2(target, ["b"], resume=True)
    assert summary["object_count"] == 2
    assert [row["key"] for row in migration.iter_inventory(target)] == ["a", "b"]


def test_gzip_inventory_resumes_from_complete_page_members(tmp_path: Path, monkeypatch):
    target = tmp_path / "inventory.jsonl.gz"; partial = target.with_suffix(".gz.partial")
    first = {"schema": migration.MIGRATION_SCHEMA, "bucket": "b", "key": "a", "object_id": migration.object_id("b", "a"), "size": 2, "etag": "a", "last_modified": None}
    with partial.open("ab") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb") as member:
            member.write(json.dumps(first, sort_keys=True).encode() + b"\n")
    class Pager:
        def paginate(self, **kwargs):
            assert kwargs == {"Bucket": "b", "StartAfter": "a"}
            return [{"Contents": [{"Key": "b", "Size": 3, "ETag": '"b"'}]}]
    class Client:
        def get_paginator(self, name): return Pager()
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    summary = migration.inventory_r2(target, ["b"], resume=True)
    assert summary["object_count"] == 2
    assert [row["key"] for row in migration.iter_inventory(target)] == ["a", "b"]


def test_gzip_inventory_discards_only_a_corrupt_final_member(tmp_path: Path):
    partial = tmp_path / "inventory.jsonl.gz.partial"
    first = {"schema": migration.MIGRATION_SCHEMA, "bucket": "b", "key": "a", "object_id": migration.object_id("b", "a"), "size": 2, "etag": "a", "last_modified": None}
    with partial.open("ab") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb") as member:
            member.write(json.dumps(first, sort_keys=True).encode() + b"\n")
        raw.write(b"not-a-complete-gzip-member")
    _, counts, cursors = migration._read_partial_inventory(partial)
    assert counts == {"b": {"objects": 1, "bytes": 2}}
    assert cursors == {"b": "a"}


def test_stream_reports_chunk_and_total_hashes_without_mutating_payload(monkeypatch, capsys):
    payload = b"abcde" * 700_000
    class Body:
        def __init__(self): self.pos = 0
        def read(self, count):
            result = payload[self.pos:self.pos + count]; self.pos += len(result); return result
    class Client:
        def head_object(self, **kwargs): return {"ContentLength": len(payload), "ETag": '"source-etag"'}
        def get_object(self, **kwargs): return {"Body": Body()}
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    sink = io.BytesIO()
    terminal = migration.stream_r2_object("bucket", "key", 1024 * 1024, sink)
    assert sink.getvalue() == payload
    assert terminal["stream_sha256"] == hashlib.sha256(payload).hexdigest()
    events = [json.loads(line[len(migration.STREAM_EVENT_PREFIX):]) for line in capsys.readouterr().err.splitlines()]
    assert [event["event"] for event in events] == ["CHUNK", "CHUNK", "CHUNK", "CHUNK", "OBJECT"]
    assert sum(event["size"] for event in events if event["event"] == "CHUNK") == len(payload)


def test_stream_can_resume_at_a_chunk_boundary(monkeypatch, capsys):
    payload = b"abcde" * 700_000
    class Body:
        def __init__(self, body): self.body = body; self.pos = 0
        def read(self, count):
            result = self.body[self.pos:self.pos + count]; self.pos += len(result); return result
    class Client:
        def head_object(self, **kwargs): return {"ContentLength": len(payload), "ETag": '"source-etag"'}
        def get_object(self, **kwargs):
            start = int(kwargs["Range"].split("=")[1].split("-")[0]) if "Range" in kwargs else 0
            return {"Body": Body(payload[start:])}
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    sink = io.BytesIO(); size = 1024 * 1024
    terminal = migration.stream_r2_object("bucket", "key", size, sink, start_offset=size)
    assert sink.getvalue() == payload[size:]
    assert terminal["start_offset"] == size and terminal["object_size"] == len(payload)


def test_small_object_pack_is_bounded_framed_and_hash_bound(monkeypatch, capsys):
    payloads = {"one": b"a" * 600_000, "two": b"b" * 600_000}
    rows = [{"schema": migration.MIGRATION_SCHEMA, "bucket": "b", "key": key,
             "object_id": migration.object_id("b", key), "size": len(value), "etag": key,
             "last_modified": None} for key, value in payloads.items()]
    class Body:
        def __init__(self, data): self.data = data; self.pos = 0
        def read(self, count):
            result = self.data[self.pos:self.pos + count]; self.pos += len(result); return result
    class Client:
        def head_object(self, Bucket, Key): return {"ContentLength": len(payloads[Key]), "ETag": f'"{Key}"'}
        def get_object(self, Bucket, Key): return {"Body": Body(payloads[Key])}
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    expected = len(migration.PACK_MAGIC) + sum(migration.pack_frame_size(row) for row in rows)
    sink = io.BytesIO(); terminal = migration.stream_r2_pack(rows, expected, sink)
    assert terminal["pack_size"] == expected and len(sink.getvalue()) == expected
    assert terminal["sha256"] == hashlib.sha256(sink.getvalue()).hexdigest()
    raw = sink.getvalue(); assert raw.startswith(migration.PACK_MAGIC)
    offset = len(migration.PACK_MAGIC)
    for row in rows:
        length = int.from_bytes(raw[offset:offset + 4], "big"); offset += 4
        header = json.loads(raw[offset:offset + length]); offset += length
        assert header["object_id"] == row["object_id"]
        assert raw[offset:offset + row["size"]] == payloads[row["key"]]
        offset += row["size"]
    with pytest.raises(ValueError, match="exceeds"):
        migration.stream_r2_pack(rows, expected - 1, io.BytesIO())


def test_small_object_pack_planner_preserves_inventory_order_and_bounds(tmp_path: Path, monkeypatch):
    records = []
    for index, size in enumerate((400_000, 400_000, 3_000_000, 400_000)):
        key = f"k{index}"
        records.append({"schema": migration.MIGRATION_SCHEMA, "bucket": "b", "key": key,
                        "object_id": migration.object_id("b", key), "size": size, "etag": key, "last_modified": None})
    inventory = tmp_path / "inventory.jsonl"
    inventory.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in records))
    packs = list(iter_small_object_packs(inventory, pack_bytes=1024 * 1024, small_object_bytes=1024 * 1024))
    assert [[row["key"] for row in group] for group in packs] == [["k0", "k1"], ["k3"]]
    assert all(len(migration.PACK_MAGIC) + sum(migration.pack_frame_size(row) for row in group) <= 1024 * 1024 for group in packs)


def test_delete_refuses_source_metadata_change(monkeypatch):
    class Client:
        def head_object(self, **kwargs): return {"ContentLength": 2, "ETag": '"changed"'}
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    with pytest.raises(RuntimeError, match="changed"):
        migration.delete_verified_r2_object("bucket", "key", 2, "expected")
