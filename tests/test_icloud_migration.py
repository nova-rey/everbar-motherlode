import hashlib
import io
import json
from pathlib import Path

import pytest

from everbar_motherlode import icloud_migration as migration


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


def test_stream_reports_chunk_and_total_hashes_without_mutating_payload(monkeypatch, capsys):
    payload = b"abcde" * 700_000
    class Body:
        def __init__(self): self.pos = 0
        def read(self, count):
            result = payload[self.pos:self.pos + count]; self.pos += len(result); return result
    class Client:
        def get_object(self, **kwargs): return {"Body": Body(), "ContentLength": len(payload), "ETag": '"source-etag"'}
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    sink = io.BytesIO()
    terminal = migration.stream_r2_object("bucket", "key", 1024 * 1024, sink)
    assert sink.getvalue() == payload
    assert terminal["sha256"] == hashlib.sha256(payload).hexdigest()
    events = [json.loads(line[len(migration.STREAM_EVENT_PREFIX):]) for line in capsys.readouterr().err.splitlines()]
    assert [event["event"] for event in events] == ["CHUNK", "CHUNK", "CHUNK", "CHUNK", "OBJECT"]
    assert sum(event["size"] for event in events if event["event"] == "CHUNK") == len(payload)


def test_delete_refuses_source_metadata_change(monkeypatch):
    class Client:
        def head_object(self, **kwargs): return {"ContentLength": 2, "ETag": '"changed"'}
    monkeypatch.setattr(migration, "_direct_s3_client", lambda _: Client())
    with pytest.raises(RuntimeError, match="changed"):
        migration.delete_verified_r2_object("bucket", "key", 2, "expected")
