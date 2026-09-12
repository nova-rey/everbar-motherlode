import json
import sqlite3
from pathlib import Path

import pytest

from everbar_motherlode.core import db, sha, writej
from everbar_motherlode.streaming_consolidation import PackageVerificationError, stream_consolidate


def _detail(canonical_hash: str, stream_id: str) -> dict:
    return {
        "brick3": "ACCEPT",
        "everbar_sha": "everbar-fixture",
        "provenance": {
            "dataset_version": "fixture-v1", "source_piece_id": f"piece-{stream_id}",
            "source_track_id": f"track-{stream_id}", "sibling_track_ids": [], "programs": [0],
            "is_drum": False, "source_track_name": "fixture", "source_native_role": None,
            "source_timing": {"source_tpq": 480},
        },
        "receipt": {
            "receipt_sha256": f"receipt-{stream_id}", "policy_id": "corpus-policy-v1",
            "policy_sha256": "policy", "language_id": "pertok-v1", "language_sha256": "language",
            "canonical": {
                "event_sha256": canonical_hash,
                "score": {
                    "schema": "dreamstream-everbar.canonical-score/v1", "tpq": 480,
                    "track": {"program": 0, "is_drum": False, "notes": [[0, 480, 60, 90]]},
                    "tempo": [[0, 500000]], "time_signature": [[0, 4, 4]],
                },
            },
        },
    }


def _package(root: Path, index: int, rows: list[tuple[str, str]], *, corrupt_ids: bool = False) -> None:
    label = f"fixture-part-{index:05d}-of-00002"
    worker_root = root / "worker" / label
    conn = db(worker_root)
    for stream_id, canonical_hash in rows:
        conn.execute("insert into items values(?,?,?,?,?,?,?)", (stream_id, "fixture", "BRICK3_COMPLETE", "unused.mid", canonical_hash, "raw", json.dumps(_detail(canonical_hash, stream_id))))
    conn.commit(); conn.close()
    destination = root / "source" / "runs" / "run-a" / "fixture" / f"shard-{index:05d}-of-00002"
    destination.mkdir(parents=True)
    source_db = worker_root / "state" / "motherlode.sqlite"
    (destination / "shard.sqlite").write_bytes(source_db.read_bytes())
    ids = sorted(stream_id for stream_id, _ in rows)
    declared_ids = ids + ["unexpected"] if corrupt_ids else ids
    manifest = {
        "state": "COMPLETE", "run_id": "run-a", "dataset_id": "fixture", "shard_index": index,
        "shard_count": 2, "item_count": len(declared_ids), "item_ids_sha256": sha("\n".join(declared_ids)),
    }
    writej(destination / "item-ids.json", {"item_ids": declared_ids})
    writej(destination / "manifest.json", manifest)
    writej(destination / "completion.json", manifest)
    writej(destination / "shard-receipt.json", {"state": "COMPLETE", "dataset_id": "fixture", "partition_index": index, "partitions": 2})


def test_streaming_consolidation_projects_one_package_at_a_time_and_resumes(tmp_path: Path):
    _package(tmp_path, 0, [("stream-a", "hash-a"), ("stream-b", "hash-shared")])
    _package(tmp_path, 1, [("stream-c", "hash-shared"), ("stream-d", "hash-d")])
    report = stream_consolidate(
        workspace=tmp_path / "workspace", source_uri="file://" + str(tmp_path / "source"),
        output_uri="file://" + str(tmp_path / "output"), run_id="run-a", dataset_id="fixture",
        shard_count=2, consolidation_id="fixture-consolidation",
    )
    assert [entry["state"] for entry in report["outcomes"]] == ["COMPLETE", "COMPLETE"]
    assert not list((tmp_path / "workspace" / "streaming-consolidations" / "fixture-consolidation" / "work").glob("*/shard.sqlite"))
    output = tmp_path / "output" / "consolidations" / "fixture-consolidation" / "fixture"
    first = sqlite3.connect(output / "shard-00000-of-00002" / "canonical.sqlite")
    second = sqlite3.connect(output / "shard-00001-of-00002" / "canonical.sqlite")
    assert first.execute("select count(*) from canonical_streams").fetchone()[0] == 2
    assert second.execute("select count(*) from canonical_streams").fetchone()[0] == 1
    assert second.execute("select status,kept_stream_id from canonical_dedupe where stream_id='stream-c'").fetchone() == ("DUPLICATE", "stream-b")
    assert second.execute("select count(*) from stream_provenance").fetchone()[0] == 2
    first.close(); second.close()
    index = sqlite3.connect(tmp_path / "workspace" / "streaming-consolidations" / "fixture-consolidation" / "canonical-index.sqlite")
    assert index.execute("select occurrence_count from canonical_hashes where canonical_score_sha256='hash-shared'").fetchone()[0] == 2
    index.close()
    resumed = stream_consolidate(
        workspace=tmp_path / "workspace", source_uri="file://" + str(tmp_path / "source"),
        output_uri="file://" + str(tmp_path / "output"), run_id="run-a", dataset_id="fixture",
        shard_count=2, consolidation_id="fixture-consolidation",
    )
    assert [entry["state"] for entry in resumed["outcomes"]] == ["SKIPPED_COMPLETE", "SKIPPED_COMPLETE"]


def test_streaming_consolidation_fails_closed_on_item_ledger_mismatch(tmp_path: Path):
    _package(tmp_path, 0, [("stream-a", "hash-a")], corrupt_ids=True)
    _package(tmp_path, 1, [("stream-b", "hash-b")])
    with pytest.raises(PackageVerificationError, match="item IDs"):
        stream_consolidate(
            workspace=tmp_path / "workspace", source_uri="file://" + str(tmp_path / "source"),
            output_uri="file://" + str(tmp_path / "output"), run_id="run-a", dataset_id="fixture",
            shard_count=2, consolidation_id="fixture-consolidation",
        )
    assert not (tmp_path / "output" / "consolidations" / "fixture-consolidation" / "fixture" / "shard-00000-of-00002" / "completion.json").exists()
