"""Validate the materialized POP909 POC against Everbar's current data path."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--everbar-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root
    source = load_json(root / "metadata/source-manifest.json")
    view = load_json(root / "training-view/training-view.json")
    block = load_json(root / "training-view/block-format.json")
    profile = load_json(root / "training-view/active-length-profile.json")
    records = [load_json(path) for path in sorted((root / "records/candidates").glob("*.json"))]
    source_rows = [json.loads(line) for line in (root / "indexes/source-index.jsonl").read_text().splitlines() if line]
    track_rows = [json.loads(line) for line in (root / "indexes/track-inventory.jsonl").read_text().splitlines() if line]
    assert source["primary_song_count"] == 909 and len(source_rows) == 909
    assert source["source"]["revision"] == "d83e6edba6872a704f5d3b8b32f5cb540088dae6"
    assert source["license"]["spdx"] == "MIT"
    assert source["annotation_file_count"] >= 909
    assert len(track_rows) == 4 * len(source_rows)
    assert all(row["pop909_track_label"] in ("", "MELODY", "BRIDGE", "PIANO") for row in track_rows)
    assert len(records) == sum(len(row["candidate_track_labels"]) for row in source_rows)
    assert all(row["pop909_track_label"] in ("MELODY", "BRIDGE", "PIANO") for row in records)
    assert all(row["brick3_receipt_sha256"] == row["receipt"]["receipt_sha256"] for row in records)
    assert all(row["receipt"]["source"]["sha256"] == row["candidate_sha256"] for row in records)
    assert all(row["split"] in ("train", "validation", "test") for row in records)
    piece_splits = {}
    for row in records:
        piece_splits.setdefault(row["source_piece_id"], row["split"])
        assert piece_splits[row["source_piece_id"]] == row["split"]
        assert len(row["sibling_track_ids"]) == 3
    accepted = [row for row in records if row["brick3_status"] == "ACCEPT"]
    rejected = [row for row in records if row["brick3_status"] == "REJECT"]
    assert accepted and all(row["canonical_score_sha256"] for row in accepted)
    assert rejected and all(row["accept_reject_reason_codes"] for row in rejected)
    canonical_groups = Counter(row["canonical_score_sha256"] for row in accepted)
    duplicate_count = sum(count - 1 for count in canonical_groups.values() if count > 1)
    assert duplicate_count == sum(row.get("canonical_dedupe", {}).get("status") == "DUPLICATE" for row in accepted)
    assert view["scope"] == block["scope"] == profile["scope"] == "POP909_POC_ONLY"
    assert not view["production"] and not block["production"] and not profile["production"]
    assert profile["status"] == "TEST_ONLY" and block["status"] == "TEST_ONLY"
    assert profile["corpus_manifest_sha256"] == view["corpus_manifest_sha256"]
    assert block["format_sha256"] == profile["block_format"]["format_sha256"]
    assert block["cap"] == profile["block_format"]["cap"] == view["cap"]
    import sys
    sys.path.insert(0, str(args.everbar_root / "src"))
    from dreamstream_everbar.generation.length import load_length_profile
    from dreamstream_everbar.packing.format import load_packing_format
    from dreamstream_everbar.training.loader import PackedBatchLoader
    fmt = load_packing_format(root / "training-view/block-format.json")
    assert fmt.cap == view["cap"] and not fmt.production
    loaded_profile = load_length_profile(root / "training-view/active-length-profile.json")
    loaded_profile.assert_compatible(block_format_id=fmt.format_id, block_format_sha256=fmt.format_sha256, cap=fmt.cap)
    loader = PackedBatchLoader.from_directory(root / "training-view", batch_size=1, split="train")
    batch = next(iter(loader))
    assert tuple(batch[0].shape) == (1, view["window_bars"] * view["cap"])
    assert tuple(batch[1].shape) == tuple(batch[0].shape)
    arrays = __import__("numpy").load(root / "training-view/packed-bars/input_ids.npy", mmap_mode="r")
    masks = __import__("numpy").load(root / "training-view/packed-bars/active_mask.npy", mmap_mode="r")
    assert arrays.shape[0] == view["packed_bar_count"] and arrays.shape[1] == view["cap"]
    assert arrays.dtype == __import__("numpy").int64 and masks.dtype == __import__("numpy").bool_
    conn = sqlite3.connect(root / "state/canonical.sqlite")
    canonical_streams, notes, bars = [conn.execute(f"select count(*) from {table}").fetchone()[0] for table in ("canonical_streams", "canonical_notes", "canonical_bars")]
    conn.close()
    assert canonical_streams == len(accepted) and notes > 0 and bars > 0
    report = {"status": "PASS", "songs": len(source_rows), "candidates": len(records), "accepted": len(accepted), "rejected": len(rejected), "rejection_reasons": Counter(code for row in rejected for code in row["accept_reject_reason_codes"]), "canonical_duplicate_streams": duplicate_count, "training_view": view, "canonical_feature_base": {"streams": canonical_streams, "notes": notes, "bars": bars}}
    report["rejection_reasons"] = dict(sorted(report["rejection_reasons"].items()))
    (root / "reports/validation.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
