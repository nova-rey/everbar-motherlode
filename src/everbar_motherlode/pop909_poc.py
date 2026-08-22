"""Bounded, POP909-only Everbar V1 proof-of-concept corpus builder.

This module owns orchestration and provenance only.  Brick 3, PerTok, and
Brick 4 remain imported from the pinned Everbar checkout supplied by the
operator.  The command is intentionally separate from Motherlode's central
SQLite/build state so a POC run cannot alter Motherlode-wide identities.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
import zipfile
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import mido

from .feature_base import ensure_feature_schema, materialize_canonical_stream


SCHEMA = "everbar-motherlode.pop909-poc/v1"
DATASET_ID = "pop909"
SOURCE_REVISION = "d83e6edba6872a704f5d3b8b32f5cb540088dae6"
SOURCE_URL = "https://github.com/music-x-lab/POP909-Dataset/archive/refs/heads/master.zip"
SOURCE_REPOSITORY = "https://github.com/music-x-lab/POP909-Dataset"
LICENSE_ID = "MIT"
TRACK_LABELS = ("MELODY", "BRIDGE", "PIANO")
CAPS = (64, 96, 128, 160, 192, 256)
WINDOW_BARS = 4


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha_json(value: Any) -> str:
    return sha_bytes(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)
    return sha_bytes(path.read_bytes())


def stable_id(kind: str, *parts: Any) -> str:
    return f"{kind}_{sha_json(parts)[:24]}"


def _primary_song_ids(source_root: Path) -> list[str]:
    result = []
    for path in sorted(source_root.iterdir()):
        if path.is_dir() and path.name.isdigit() and len(path.name) == 3 and (path / f"{path.name}.mid").is_file():
            result.append(path.name)
    return result


def _absolute_track_end(track: mido.MidiTrack) -> int:
    return sum(int(message.time) for message in track)


def _track_inventory(piece_id: str, index: int, track: mido.MidiTrack) -> dict[str, Any]:
    label = next((str(message.name) for message in track if message.type == "track_name"), "")
    programs = [int(message.program) for message in track if message.type == "program_change"]
    channels = sorted({int(message.channel) for message in track if hasattr(message, "channel")})
    is_drum = 9 in channels
    note_on_count = sum(message.type == "note_on" and int(message.velocity) > 0 for message in track)
    note_off_count = sum(message.type == "note_off" or (message.type == "note_on" and int(message.velocity) == 0) for message in track)
    source_track_id = stable_id("source_track", DATASET_ID, piece_id, index, label, programs, channels, is_drum)
    return {
        "source_track_id": source_track_id,
        "track_index": index,
        "source_track_name": label,
        "pop909_track_label": label,
        "programs": programs,
        "channels": channels,
        "is_drum": is_drum,
        "has_notes": bool(note_on_count or note_off_count),
        "note_on_count": note_on_count,
        "note_off_count": note_off_count,
        "track_end_tick": _absolute_track_end(track),
        "source_native_role": label if label in TRACK_LABELS else None,
    }


def _source_timing(mid: mido.MidiFile) -> dict[str, Any]:
    tempos: list[list[int]] = []
    meters: list[list[int]] = []
    for track in mid.tracks:
        tick = 0
        for message in track:
            tick += int(message.time)
            if message.type == "set_tempo":
                tempos.append([tick, int(message.tempo)])
            elif message.type == "time_signature":
                meters.append([tick, int(message.numerator), int(message.denominator)])
    return {
        "source_tpq": int(mid.ticks_per_beat),
        "tempos_mspq": sorted(tempos),
        "time_signatures": sorted(meters),
        "track_end_ticks": [_absolute_track_end(track) for track in mid.tracks],
    }


def _file_record(path: Path, root: Path) -> dict[str, Any]:
    data = path.read_bytes()
    return {"relative_path": path.relative_to(root).as_posix(), "size_bytes": len(data), "sha256": sha_bytes(data)}


def _source_archive_evidence(archive: Path) -> dict[str, Any]:
    data = archive.read_bytes()
    with zipfile.ZipFile(io.BytesIO(data)) as handle:
        comment = handle.comment.decode("ascii", errors="replace").strip()
        files = [item for item in handle.infolist() if not item.is_dir()]
    if comment != SOURCE_REVISION:
        raise ValueError(f"POP909 archive comment does not match current source revision: {comment!r}")
    return {
        "url": SOURCE_URL,
        "repository": SOURCE_REPOSITORY,
        "revision": SOURCE_REVISION,
        "revision_evidence": "GitHub archive ZIP comment",
        "archive_path": str(archive),
        "archive_sha256": sha_bytes(data),
        "archive_file_count": len(files),
        "archive_zip_comment": comment,
    }


def index_source(*, source_root: Path, archive: Path, output_root: Path) -> dict[str, Any]:
    """Index primary POP909 songs and preserve source/annotation checksums."""
    song_ids = _primary_song_ids(source_root)
    if len(song_ids) != 909 or song_ids != [f"{index:03d}" for index in range(1, 910)]:
        raise ValueError(f"expected primary POP909 songs 001..909, found {len(song_ids)}")
    evidence = _source_archive_evidence(archive)
    pieces: list[dict[str, Any]] = []
    tracks: list[dict[str, Any]] = []
    raw_hashes: Counter[str] = Counter()
    duplicate_song_ids: list[str] = []
    for song_id in song_ids:
        song_root = source_root / song_id
        midi_path = song_root / f"{song_id}.mid"
        raw = midi_path.read_bytes()
        raw_hash = sha_bytes(raw)
        raw_hashes[raw_hash] += 1
        piece_id = stable_id("source_piece", DATASET_ID, SOURCE_REVISION, f"POP909/{song_id}/{song_id}.mid", raw_hash)
        mid = mido.MidiFile(file=io.BytesIO(raw))
        inventory = [_track_inventory(piece_id, index, track) for index, track in enumerate(mid.tracks)]
        sibling_ids = [item["source_track_id"] for item in inventory]
        for item in inventory:
            item = {**item, "source_piece_id": piece_id, "sibling_track_ids": [value for value in sibling_ids if value != item["source_track_id"]], "song_id": song_id}
            tracks.append(item)
        files = [_file_record(path, source_root) for path in sorted(song_root.rglob("*")) if path.is_file()]
        annotation_files = [item for item in files if item["relative_path"].split("/")[-1] != f"{song_id}.mid" and "/versions/" not in item["relative_path"]]
        version_files = [item for item in files if "/versions/" in item["relative_path"]]
        candidate_labels = [item["pop909_track_label"] for item in inventory if item["pop909_track_label"] in TRACK_LABELS and item["has_notes"] and not item["is_drum"]]
        piece = {
            "source_piece_id": piece_id,
            "dataset_id": DATASET_ID,
            "dataset_version": "2020",
            "source_revision": SOURCE_REVISION,
            "song_id": song_id,
            "source_relative_path": f"POP909/{song_id}/{song_id}.mid",
            "source_raw_sha256": raw_hash,
            "source_size_bytes": len(raw),
            "source_timing": _source_timing(mid),
            "source_track_ids": sibling_ids,
            "candidate_track_labels": candidate_labels,
            "annotation_files": annotation_files,
            "version_files": version_files,
            "source_files": files,
            "raw_source_preserved": True,
        }
        pieces.append(piece)
    duplicate_raw_sources = sorted({value for value, count in raw_hashes.items() if count > 1})
    pieces_hash = write_jsonl(output_root / "indexes/source-index.jsonl", pieces)
    tracks_hash = write_jsonl(output_root / "indexes/track-inventory.jsonl", tracks)
    manifest = {
        "schema": "everbar-motherlode.pop909-source-manifest/v1",
        "dataset_id": DATASET_ID,
        "dataset_name": "POP909",
        "dataset_version": "2020",
        "source": evidence,
        "license": {
            "spdx": LICENSE_ID,
            "license_url": "https://github.com/music-x-lab/POP909-Dataset/blob/master/LICENSE",
            "copyright": "Copyright (c) 2020 Music X Lab",
            "training_terms": "The upstream repository publishes the source repository under MIT; retain upstream attribution and verify any external musical-rights obligations independently.",
        },
        "primary_song_count": len(pieces),
        "primary_midi_count": len(pieces),
        "version_midi_count": sum(len(piece["version_files"]) for piece in pieces),
        "annotation_file_count": sum(len(piece["annotation_files"]) for piece in pieces),
        "candidate_track_labels": list(TRACK_LABELS),
        "candidate_track_count": sum(len(piece["candidate_track_labels"]) for piece in pieces),
        "duplicate_primary_raw_sha256_count": len(duplicate_raw_sources),
        "duplicate_primary_raw_sha256": duplicate_raw_sources,
        "duplicate_song_ids": duplicate_song_ids,
        "source_index_sha256": pieces_hash,
        "track_inventory_sha256": tracks_hash,
        "expected_file_counts": {"songs": 909, "primary_midis": 909, "annotations": 909},
    }
    manifest["manifest_hash"] = sha_json(manifest)
    write_json(output_root / "metadata/source-manifest.json", manifest)
    license_path = output_root / "metadata/LICENSE"
    license_path.parent.mkdir(parents=True, exist_ok=True)
    license_path.write_bytes((source_root.parent / "LICENSE").read_bytes())
    write_json(output_root / "metadata/license-record.json", manifest["license"] | {"source_license_sha256": sha_bytes(license_path.read_bytes())})
    (output_root / "metadata/attribution.md").write_text(
        "# POP909 attribution\n\n"
        "Use POP909: A Pop-song Dataset for Music Arrangement Generation, Wang et al., ISMIR 2020.\n\n"
        "Citation: Ziyu Wang, Ke Chen, Junyan Jiang, Yiyi Zhang, Maoran Xu, Shuqi Dai, Guxian Bin, and Gus Xia. "
        "\"POP909: A Pop-song Dataset for Music Arrangement Generation.\" Proceedings of ISMIR, 2020. "
        "https://arxiv.org/abs/2008.07142\n\n"
        "Source repository: https://github.com/music-x-lab/POP909-Dataset\n",
        encoding="utf-8",
    )
    return manifest


def split_for_source_piece(source_piece_id: str) -> str:
    bucket = int(sha_bytes(source_piece_id.encode())[:8], 16) % 1000
    return "train" if bucket < 800 else "validation" if bucket < 900 else "test"


def _candidate_bytes(mid: mido.MidiFile, track: mido.MidiTrack) -> bytes:
    candidate = mido.MidiFile(type=1, ticks_per_beat=mid.ticks_per_beat)
    candidate.tracks.append(track.copy())
    output = io.BytesIO()
    candidate.save(file=output)
    return output.getvalue()


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _candidate_record_paths(root: Path) -> list[Path]:
    return sorted((root / "records/candidates").glob("*.json"))


def _load_existing_records(root: Path) -> dict[str, dict[str, Any]]:
    return {path.stem: json.loads(path.read_text(encoding="utf-8")) for path in _candidate_record_paths(root)}


def _persist_source_tables(conn: sqlite3.Connection, pieces: list[dict[str, Any]], tracks: list[dict[str, Any]]) -> None:
    for piece in pieces:
        conn.execute("insert or replace into source_pieces values(?,?,?,?,?,?,?,?)", (
            piece["source_piece_id"], DATASET_ID, piece["dataset_version"], f"artifact_{piece['source_raw_sha256'][:24]}",
            piece["source_relative_path"], piece["source_raw_sha256"], json.dumps(piece["source_timing"], sort_keys=True),
            json.dumps({"song_id": piece["song_id"], "annotation_files": piece["annotation_files"], "version_files": piece["version_files"], "raw_source_preserved": True}, sort_keys=True),
        ))
    for track in tracks:
        conn.execute("insert or replace into source_tracks values(?,?,?,?,?,?,?,?,?,?)", (
            track["source_track_id"], track["source_piece_id"], track["track_index"], track["source_track_name"],
            json.dumps(track["programs"]), json.dumps(track["channels"]), int(track["is_drum"]), int(track["has_notes"]),
            track["source_native_role"], json.dumps({"song_id": track["song_id"], "pop909_track_label": track["pop909_track_label"], "track_end_tick": track["track_end_tick"]}, sort_keys=True),
        ))


def _brick3_reason_codes(receipt: dict[str, Any]) -> list[str]:
    return sorted(set((receipt.get("decision") or {}).get("reason_codes") or []))


def _tokenization_payload(piece: Any) -> dict[str, Any]:
    spans = tuple(piece.sequence.split_per_bars())
    return {
        "tokenization_sha256": piece.tokenization_sha256,
        "ids": list(piece.ids),
        "tokens": list(piece.tokens),
        "events": list(piece.events),
        "bar_ticks": list(piece.bar_ticks),
        "bar_ids": [list(span.ids) for span in spans],
        "bar_tokens": [list(span.tokens) for span in spans],
    }


def _canonical_notes(score_payload: dict[str, Any]) -> list[SimpleNamespace]:
    return [SimpleNamespace(time=int(row[0]), duration=int(row[1])) for row in score_payload["track"]["notes"]]


class _PersistedSpan:
    def __init__(self, ids: list[int], tokens: list[str]):
        self.ids = tuple(ids)
        self.tokens = tuple(tokens)


class _PersistedSequence:
    def __init__(self, tokenization: dict[str, Any]):
        self.ids = tuple(tokenization["ids"])
        self.tokens = tuple(tokenization["tokens"])
        self.events = tuple(tokenization["events"])
        self._ticks_bars = tuple(tokenization["bar_ticks"])
        self._spans = tuple(_PersistedSpan(ids, tokens) for ids, tokens in zip(tokenization["bar_ids"], tokenization["bar_tokens"]))

    def split_per_bars(self):
        return self._spans


class _PersistedPiece:
    def __init__(self, record: dict[str, Any]):
        tokenization = record["tokenization"]
        self.schema = "dreamstream-everbar.tokenized-piece/v1"
        self.source_id = record["candidate_id"]
        self.source_sha256 = record["candidate_sha256"]
        self.canonical_score_sha256 = record["canonical_score_sha256"]
        self.policy_id = record["receipt"]["policy_id"]
        self.policy_sha256 = record["receipt"]["policy_sha256"]
        self.language_id = record["receipt"]["language_id"]
        self.language_sha256 = record["receipt"]["language_sha256"]
        self.vocabulary_sha256 = record["receipt"]["canonical"].get("vocabulary_sha256", "")
        self.ids = tuple(tokenization["ids"])
        self.tokens = tuple(tokenization["tokens"])
        self.events = tuple(tokenization["events"])
        self.bar_ticks = tuple(tokenization["bar_ticks"])
        self.tokenization_sha256 = tokenization["tokenization_sha256"]
        self.sequence = _PersistedSequence(tokenization)
        notes = _canonical_notes(record["receipt"]["canonical"]["score"])
        self.score = SimpleNamespace(tracks=[SimpleNamespace(notes=notes)])


def _percentiles(values: list[int]) -> dict[str, float | None]:
    if not values:
        return {key: None for key in ("p50", "p75", "p90", "p95", "p99", "max")}
    ordered = sorted(values)
    def higher(percentile: int) -> float:
        index = max(0, min(len(ordered) - 1, (percentile * len(ordered) + 99) // 100 - 1))
        return float(ordered[index])
    return {"p50": higher(50), "p75": higher(75), "p90": higher(90), "p95": higher(95), "p99": higher(99), "max": float(ordered[-1])}


def _cap_report(records: list[dict[str, Any]], candidates: tuple[int, ...] = CAPS) -> dict[str, Any]:
    all_lengths = [len(ids) for record in records for ids in record["tokenization"]["bar_ids"]]
    report: dict[str, Any] = {
        "schema": "everbar-motherlode.pop909-brick4-profile/v1",
        "scope": "POP909_POC_ONLY",
        "accepted_unique_stream_count": len(records),
        "accepted_unique_represented_bar_count": len(all_lengths),
        "token_length_distribution": _percentiles(all_lengths),
        "candidate_caps": {},
    }
    selected = None
    for cap in candidates:
        fitting_records = [record for record in records if all(len(ids) <= cap for ids in record["tokenization"]["bar_ids"])]
        fitting_bars = sum(len(ids) <= cap for record in records for ids in record["tokenization"]["bar_ids"])
        total_bars = len(all_lengths)
        padding = sum(max(0, cap - length) for length in all_lengths)
        label_breakdown: dict[str, Any] = {}
        for label in TRACK_LABELS:
            material = [record for record in records if record["pop909_track_label"] == label]
            lengths = [len(ids) for record in material for ids in record["tokenization"]["bar_ids"]]
            label_fitting = sum(value <= cap for value in lengths)
            label_breakdown[label] = {
                "streams": len(material), "bars": len(lengths), "bars_fit": label_fitting,
                "bar_coverage": label_fitting / len(lengths) if lengths else 0.0,
                "whole_streams_fit": sum(all(value <= cap for value in (len(ids) for ids in item["tokenization"]["bar_ids"])) for item in material),
                "stream_coverage": sum(all(len(ids) <= cap for ids in item["tokenization"]["bar_ids"]) for item in material) / len(material) if material else 0.0,
                "token_length_distribution": _percentiles(lengths),
            }
        bar_coverage = fitting_bars / total_bars if total_bars else 0.0
        stream_coverage = len(fitting_records) / len(records) if records else 0.0
        row = {
            "cap": cap,
            "represented_bar_coverage": bar_coverage,
            "accepted_whole_stream_coverage": stream_coverage,
            "bars_fit": fitting_bars,
            "bars_total": total_bars,
            "whole_streams_fit": len(fitting_records),
            "whole_streams_total": len(records),
            "overflow_bar_count": total_bars - fitting_bars,
            "overflow_stream_count": len(records) - len(fitting_records),
            "pad_ratio_estimate_if_all_fit": padding / (cap * total_bars) if total_bars else 0.0,
            "active_token_ratio_estimate_if_all_fit": 1.0 - (padding / (cap * total_bars) if total_bars else 0.0),
            "token_length_distribution": _percentiles(all_lengths),
            "track_label_breakdown": label_breakdown,
            "context_total_blocks": 1024 // cap,
            "context_previous_blocks": max(0, 1024 // cap - 1),
        }
        report["candidate_caps"][str(cap)] = row
        if selected is None and bar_coverage >= 0.99 and stream_coverage >= 0.99:
            selected = cap
    if selected is None and records:
        selected = max(candidates, key=lambda cap: (report["candidate_caps"][str(cap)]["accepted_whole_stream_coverage"], report["candidate_caps"][str(cap)]["represented_bar_coverage"], -cap))
        criterion = "No candidate reached both 99% represented-bar and 99% whole-stream coverage; chose highest whole-stream coverage, then bar coverage, then smallest cap."
    else:
        criterion = "Smallest candidate cap with at least 99% represented-bar coverage and at least 99% accepted whole-stream coverage."
    report["selected_cap"] = selected
    report["selection_criterion"] = criterion
    report["status"] = "POC_ONLY_CAP_SELECTED" if selected else "NO_ACCEPTED_STREAMS"
    return report


def _load_brick4_helpers(everbar_checkout: Path):
    if str(everbar_checkout / "src") not in sys.path:
        sys.path.insert(0, str(everbar_checkout / "src"))
    from dreamstream_everbar.corpus import evaluate_midi_bytes, load_policy
    from dreamstream_everbar.packing import pack_tokenized_piece, profile_tokenized_pieces, tokenize_corpus_result
    from dreamstream_everbar.packing.format import PackingFormat
    from dreamstream_everbar.generation.length import LengthProfile
    return evaluate_midi_bytes, load_policy, pack_tokenized_piece, profile_tokenized_pieces, tokenize_corpus_result, PackingFormat, LengthProfile


def _write_progress(root: Path, *, started: float, songs_done: int, candidates: int, brick3_calls: int, bars: int, total_songs: int = 909) -> None:
    elapsed = max(1e-9, time.monotonic() - started)
    songs_per_second = songs_done / elapsed
    progress = {
        "schema": "everbar-motherlode.pop909-progress/v1", "state": "RUNNING", "stage": "BRICK3_AND_TOKENIZATION",
        "songs_done": songs_done, "songs_total": total_songs, "candidate_streams": candidates, "brick3_streams": brick3_calls,
        "accepted_represented_bars": bars, "elapsed_seconds": elapsed,
        "songs_per_second": songs_per_second, "candidate_streams_per_second": candidates / elapsed,
        "brick3_streams_per_second": brick3_calls / elapsed, "bars_per_second": bars / elapsed,
        "estimated_full_seconds": (total_songs / songs_per_second) if songs_per_second else None,
        "updated_at_epoch": time.time(),
    }
    write_json(root / "progress/current.json", progress)


def _stream_records(root: Path) -> list[dict[str, Any]]:
    return sorted((json.loads(path.read_text(encoding="utf-8")) for path in (root / "records/candidates").glob("*.json") if path.exists()), key=lambda row: row["candidate_id"])


def _finalize_training_view(*, root: Path, source_manifest: dict[str, Any], candidate_records: list[dict[str, Any]], everbar_sha: str, motherlode_sha: str, block_helpers: tuple[Any, ...]) -> dict[str, Any]:
    _evaluate, _load_policy, _pack, profile_tokenized_pieces, _tokenize, PackingFormat, LengthProfile = block_helpers
    accepted = [row for row in candidate_records if row.get("brick3_status") == "ACCEPT"]
    unique = [row for row in accepted if row.get("canonical_dedupe", {}).get("status") == "UNIQUE"]
    # Exercise the existing Brick 4 profiler over durable post-PerTok records.
    existing_profile = profile_tokenized_pieces([_PersistedPiece(row) for row in unique], candidates=CAPS)
    cap_report = _cap_report(unique)
    selected_cap = cap_report["selected_cap"]
    if selected_cap is None:
        raise RuntimeError("POP909 produced no accepted streams to pack")
    candidate_manifest = [
        {key: row.get(key) for key in ("candidate_id", "source_piece_id", "source_track_id", "song_id", "pop909_track_label", "candidate_sha256", "brick3_status", "canonical_score_sha256", "brick3_receipt_sha256", "accept_reject_reason_codes", "split", "canonical_dedupe")}
        for row in candidate_records
    ]
    candidate_manifest_sha = sha_json(candidate_manifest)
    selected_unique = [row for row in unique if all(len(ids) <= selected_cap for ids in row["tokenization"]["bar_ids"])]
    corpus_manifest = {
        "schema": "everbar-motherlode.pop909-corpus-manifest/v1",
        "scope": "POP909_POC_ONLY",
        "dataset_id": DATASET_ID,
        "source_manifest_sha256": source_manifest["manifest_hash"],
        "candidate_manifest_sha256": candidate_manifest_sha,
        "everbar_brick3_sha": everbar_sha,
        "motherlode_sha": motherlode_sha,
        "policy_id": unique[0]["receipt"]["policy_id"],
        "policy_sha256": unique[0]["receipt"]["policy_sha256"],
        "language_id": unique[0]["receipt"]["language_id"],
        "language_sha256": unique[0]["receipt"]["language_sha256"],
        "selected_cap": selected_cap,
        "accepted_candidate_stream_count": len(accepted),
        "accepted_unique_stream_count": len(unique),
        "training_stream_count": len(selected_unique),
        "canonical_duplicate_stream_count": len(accepted) - len(unique),
        "overflow_stream_count_at_selected_cap": len(unique) - len(selected_unique),
        "split_policy": "source_piece_sha256_mod_1000: train<800, validation<900, test>=900",
        "window_policy": f"within-stream contiguous {WINDOW_BARS}-represented-bar windows; no cross-piece or cross-sibling windows",
    }
    corpus_manifest["manifest_hash"] = sha_json(corpus_manifest)
    write_json(root / "training-view/corpus-manifest.json", corpus_manifest)
    write_json(root / "reports/brick4-existing-machinery.json", existing_profile | {"scope": "POP909_POC_ONLY", "production": False, "corpus_manifest_sha256": corpus_manifest["manifest_hash"]})
    write_json(root / "reports/candidate-caps.json", cap_report | {"corpus_manifest_sha256": corpus_manifest["manifest_hash"], "existing_brick4_profile_selected_cap": existing_profile.get("selected_cap")})

    base_format = PackingFormat(format_id=f"pop909-poc-block-format-v1-cap-{selected_cap}", cap=selected_cap, production=False)
    block_format = base_format.to_dict() | {
        "status": "TEST_ONLY", "scope": "POP909_POC_ONLY", "production": False,
        "corpus_manifest_sha256": corpus_manifest["manifest_hash"], "authority": "POC_ONLY_NOT_MOTHERLODE_PRODUCTION",
        "format_sha256": base_format.format_sha256,
    }
    write_json(root / "training-view/block-format.json", block_format)

    later_lengths = sorted(len(ids) for row in selected_unique for ids in row["tokenization"]["bar_ids"][1:])
    length_counts = Counter(later_lengths)
    profile = LengthProfile(
        profile_id=f"pop909-poc-active-length-v1-cap-{selected_cap}", block_format_id=base_format.format_id,
        block_format_sha256=base_format.format_sha256, cap=selected_cap,
        counts=tuple(sorted((length, count) for length, count in length_counts.items())), status="TEST_ONLY",
        source_profile_sha256=sha_json({"cap_report": cap_report, "selected_cap": selected_cap}),
    ).to_dict()
    profile.update({
        "scope": "POP909_POC_ONLY", "production": False, "authority": "POC_ONLY_NOT_BRICK6_PRODUCTION",
        "corpus_manifest_sha256": corpus_manifest["manifest_hash"],
        "pertok": {"language_id": corpus_manifest["language_id"], "language_sha256": corpus_manifest["language_sha256"], "config_path": "configs/pertok-v1.json"},
    })
    write_json(root / "training-view/active-length-profile.json", profile)

    import numpy as np
    view = root / "training-view"
    packed = view / "packed-bars"
    packed.mkdir(parents=True, exist_ok=True)
    total_bars = sum(len(row["tokenization"]["bar_ids"]) for row in selected_unique)
    total_windows = sum(max(0, len(row["tokenization"]["bar_ids"]) - WINDOW_BARS + 1) for row in selected_unique)
    bar_ids_array = np.lib.format.open_memmap(packed / "input_ids.npy", mode="w+", dtype=np.int64, shape=(total_bars, selected_cap))
    bar_mask_array = np.lib.format.open_memmap(packed / "active_mask.npy", mode="w+", dtype=np.bool_, shape=(total_bars, selected_cap))
    window_ids_array = np.lib.format.open_memmap(view / "input_ids.npy", mode="w+", dtype=np.int64, shape=(total_windows, WINDOW_BARS, selected_cap))
    window_mask_array = np.lib.format.open_memmap(view / "active_mask.npy", mode="w+", dtype=np.bool_, shape=(total_windows, WINDOW_BARS, selected_cap))
    bar_rows: list[dict[str, Any]] = []
    window_rows: list[dict[str, Any]] = []
    split_indices: dict[str, list[int]] = {"train": [], "validation": [], "test": []}
    bar_offset = window_offset = 0
    for row in selected_unique:
        bars = row["tokenization"]["bar_ids"]
        split = row["split"]
        for bar_index, ids in enumerate(bars):
            values = np.asarray(ids, dtype=np.int64)
            bar_ids_array[bar_offset, :] = 0
            bar_mask_array[bar_offset, :] = False
            bar_ids_array[bar_offset, : len(values)] = values
            bar_mask_array[bar_offset, : len(values)] = True
            bar_rows.append({"bar_row": bar_offset, "candidate_id": row["candidate_id"], "source_piece_id": row["source_piece_id"], "source_track_id": row["source_track_id"], "song_id": row["song_id"], "pop909_track_label": row["pop909_track_label"], "split": split, "bar_index": bar_index, "active_length": len(ids)})
            bar_offset += 1
        for start in range(max(0, len(bars) - WINDOW_BARS + 1)):
            for block_index in range(WINDOW_BARS):
                ids = bars[start + block_index]
                values = np.asarray(ids, dtype=np.int64)
                window_ids_array[window_offset, block_index, :] = 0
                window_mask_array[window_offset, block_index, :] = False
                window_ids_array[window_offset, block_index, : len(values)] = values
                window_mask_array[window_offset, block_index, : len(values)] = True
            window_rows.append({"window_index": window_offset, "candidate_id": row["candidate_id"], "source_piece_id": row["source_piece_id"], "source_track_id": row["source_track_id"], "song_id": row["song_id"], "pop909_track_label": row["pop909_track_label"], "split": split, "start_bar": start})
            split_indices[split].append(window_offset)
            window_offset += 1
    del bar_ids_array, bar_mask_array, window_ids_array, window_mask_array
    bar_manifest_sha = write_jsonl(view / "packed-bars/bar-manifest.jsonl", bar_rows)
    window_manifest_sha = write_jsonl(view / "window-manifest.jsonl", window_rows)
    for split, indices in split_indices.items():
        np.save(view / f"{split}-indices.npy", np.asarray(indices, dtype=np.int64))
    view_manifest = {
        "schema": "dreamstream-everbar.training-view/v1", "scope": "POP909_POC_ONLY", "production": False,
        "corpus_manifest_sha256": corpus_manifest["manifest_hash"], "block_format_id": base_format.format_id, "block_format_sha256": base_format.format_sha256,
        "cap": selected_cap, "active_length_profile_id": profile["profile_id"], "active_length_profile_sha256": profile["profile_sha256"],
        "pertok_language_id": corpus_manifest["language_id"], "pertok_language_sha256": corpus_manifest["language_sha256"],
        "input_ids_shape": [total_windows, WINDOW_BARS, selected_cap], "active_mask_shape": [total_windows, WINDOW_BARS, selected_cap],
        "packed_bar_count": total_bars, "window_count": total_windows, "window_bars": WINDOW_BARS,
        "split_window_counts": {key: len(value) for key, value in split_indices.items()}, "bar_manifest_sha256": bar_manifest_sha, "window_manifest_sha256": window_manifest_sha,
        "loader": "dreamstream_everbar.training.loader.SmokeBatchLoader.from_directory",
        "trainer_config": "trainer-config.json", "model_config": "model-config.json",
        "authority": "POC_ONLY_NOT_MOTHERLODE_PRODUCTION",
    }
    view_manifest["manifest_hash"] = sha_json(view_manifest)
    write_json(view / "training-view.json", view_manifest)
    model_config = {"schema": "brick8-training-model/v1", "vocab_size": 686, "hidden_size": 192, "conditioning_dim": 192, "num_layers": 6, "num_heads": 6, "mlp_ratio": 4, "dropout": 0.0, "block_size": selected_cap, "num_blocks": WINDOW_BARS, "model_length": WINDOW_BARS * selected_cap, "test_only": True}
    training_config = {"schema": "brick8-training-config/v1", "learning_rate": 0.0005, "betas": [0.9, 0.95], "weight_decay": 0.0, "epsilon": 1e-8, "warmup_steps": 50, "max_steps": 2000, "training_minutes": 50, "effective_batch_size": 32, "precision": "fp32", "seed": 8, "checkpoint_minutes": [5, 10, 20, 30, 40, 50]}
    write_json(view / "model-config.json", model_config)
    write_json(view / "trainer-config.json", training_config)
    write_json(root / "reports/training-view.json", view_manifest)
    return {"source_manifest": source_manifest, "corpus_manifest": corpus_manifest, "cap_report": cap_report, "view_manifest": view_manifest, "selected_cap": selected_cap, "accepted_streams": len(accepted), "accepted_unique_streams": len(unique), "training_streams": len(selected_unique), "represented_bars": total_bars, "windows": total_windows, "active_tokens": sum(len(ids) for row in selected_unique for ids in row["tokenization"]["bar_ids"]), "later_active_tokens": sum(later_lengths)}


def run_poc(*, root: Path, source_root: Path, archive: Path, everbar_checkout: Path, everbar_sha: str, motherlode_sha: str, limit_songs: int | None, resume: bool) -> dict[str, Any]:
    started = time.monotonic()
    for relative in ("metadata", "indexes", "records/candidates", "records/streams", "receipts/brick3", "training-view", "packed", "reports", "progress", "state"):
        (root / relative).mkdir(parents=True, exist_ok=True)
    source_manifest = index_source(source_root=source_root, archive=archive, output_root=root)
    pieces = _load_jsonl(root / "indexes/source-index.jsonl")
    tracks = _load_jsonl(root / "indexes/track-inventory.jsonl")
    block_helpers = _load_brick4_helpers(everbar_checkout)
    evaluate_midi_bytes, load_policy, _pack, _profile, tokenize_corpus_result, _PackingFormat, _LengthProfile = block_helpers
    everbar_root = everbar_checkout
    policy = load_policy(everbar_root / "configs/corpus-policy-v1.json", everbar_root / "artifacts/vocab/pertok-v1-vocab.json")
    existing = _load_existing_records(root) if resume else {}
    canonical_seen: dict[str, str] = {}
    for record in sorted(existing.values(), key=lambda row: row["candidate_id"]):
        if record.get("brick3_status") == "ACCEPT" and record.get("canonical_score_sha256") and record.get("canonical_dedupe", {}).get("status") == "UNIQUE":
            canonical_seen.setdefault(record["canonical_score_sha256"], record["candidate_id"])
    conn = sqlite3.connect(root / "state/canonical.sqlite")
    ensure_feature_schema(conn)
    _persist_source_tables(conn, pieces, tracks)
    conn.commit()
    selected_pieces = pieces if limit_songs is None else pieces[:limit_songs]
    songs_done = candidates = brick3_calls = accepted_bars = 0
    for piece in selected_pieces:
        song_id = piece["song_id"]
        source_path = source_root / song_id / f"{song_id}.mid"
        raw = source_path.read_bytes()
        mid = mido.MidiFile(file=io.BytesIO(raw))
        inventory = [item for item in tracks if item["source_piece_id"] == piece["source_piece_id"]]
        for item in sorted(inventory, key=lambda value: value["track_index"]):
            label = item["pop909_track_label"]
            if label not in TRACK_LABELS or not item["has_notes"] or item["is_drum"]:
                continue
            candidate_id = stable_id("pop909_candidate", piece["source_piece_id"], item["source_track_id"], label)
            candidate_bytes = _candidate_bytes(mid, mid.tracks[item["track_index"]])
            candidate_sha = sha_bytes(candidate_bytes)
            candidate_path = root / "derived/candidates" / song_id / f"{label}.mid"
            candidate_path.parent.mkdir(parents=True, exist_ok=True)
            if not candidate_path.exists() or sha_bytes(candidate_path.read_bytes()) != candidate_sha:
                candidate_path.write_bytes(candidate_bytes)
            split = split_for_source_piece(piece["source_piece_id"])
            record_path = root / "records/candidates" / f"{candidate_id}.json"
            if candidate_id in existing and existing[candidate_id].get("candidate_sha256") == candidate_sha and existing[candidate_id].get("brick3_status") in {"ACCEPT", "REJECT"}:
                continue
            result = evaluate_midi_bytes(candidate_bytes, policy, corpus_id="pop909-v1", relative_path=f"POP909/{song_id}/{label}.mid", manifest_path=everbar_root / "artifacts/vocab/pertok-v1-vocab.json", config_path=everbar_root / "configs/pertok-v1.json")
            receipt = result.receipt.payload
            status = receipt["decision"]["status"]
            envelope: dict[str, Any] = {
                "schema": "everbar-motherlode.pop909-candidate/v1", "candidate_id": candidate_id, "source_piece_id": piece["source_piece_id"], "song_id": song_id,
                "source_track_id": item["source_track_id"], "source_track_index": item["track_index"], "pop909_track_label": label, "source_track_name": item["source_track_name"],
                "sibling_track_ids": item["sibling_track_ids"], "programs": item["programs"], "channels": item["channels"], "split": split,
                "candidate_relative_path": f"derived/candidates/{song_id}/{label}.mid", "candidate_sha256": candidate_sha, "brick3_status": status,
                "brick3_receipt_sha256": receipt["receipt_sha256"], "accept_reject_reason_codes": _brick3_reason_codes(receipt), "canonical_score_sha256": ((receipt.get("canonical") or {}).get("event_sha256")),
                "receipt": receipt, "everbar_sha": everbar_sha, "motherlode_sha": motherlode_sha,
            }
            if status == "ACCEPT":
                canonical_hash = envelope["canonical_score_sha256"]
                duplicate_of = canonical_seen.get(canonical_hash)
                if duplicate_of:
                    envelope["canonical_dedupe"] = {"status": "DUPLICATE", "kept_candidate_id": duplicate_of, "identity": "brick3_canonical_event_sha256"}
                else:
                    envelope["canonical_dedupe"] = {"status": "UNIQUE", "identity": "brick3_canonical_event_sha256"}
                    canonical_seen[canonical_hash] = candidate_id
                    tokenized = tokenize_corpus_result(result, config_path=everbar_root / "configs/pertok-v1.json", manifest_path=everbar_root / "artifacts/vocab/pertok-v1-vocab.json")
                    envelope["tokenization"] = _tokenization_payload(tokenized)
                    accepted_bars += len(envelope["tokenization"]["bar_ids"])
                materialize_canonical_stream(conn, stream_id=candidate_id, dataset_id=DATASET_ID, detail={"brick3": "ACCEPT", "everbar_sha": everbar_sha, "receipt": receipt, "provenance": {"dataset_version": "2020", "source_piece_id": piece["source_piece_id"], "source_track_id": item["source_track_id"], "sibling_track_ids": item["sibling_track_ids"], "programs": item["programs"], "is_drum": False, "source_track_name": item["source_track_name"], "source_native_role": label, "source_timing": piece["source_timing"]}})
            write_json(record_path, envelope)
            existing[candidate_id] = envelope
            candidates += 1
            brick3_calls += 1
        songs_done += 1
        conn.commit()
        _write_progress(root, started=started, songs_done=songs_done, candidates=candidates, brick3_calls=brick3_calls, bars=accepted_bars)
    conn.commit()
    conn.close()
    records = _stream_records(root)
    if limit_songs is not None:
        elapsed = max(1e-9, time.monotonic() - started)
        sample_report = {"schema": "everbar-motherlode.pop909-throughput-sample/v1", "songs": len(selected_pieces), "candidates": candidates, "brick3_streams": brick3_calls, "accepted_unique_represented_bars": accepted_bars, "elapsed_seconds": elapsed, "songs_per_second": len(selected_pieces) / elapsed, "candidate_streams_per_second": candidates / elapsed, "brick3_streams_per_second": brick3_calls / elapsed, "bars_per_second": accepted_bars / elapsed, "estimated_full_seconds": elapsed * 909 / max(1, len(selected_pieces)), "estimated_full_hours": elapsed * 909 / max(1, len(selected_pieces)) / 3600}
        write_json(root / "reports/throughput-sample.json", sample_report)
        _write_progress(root, started=started, songs_done=songs_done, candidates=candidates, brick3_calls=brick3_calls, bars=accepted_bars)
        return sample_report
    final = _finalize_training_view(root=root, source_manifest=source_manifest, candidate_records=records, everbar_sha=everbar_sha, motherlode_sha=motherlode_sha, block_helpers=block_helpers)
    elapsed = max(1e-9, time.monotonic() - started)
    final["wall_seconds"] = elapsed
    final["songs_processed"] = len(selected_pieces)
    final["candidate_streams_processed"] = len(records)
    final["brick3_streams_per_second"] = len(records) / elapsed
    write_json(root / "reports/summary.json", final)
    _write_progress(root, started=started, songs_done=songs_done, candidates=len(records), brick3_calls=len(records), bars=final["represented_bars"])
    progress = json.loads((root / "progress/current.json").read_text())
    progress.update({"state": "COMPLETE", "stage": "TRAINING_VIEW_READY", "finished_at_epoch": time.time(), "estimated_full_seconds": elapsed})
    write_json(root / "progress/current.json", progress)
    return final


def _detached(argv: list[str], root: Path) -> int:
    log = root / "logs/pop909-poc.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    handle = log.open("a", encoding="utf-8")
    child_args = [arg for arg in argv if arg != "--detach"]
    child = subprocess.Popen([sys.executable, "-m", "everbar_motherlode.pop909_poc", *child_args], stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    write_json(root / "progress/launch.json", {"pid": child.pid, "log_path": str(log), "command": [sys.executable, "-m", "everbar_motherlode.pop909_poc", *child_args], "launched_at_epoch": time.time()})
    print(child.pid)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--everbar-checkout", type=Path, required=True)
    parser.add_argument("--everbar-sha", required=True)
    parser.add_argument("--motherlode-sha", required=True)
    parser.add_argument("--songs", type=int, default=None, help="bounded sample size; omit for full 909-song processing")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--detach", action="store_true")
    args = parser.parse_args(argv)
    if args.songs is not None and not 1 <= args.songs <= 909:
        parser.error("--songs must be in 1..909")
    args.root.mkdir(parents=True, exist_ok=True)
    if args.detach:
        return _detached(sys.argv[1:] if argv is None else argv, args.root)
    result = run_poc(root=args.root, source_root=args.source_root, archive=args.archive, everbar_checkout=args.everbar_checkout, everbar_sha=args.everbar_sha, motherlode_sha=args.motherlode_sha, limit_songs=args.songs, resume=args.resume)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
