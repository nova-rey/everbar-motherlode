"""Durable post-Brick-3 event base and independently versioned features.

This module deliberately consumes only persisted Brick 3 receipts and SQLite
canonical tables.  It never opens a MIDI file, decodes PerTok, or invokes
Everbar.  The canonical tables are authority; feature databases are replaceable
views keyed by extractor version.
"""
from __future__ import annotations

import json, math, sqlite3, statistics, time
from pathlib import Path
from typing import Any


CANONICAL_EVENT_SCHEMA = "everbar-motherlode.canonical-event/v1"
PRIMITIVE_EXTRACTOR = "primitive-v1"


def ensure_feature_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    create table if not exists source_pieces(
      source_piece_id text primary key, dataset_id text not null,
      dataset_version text, source_artifact_id text, source_relative_path text,
      source_raw_sha256 text, source_timing_json text not null, detail_json text not null
    );
    create table if not exists source_tracks(
      source_track_id text primary key, source_piece_id text not null,
      track_index integer not null, source_track_name text, programs_json text not null,
      channels_json text not null, is_drum integer not null, has_notes integer not null,
      source_native_role text, timing_json text not null
    );
    create table if not exists canonical_streams(
      stream_id text primary key, canonical_score_sha256 text not null,
      dataset_id text not null, dataset_version text,
      source_piece_id text, source_track_id text, sibling_track_ids_json text not null,
      programs_json text not null, is_drum integer not null, source_track_name text,
      source_native_role text, source_timing_json text not null,
      brick3_receipt_sha256 text not null, everbar_sha text not null,
      policy_id text, policy_sha256 text, language_id text, language_sha256 text,
      canonical_schema text not null, tpq integer not null,
      meter_numerator integer not null, meter_denominator integer not null,
      tempo_mspq integer not null, canonical_span_end_tick integer not null
    );
    create table if not exists canonical_notes(
      stream_id text not null, note_index integer not null,
      onset_tick integer not null, duration_ticks integer not null,
      end_tick integer not null, pitch integer not null, velocity integer not null,
      onset_bar_index integer not null, end_bar_index integer not null,
      onset_in_bar_tick integer not null,
      primary key(stream_id, note_index)
    );
    create table if not exists canonical_bars(
      stream_id text not null, bar_index integer not null,
      start_tick integer not null, end_tick integer not null,
      numerator integer not null, denominator integer not null,
      beats real not null, is_empty integer not null,
      primary key(stream_id, bar_index)
    );
    create index if not exists canonical_notes_stream_onset on canonical_notes(stream_id,onset_tick);
    create index if not exists canonical_bars_stream_index on canonical_bars(stream_id,bar_index);
    """)


def _bar_ticks(tpq: int, numerator: int, denominator: int) -> int:
    value = tpq * 4 * numerator
    if value % denominator:
        raise ValueError("canonical meter does not have integral tick bars")
    return value // denominator


def materialize_canonical_stream(conn: sqlite3.Connection, *, stream_id: str, dataset_id: str, detail: dict[str, Any]) -> bool:
    """Materialize one accepted Brick 3 receipt without touching MIDI."""
    if detail.get("brick3") != "ACCEPT":
        return False
    receipt = detail.get("receipt") or {}
    canonical = receipt.get("canonical") or {}
    score = canonical.get("score") or {}
    if score.get("schema") != "dreamstream-everbar.canonical-score/v1":
        return False
    notes = score.get("track", {}).get("notes")
    tempos = score.get("tempo")
    meters = score.get("time_signature")
    if not isinstance(notes, list) or len(tempos or []) != 1 or len(meters or []) != 1:
        return False
    tpq = int(score["tpq"])
    tempo_tick, mspq = tempos[0]
    meter_tick, numerator, denominator = meters[0]
    if int(tempo_tick) != 0 or int(meter_tick) != 0:
        return False
    numerator, denominator, mspq = int(numerator), int(denominator), int(mspq)
    bar_ticks = _bar_ticks(tpq, numerator, denominator)
    canonical_hash = canonical.get("event_sha256")
    if not canonical_hash:
        return False
    normalized = [(int(onset), int(duration), int(pitch), int(velocity)) for onset, duration, pitch, velocity in notes]
    if any(duration <= 0 for _onset, duration, _pitch, _velocity in normalized):
        return False
    span_end = max((onset + duration for onset, duration, _pitch, _velocity in normalized), default=0)
    # Canonical score authority ends at the final sounding note. Empty bars
    # inside that represented span are materialized; source-only trailing EOT
    # silence remains separately available in source_timing_json.
    bar_count = max(1, math.ceil(span_end / bar_ticks))
    provenance = detail.get("provenance") or detail
    conn.execute("""insert into canonical_streams values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      on conflict(stream_id) do update set canonical_score_sha256=excluded.canonical_score_sha256,
      brick3_receipt_sha256=excluded.brick3_receipt_sha256, canonical_span_end_tick=excluded.canonical_span_end_tick""", (
        stream_id, canonical_hash, dataset_id, provenance.get("dataset_version"),
        provenance.get("source_piece_id"), provenance.get("source_track_id"),
        json.dumps(provenance.get("sibling_track_ids", []), sort_keys=True),
        json.dumps(provenance.get("programs", []), sort_keys=True), int(bool(provenance.get("is_drum", False))),
        provenance.get("source_track_name"), provenance.get("source_native_role"),
        json.dumps(provenance.get("source_timing", {}), sort_keys=True), receipt.get("receipt_sha256", ""),
        detail.get("everbar_sha", ""), receipt.get("policy_id"), receipt.get("policy_sha256"),
        receipt.get("language_id"), receipt.get("language_sha256"), score["schema"], tpq, numerator,
        denominator, mspq, span_end,
    ))
    conn.execute("delete from canonical_notes where stream_id=?", (stream_id,))
    conn.execute("delete from canonical_bars where stream_id=?", (stream_id,))
    note_rows = []
    # A difference array makes bar occupancy O(notes + bars), rather than
    # rescanning every note for every bar of a long accepted stream. It is only
    # an insertion optimization: the durable canonical note and bar values are
    # identical to the previous definition.
    active_delta = [0] * (bar_count + 1)
    for index, (onset, duration, pitch, velocity) in enumerate(normalized):
        end = onset + duration
        onset_bar = onset // bar_ticks
        end_bar = max(onset_bar, (end - 1) // bar_ticks)
        note_rows.append((
            stream_id, index, onset, duration, end, pitch, velocity, onset_bar,
            end_bar, onset % bar_ticks,
        ))
        active_delta[onset_bar] += 1
        active_delta[end_bar + 1] -= 1
    conn.executemany("insert into canonical_notes values(?,?,?,?,?,?,?,?,?,?)", note_rows)
    active = 0
    bar_rows = []
    for index in range(bar_count):
        active += active_delta[index]
        start, end = index * bar_ticks, (index + 1) * bar_ticks
        bar_rows.append((
            stream_id, index, start, end, numerator, denominator, numerator * 4 / denominator, int(active == 0),
        ))
    conn.executemany("insert into canonical_bars values(?,?,?,?,?,?,?,?)", bar_rows)
    return True


def backfill_canonical(root: Path) -> dict[str, Any]:
    """Idempotently backfill canonical tables from durable SQLite receipts only."""
    databases = [root / "state" / "motherlode.sqlite"]
    databases.extend(sorted((root / "state" / "shards").glob("*/state/motherlode.sqlite")))
    scanned = materialized = 0
    for path in databases:
        if not path.exists():
            continue
        conn = sqlite3.connect(path)
        ensure_feature_schema(conn)
        rows = conn.execute("select id,dataset_id,detail from items where state='BRICK3_COMPLETE'").fetchall()
        for stream_id, dataset_id, detail_json in rows:
            scanned += 1
            try:
                if materialize_canonical_stream(conn, stream_id=stream_id, dataset_id=dataset_id, detail=json.loads(detail_json)):
                    materialized += 1
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
        conn.commit(); conn.close()
    report = {"schema": CANONICAL_EVENT_SCHEMA, "scanned_items": scanned, "materialized_streams": materialized, "used_raw_midi": False, "used_brick3": False, "at": time.time()}
    report_path = root / "reports" / "canonical-backfill.json"; report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def extract_primitive_features(root: Path, extractor_id: str = PRIMITIVE_EXTRACTOR) -> dict[str, Any]:
    """Compute a small proof feature view from canonical SQLite tables only."""
    source = root / "state" / "motherlode.sqlite"
    if not source.exists():
        raise FileNotFoundError(f"canonical state database is unavailable: {source}")
    conn = sqlite3.connect(source); ensure_feature_schema(conn)
    destination = root / "features" / extractor_id; destination.mkdir(parents=True, exist_ok=True)
    out = sqlite3.connect(destination / "features.sqlite")
    out.executescript("""create table if not exists feature_runs(extractor_id text primary key, canonical_schema text not null, created_at real not null);
    create table if not exists bar_features(
      extractor_id text not null, stream_id text not null, bar_index integer not null,
      note_count integer not null, onset_count integer not null, notes_per_beat real not null,
      onsets_per_beat real not null, mean_polyphony real not null, max_polyphony integer not null,
      occupied_fraction real not null, rest_fraction real not null, median_duration real,
      median_pitch real, pitch_range integer, mean_velocity real,
      primary key(extractor_id,stream_id,bar_index));""")
    out.execute("insert or replace into feature_runs values(?,?,?)", (extractor_id, CANONICAL_EVENT_SCHEMA, time.time()))
    out.execute("delete from bar_features where extractor_id=?", (extractor_id,))
    count = 0
    for stream_id, bar_index, start, end, beats in conn.execute("select stream_id,bar_index,start_tick,end_tick,beats from canonical_bars order by stream_id,bar_index"):
        rows = conn.execute("select onset_tick,duration_ticks,end_tick,pitch,velocity from canonical_notes where stream_id=?", (stream_id,)).fetchall()
        onsets = [row for row in rows if start <= row[0] < end]
        boundaries: list[tuple[int, int]] = []
        for onset, _duration, note_end, _pitch, _velocity in rows:
            left, right = max(start, onset), min(end, note_end)
            if left < right: boundaries.extend(((left, 1), (right, -1)))
        active = maximum = occupied = 0; previous = start
        for tick, delta in sorted(boundaries, key=lambda pair: (pair[0], pair[1])):
            if tick > previous:
                occupied += (tick - previous) if active else 0
                previous = tick
            active += delta; maximum = max(maximum, active)
        if previous < end and active: occupied += end - previous
        durations = [row[1] for row in onsets]; pitches = [row[3] for row in onsets]; velocities = [row[4] for row in onsets]
        note_count = len(onsets); fraction = occupied / (end - start)
        out.execute("insert into bar_features values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            extractor_id, stream_id, bar_index, note_count, note_count, note_count / beats, note_count / beats,
            sum(0 for _ in []) if False else sum(max(0, min(end, nend) - max(start, onset)) for onset, _d, nend, _p, _v in rows) / (end - start),
            maximum, fraction, 1 - fraction, statistics.median(durations) if durations else None,
            statistics.median(pitches) if pitches else None, max(pitches) - min(pitches) if pitches else None,
            statistics.mean(velocities) if velocities else None,
        )); count += 1
    out.commit(); out.close(); conn.close()
    manifest = {"extractor_id": extractor_id, "canonical_schema": CANONICAL_EVENT_SCHEMA, "bar_rows": count, "used_raw_midi": False, "used_pertok_decode": False, "used_brick3": False}
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest
