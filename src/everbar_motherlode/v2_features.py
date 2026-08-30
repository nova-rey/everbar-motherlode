"""Versioned V2 candidate features over canonical Motherlode SQLite.

This is intentionally a feature view: it never opens MIDI, calls PerTok, or
changes canonical records.  The values are candidates until characterization
accepts them for modeling.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Iterable, Sequence

from .v2_artifacts import write_artifact_manifest, write_json, write_jsonl


EXTRACTOR_SCHEMA = "everbar-motherlode.v2-candidate-features/v1"
CONTROL_NAMES = (
    "rhythmic_density", "polyphony", "occupancy", "articulation",
    "time_signature", "key_tonal_center", "pitch_trend", "contour_reversal",
    "interval_magnitude", "register", "pitch_span", "note_duration_tendency",
    "microtiming_grid_rigidity", "pitch_class_diversity", "lifecycle",
)


@dataclass(frozen=True)
class FeatureRow:
    stream_id: str
    bar_index: int
    source_piece_id: str | None
    source_track_id: str | None
    dataset_id: str
    split: str | None
    values: dict[str, Any]
    missing: dict[str, bool]
    confidence: dict[str, float | None]
    extractor_id: str = EXTRACTOR_SCHEMA

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _quantile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values); index = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return float(ordered[index])


def _theil_sen(points: Sequence[tuple[float, float]]) -> float | None:
    slopes = [(y2 - y1) / (x2 - x1) for i, (x1, y1) in enumerate(points)
              for x2, y2 in points[i + 1:] if x2 != x1]
    return median(slopes) if slopes else None


def _interval_stats(notes: Sequence[tuple[int, int, int, int]], start: int, end: int) -> tuple[float, int, float]:
    boundaries: list[tuple[int, int]] = []
    for onset, duration, _pitch, _velocity in notes:
        left, right = max(start, onset), min(end, onset + duration)
        if left < right:
            boundaries.extend(((left, 1), (right, -1)))
    active = max_active = occupied = polyphonic = 0
    previous = start
    for tick, delta in sorted(boundaries, key=lambda item: (item[0], item[1])):
        if tick > previous:
            if active:
                occupied += tick - previous
            if active >= 2:
                polyphonic += tick - previous
            previous = tick
            max_active = max(max_active, active)
        active += delta
        max_active = max(max_active, active)
    if previous < end and active:
        occupied += end - previous
    length = end - start
    return (occupied / length if length else 0.0, max_active, polyphonic / length if length else 0.0)


def _bar_values(notes: Sequence[tuple[int, int, int, int]], start: int, end: int, beats: float,
                numerator: int, denominator: int) -> dict[str, Any]:
    local = sorted((n for n in notes if start <= n[0] < end), key=lambda n: (n[0], n[2], n[3]))
    onsets = sorted({n[0] for n in local})
    occupied, maximum, polyphonic_fraction = _interval_stats(notes, start, end)
    sounding = [(max(start, n[0]), min(end, n[0] + n[1]), n[2]) for n in notes
                if n[0] < end and n[0] + n[1] > start]
    voices = sum((right - left) for left, right, _ in sounding) / (end - start) if end > start else 0.0
    durations = [n[1] for n in local]
    pitches = [n[2] for n in local]
    velocities = [n[3] for n in local]
    groups = [(tick, [n for n in local if n[0] == tick]) for tick in onsets]
    reduced = [(tick, median([n[2] for n in group])) for tick, group in groups]
    iois = [reduced[i + 1][0] - reduced[i][0] for i in range(len(reduced) - 1)]
    articulation = [(dur / ioi) for dur, ioi in zip(durations, iois) if ioi > 0]
    # A final onset has no fabricated following onset and is therefore omitted.
    sen_points = [(float(tick), float(pitch)) for tick, pitch in reduced]
    intervals = [abs(reduced[i + 1][1] - reduced[i][1]) for i in range(len(reduced) - 1)]
    directions = [0 if reduced[i + 1][1] == reduced[i][1] else (1 if reduced[i + 1][1] > reduced[i][1] else -1)
                  for i in range(len(reduced) - 1)]
    nonzero = [direction for direction in directions if direction]
    reversals = sum(a != b for a, b in zip(nonzero, nonzero[1:])) / max(1, len(nonzero) - 1)
    pcs = [pitch % 12 for pitch in pitches]
    pc_counts = {pc: pcs.count(pc) for pc in set(pcs)}
    pc_total = sum(pc_counts.values())
    entropy = -sum((count / pc_total) * math.log2(count / pc_total) for count in pc_counts.values()) if pc_total else None
    tonic = max(pc_counts, key=lambda pc: (-pc_counts[pc], pc)) if pc_counts else None
    sorted_pcs = sorted(pc_counts.values(), reverse=True)
    key_margin = ((sorted_pcs[0] - sorted_pcs[1]) / pc_total) if len(sorted_pcs) > 1 and pc_total else None
    grid = [abs((onset - start) % max(1, round(beats and (end - start) / beats)) ) for onset in onsets]
    overlap_count = sum(1 for i, left in enumerate(local) for right in local[i + 1:]
                        if left[0] < right[0] + right[1] and right[0] < left[0] + left[1])
    return {
        "rhythmic_density": len(onsets) / beats if beats else 0.0,
        "polyphony": {"time_weighted_voices": voices, "max_voices": maximum,
                      "polyphonic_time_fraction": polyphonic_fraction},
        "occupancy": occupied,
        "articulation": {"median_duration_to_next_onset": median(articulation) if articulation else None,
                          "durations": durations, "iois": iois, "overlap_count": overlap_count},
        "time_signature": {"numerator": numerator, "denominator": denominator},
        "key_tonal_center": {"tonic_pc": tonic, "mode": "UNKNOWN", "score_margin": key_margin,
                             "entropy": entropy, "evidence_count": len(pitches)},
        "pitch_trend": {"theil_sen": _theil_sen(sen_points), "first_last": (reduced[-1][1] - reduced[0][1]) if len(reduced) > 1 else None},
        "contour_reversal": reversals,
        "interval_magnitude": {"median_abs_interval": median(intervals) if intervals else None,
                                "p90_abs_interval": _quantile(intervals, .9)},
        "register": {"median_pitch": median(pitches) if pitches else None,
                      "duration_weighted_median_candidate": median(pitches) if pitches else None},
        "pitch_span": {"p90_p10": (_quantile(pitches, .9) - _quantile(pitches, .1)) if pitches else None,
                       "absolute": (max(pitches) - min(pitches)) if pitches else None},
        "note_duration_tendency": {"p25": _quantile(durations, .25), "median": median(durations) if durations else None,
                                    "p75": _quantile(durations, .75)},
        "microtiming_grid_rigidity": {"exact_grid_fraction": sum(value == 0 for value in grid) / len(grid) if grid else None,
                                       "residuals": grid, "declared_resolution_ticks": max(1, round((end - start) / max(1, beats)))},
        "pitch_class_diversity": {"unique_count": len(set(pcs)), "entropy": entropy},
    }


def extract_rows(conn: sqlite3.Connection, *, split_by_stream: dict[str, str] | None = None,
                 stream_ids: Sequence[str] | None = None) -> list[FeatureRow]:
    """Extract all candidate rows from canonical tables in deterministic order."""
    stream_rows = conn.execute("select stream_id,source_piece_id,source_track_id,dataset_id from canonical_streams order by stream_id").fetchall()
    if stream_ids is not None:
        selected = set(stream_ids)
        stream_rows = [row for row in stream_rows if str(row[0]) in selected]
    result: list[FeatureRow] = []
    for stream_id, piece, track, dataset in stream_rows:
        notes = conn.execute("select onset_tick,duration_ticks,pitch,velocity from canonical_notes where stream_id=? order by note_index", (stream_id,)).fetchall()
        bars = conn.execute(
            "select bar_index,start_tick,end_tick,numerator,denominator,beats,is_empty from canonical_bars "
            "where stream_id=? order by bar_index", (stream_id,)
        ).fetchall()
        cursor = 0
        active: list[tuple[int, int, int, int]] = []
        for bar, start, end, numerator, denominator, beats, _empty in bars:
            start, end = int(start), int(end)
            active = [note for note in active if note[0] + note[1] > start]
            while cursor < len(notes) and int(notes[cursor][0]) < end:
                active.append(tuple(int(value) for value in notes[cursor])); cursor += 1
            values = _bar_values(active, start, end, float(beats), int(numerator), int(denominator))
            values["lifecycle"] = {"source_bar_index": int(bar), "source_bar_count": 0,
                                    "source_position": None, "segment_id": None, "segment_position": None}
            missing = {name: values.get(name) is None for name in CONTROL_NAMES}
            confidence = {"key_tonal_center": min(1.0, len(notes) / 8), "lifecycle": None}
            result.append(FeatureRow(str(stream_id), int(bar), piece, track, str(dataset),
                                     (split_by_stream or {}).get(str(stream_id)), values, missing, confidence))
    # Fill source-compositional lifecycle denominator without changing row order.
    counts: dict[str, int] = {}
    for row in result: counts[row.stream_id] = max(counts.get(row.stream_id, 0), row.bar_index + 1)
    return [FeatureRow(r.stream_id, r.bar_index, r.source_piece_id, r.source_track_id, r.dataset_id, r.split,
                       {**r.values, "lifecycle": {**r.values["lifecycle"], "source_bar_count": counts[r.stream_id],
                                                    "source_position": (r.bar_index + .5) / counts[r.stream_id]}},
                       r.missing, r.confidence, r.extractor_id) for r in result]


def characterize(rows: Iterable[FeatureRow]) -> dict[str, Any]:
    """Return deterministic descriptive evidence and provisional statuses."""
    rows = list(rows); evidence: dict[str, Any] = {"schema": EXTRACTOR_SCHEMA, "row_count": len(rows), "controls": {}}
    for name in CONTROL_NAMES:
        present = [row.values[name] for row in rows if not row.missing.get(name)]
        numeric = [float(value) for value in present if isinstance(value, (int, float)) and math.isfinite(value)]
        evidence["controls"][name] = {"present": len(present), "missing": len(rows) - len(present),
                                       "mean": mean(numeric) if numeric else None,
                                       "std": pstdev(numeric) if len(numeric) > 1 else 0.0 if numeric else None,
                                       "p10": _quantile(numeric, .1), "p50": _quantile(numeric, .5),
                                       "p90": _quantile(numeric, .9), "status": "ACCEPTED_FOR_MODELING" if present else "DEFERRED"}
    evidence["hash"] = hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return evidence


def attach_projection(rows: Iterable[FeatureRow], projection_rows: Iterable[Any]) -> list[FeatureRow]:
    """Attach derived segment fields while retaining source-compositional position."""
    by_key = {(row.stream_id, row.bar_index): row for row in projection_rows}
    result = []
    for row in rows:
        projection = by_key.get((row.stream_id, row.bar_index))
        if projection is None:
            raise ValueError(f"projection is missing {row.stream_id}:{row.bar_index}")
        lifecycle = {**row.values["lifecycle"], "segment_id": projection.segment_id,
                     "segment_position": projection.segment_position}
        result.append(FeatureRow(row.stream_id, row.bar_index, row.source_piece_id, row.source_track_id,
                                 row.dataset_id, row.split, {**row.values, "lifecycle": lifecycle},
                                 row.missing, row.confidence, row.extractor_id))
    return result


def control_status(rows: Iterable[FeatureRow]) -> dict[str, Any]:
    """Return the stable status sidecar without changing candidate values."""
    report = characterize(rows)
    controls = {
        name: {
            "status": value["status"],
            "reason": "observed in synthetic or canonical-derived rows" if value["present"] else "no non-missing observations",
            "present": value["present"],
            "missing": value["missing"],
        }
        for name, value in report["controls"].items()
    }
    return {"schema": f"{EXTRACTOR_SCHEMA}#control-status", "controls": controls}


def _lifecycle_label(row: FeatureRow, policy: str) -> str:
    lifecycle = row.values.get("lifecycle", {})
    index = int(lifecycle.get("source_bar_index", row.bar_index))
    count = max(1, int(lifecycle.get("source_bar_count", row.bar_index + 1)))
    position = float(lifecycle.get("source_position") if lifecycle.get("source_position") is not None else (index + .5) / count)
    if policy == "fixed-bar":
        return "START" if index == 0 else "END" if index == count - 1 else "CRUISE"
    if policy == "fractional-position":
        return "START" if position < .25 else "END" if position >= .75 else "CRUISE"
    if policy == "asymmetric":
        return "START" if position < .20 else "END" if position >= .80 else "CRUISE"
    if policy == "hybrid":
        return "START" if index == 0 or position < .20 else "END" if index == count - 1 or position >= .80 else "CRUISE"
    raise ValueError(f"unknown lifecycle policy: {policy}")


def compare_lifecycle_policies(rows: Iterable[FeatureRow]) -> dict[str, Any]:
    """Compare unfrozen lifecycle label candidates and their row distributions."""
    ordered = list(rows)
    policies = {
        "fixed-bar": {"description": "first and last source bars", "boundaries": {"start_bars": 1, "end_bars": 1}},
        "fractional-position": {"description": "source position quartile boundaries", "boundaries": {"start_lt": .25, "end_gte": .75}},
        "asymmetric": {"description": "asymmetric source position boundaries", "boundaries": {"start_lt": .20, "end_gte": .80}},
        "hybrid": {"description": "first or last source bar, widened by asymmetric position", "boundaries": {"start_lt": .20, "end_gte": .80}},
    }
    numeric_controls = tuple(name for name in CONTROL_NAMES if name != "lifecycle")
    result: dict[str, Any] = {"schema": f"{EXTRACTOR_SCHEMA}#lifecycle-policy-comparison", "row_count": len(ordered), "policies": {}}
    for name, definition in policies.items():
        labels = [_lifecycle_label(row, name) for row in ordered]
        detail: dict[str, Any] = {**definition, "class_counts": {}, "source_dataset_counts": {}, "split_counts": {}, "numeric_means": {}}
        for label in ("START", "CRUISE", "END"):
            selected = [row for row, current in zip(ordered, labels) if current == label]
            detail["class_counts"][label] = len(selected)
            detail["numeric_means"][label] = {
                control: mean([float(row.values[control]) for row in selected if isinstance(row.values.get(control), (int, float))])
                if any(isinstance(row.values.get(control), (int, float)) for row in selected) else None
                for control in numeric_controls
            }
        for row, label in zip(ordered, labels):
            dataset = row.dataset_id
            split = row.split if row.split is not None else "UNASSIGNED"
            detail["source_dataset_counts"].setdefault(dataset, {key: 0 for key in ("START", "CRUISE", "END")})[label] += 1
            detail["split_counts"].setdefault(split, {key: 0 for key in ("START", "CRUISE", "END")})[label] += 1
        detail["boundary_rows"] = [
            {"stream_id": row.stream_id, "bar_index": row.bar_index, "label": label,
             "source_position": row.values["lifecycle"].get("source_position"),
             "segment_position": row.values["lifecycle"].get("segment_position")}
            for row, label in zip(ordered, labels)
            if row.values.get("lifecycle", {}).get("segment_position") == 0
            or row.values.get("lifecycle", {}).get("source_bar_index") in {
                0, int(row.values.get("lifecycle", {}).get("source_bar_count", 1)) - 1,
            }
        ]
        result["policies"][name] = detail
    result["comparison_hash"] = hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return result


# Short name for callers that treat this as a comparison rather than a report.
lifecycle_policy_comparison = compare_lifecycle_policies


def _write_primitives_sqlite(rows: Sequence[FeatureRow], path: Path) -> None:
    """Write a logical feature view with no wall-clock fields."""
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.execute("pragma journal_mode=delete")
    conn.executescript("""
      create table primitive_features(
        stream_id text not null, bar_index integer not null,
        source_piece_id text, source_track_id text, dataset_id text not null,
        split text, values_json text not null, missing_json text not null,
        confidence_json text not null, extractor_id text not null,
        primary key(stream_id, bar_index)
      );
    """)
    conn.executemany(
        "insert into primitive_features values(?,?,?,?,?,?,?,?,?,?)",
        [(row.stream_id, row.bar_index, row.source_piece_id, row.source_track_id, row.dataset_id, row.split,
          json.dumps(row.values, sort_keys=True, separators=(",", ":")),
          json.dumps(row.missing, sort_keys=True, separators=(",", ":")),
          json.dumps(row.confidence, sort_keys=True, separators=(",", ":")), row.extractor_id)
         for row in rows],
    )
    conn.commit()
    conn.execute("vacuum")
    conn.close()


def write_feature_view(rows: Iterable[FeatureRow], output_dir: Any) -> dict[str, Any]:
    """Persist replaceable feature sidecars with deterministic metadata."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ordered_rows = list(rows)
    ordered_rows.sort(key=lambda row: (row.stream_id, row.bar_index))
    ordered = [row.to_dict() for row in ordered_rows]
    features_path = output_dir / "features.jsonl"
    features_sha = write_jsonl(features_path, ordered)
    primitives_path = output_dir / "primitives.sqlite"
    _write_primitives_sqlite(ordered_rows, primitives_path)
    report = characterize(ordered_rows)
    status = control_status(ordered_rows)
    lifecycle = compare_lifecycle_policies(ordered_rows)
    characterization_path = output_dir / "characterization.json"
    status_path = output_dir / "control-status.json"
    lifecycle_path = output_dir / "lifecycle-policy-comparison.json"
    write_json(characterization_path, report)
    write_json(status_path, status)
    write_json(lifecycle_path, lifecycle)
    return write_artifact_manifest(
        output_dir, schema=EXTRACTOR_SCHEMA,
        files={"features": features_path, "primitives": primitives_path,
               "characterization": characterization_path, "control_status": status_path,
               "lifecycle_policy_comparison": lifecycle_path},
        metadata={"row_count": len(ordered_rows), "features_sha256": features_sha,
                  "characterization_sha256": report["hash"],
                  "control_status_sha256": status["schema"] and hashlib.sha256(
                      json.dumps(status, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                  "lifecycle_comparison_sha256": lifecycle["comparison_hash"],
                  "extractor_id": EXTRACTOR_SCHEMA, "canonical_read_only": True},
    )
