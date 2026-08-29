from __future__ import annotations

import sqlite3

from everbar_motherlode.feature_base import ensure_feature_schema
from everbar_motherlode.v2_features import CONTROL_NAMES, characterize, extract_rows
from everbar_motherlode.v2_projection import eligible_four_span_windows, project_stream


def _db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    ensure_feature_schema(db)
    db.execute("insert into canonical_streams values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
        "s", "hash", "ds", "v", "piece", "track", "[]", "[]", 0, "piano", "melody", "{}", "r", "e", "policy", "p", "pertok-v1", "l", "schema", 480, 4, 4, 100000, 1920,
    ))
    # Four bars. The note beginning in bar 0 sustains into bar 1; bar 2 is
    # empty; bar 3 has a note. Occupancy must not use onset-only membership.
    notes = [("s", 0, 0, 2400, 2400, 60, 80, 0, 1, 0), ("s", 1, 2400, 240, 2640, 64, 80, 1, 1, 480), ("s", 2, 6240, 240, 6480, 67, 80, 3, 3, 480)]
    db.executemany("insert into canonical_notes values(?,?,?,?,?,?,?,?,?,?)", notes)
    bars = [("s", 0, 0, 1920, 4, 4, 4.0, 0), ("s", 1, 1920, 3840, 4, 4, 4.0, 1), ("s", 2, 3840, 5760, 4, 4, 4.0, 1), ("s", 3, 5760, 7680, 4, 4, 4.0, 0)]
    db.executemany("insert into canonical_bars values(?,?,?,?,?,?,?,?)", bars)
    db.commit()
    return db


def test_projection_uses_half_open_sustain_occupancy_and_original_position():
    db = _db()
    rows, segments = project_stream(db, "s")
    assert [row.occupied for row in rows] == [True, True, False, True]
    assert len(segments) == 2
    assert rows[3].source_position == 0.875
    assert rows[3].segment_position == 0


def test_projection_requires_one_live_segment_for_four_span_window():
    db = _db(); rows, _ = project_stream(db, "s")
    spans = [{"span_index": i, "bar_index": i, "start_tick": i * 1920, "end_tick": (i + 1) * 1920} for i in range(4)]
    assert eligible_four_span_windows(rows, spans) == []


def test_candidate_features_are_versioned_and_characterizable():
    db = _db(); rows = extract_rows(db, split_by_stream={"s": "train"})
    assert len(rows) == 4
    assert rows[0].values["occupancy"] == 1.0
    assert set(rows[0].values) >= set(CONTROL_NAMES)
    report = characterize(rows)
    assert report["row_count"] == 4
    assert report["controls"]["rhythmic_density"]["status"] == "ACCEPTED_FOR_MODELING"
    assert len(report["hash"]) == 64
