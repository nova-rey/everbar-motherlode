"""Sustain-aware, read-only live-performer projection for V2.

The canonical SQLite tables remain the authority.  This module creates only
derived rows and never updates the input database.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .v2_artifacts import write_artifact_manifest, write_int64_npy, write_jsonl


PROJECTION_SCHEMA = "everbar-motherlode.v2-live-projection/v1"


@dataclass(frozen=True)
class LiveBar:
    stream_id: str
    bar_index: int
    start_tick: int
    end_tick: int
    occupied: bool
    segment_id: str | None
    segment_position: int | None
    source_bar_index: int
    source_bar_count: int
    source_position: float


@dataclass(frozen=True)
class LiveSegment:
    segment_id: str
    stream_id: str
    start_bar_index: int
    end_bar_index: int
    bar_count: int


def _occupied(rows: Sequence[tuple[int, int]], start: int, end: int) -> bool:
    """Return occupancy under [start,end), including cross-bar sustains."""
    return any(onset < end and note_end > start for onset, note_end in rows)


def project_stream(conn: sqlite3.Connection, stream_id: str) -> tuple[list[LiveBar], list[LiveSegment]]:
    """Project one canonical stream, preserving original bar positions."""
    bars = conn.execute(
        "select bar_index,start_tick,end_tick from canonical_bars "
        "where stream_id=? order by bar_index", (stream_id,)
    ).fetchall()
    notes = conn.execute(
        "select onset_tick,end_tick from canonical_notes where stream_id=?", (stream_id,)
    ).fetchall()
    count = len(bars)
    occupied = [_occupied(notes, int(start), int(end)) for _, start, end in bars]
    segments: list[LiveSegment] = []
    projected: list[LiveBar] = []
    i = 0
    while i < count:
        if not occupied[i]:
            projected.append(LiveBar(stream_id, int(bars[i][0]), int(bars[i][1]), int(bars[i][2]), False,
                                     None, None, int(bars[i][0]), count, (int(bars[i][0]) + .5) / count))
            i += 1
            continue
        start = i
        while i < count and occupied[i]:
            i += 1
        end = i - 1
        digest = hashlib.sha256(json.dumps(
            {"schema": PROJECTION_SCHEMA, "stream_id": stream_id,
             "start_bar_index": int(bars[start][0]), "end_bar_index": int(bars[end][0])},
            sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()[:24]
        segment_id = f"{stream_id}:{digest}"
        segment = LiveSegment(segment_id, stream_id, int(bars[start][0]), int(bars[end][0]), end - start + 1)
        segments.append(segment)
        for position, row_index in enumerate(range(start, end + 1)):
            bar_index, bar_start, bar_end = bars[row_index]
            projected.append(LiveBar(stream_id, int(bar_index), int(bar_start), int(bar_end), True,
                                     segment_id, position, int(bar_index), count, (int(bar_index) + .5) / count))
    projected.sort(key=lambda row: row.bar_index)
    return projected, segments


def eligible_four_span_windows(
    projected: Sequence[LiveBar],
    represented_spans: Sequence[Mapping[str, int]],
    *, window_size: int = 4,
) -> list[dict[str, object]]:
    """Keep represented windows fully contained in one live segment.

    ``represented_spans`` must contain exactly one row per represented span,
    with ``span_index``, ``bar_index``, ``start_tick``, and ``end_tick``.
    """
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    # A bar index is only unique within a stream.  Keep the legacy synthetic
    # single-stream input (which omits ``stream_id``) usable, but refuse to
    # guess when an omitted stream id would make a cross-stream match
    # ambiguous.
    by_key = {(row.stream_id, row.bar_index): row for row in projected}
    by_bar: dict[int, list[LiveBar]] = {}
    for row in projected:
        by_bar.setdefault(row.bar_index, []).append(row)
    spans = sorted(represented_spans, key=lambda row: int(row["span_index"]))
    result: list[dict[str, object]] = []
    for offset in range(max(0, len(spans) - window_size + 1)):
        window = spans[offset:offset + window_size]
        rows: list[LiveBar | None] = []
        for item in window:
            bar_index = int(item["bar_index"])
            stream_id = item.get("stream_id")
            if stream_id is not None:
                row = by_key.get((str(stream_id), bar_index))
            else:
                matches = by_bar.get(bar_index, [])
                row = matches[0] if len(matches) == 1 else None
            rows.append(row)
        if any(row is None or not row.occupied for row in rows):
            continue
        stream_ids = {row.stream_id for row in rows if row is not None}
        if len(stream_ids) != 1:
            continue
        segment_ids = {row.segment_id for row in rows}
        if len(segment_ids) != 1:
            continue
        result.append({
            "start_span_index": int(window[0]["span_index"]),
            "span_indices": [int(item["span_index"]) for item in window],
            "segment_id": rows[0].segment_id,
            "bar_indices": [row.bar_index for row in rows],
            "source_bar_indices": [row.source_bar_index for row in rows],
        })
    return result


def represented_span_rows(
    rows: Iterable[LiveBar], represented_spans: Iterable[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Normalize represented spans and bind them to projected bars.

    A caller with a frozen base view can provide its exact represented spans.
    The default is a canonical-bar mapping, useful for synthetic fixtures and
    for projection-only inspection before a base array is available.
    """
    projected = {(row.stream_id, row.bar_index): row for row in rows}
    if represented_spans is None:
        source = [
            {"stream_id": row.stream_id, "span_index": index, "bar_index": row.bar_index,
             "start_tick": row.start_tick, "end_tick": row.end_tick}
            for index, row in enumerate(sorted(projected.values(), key=lambda value: (value.stream_id, value.bar_index)))
        ]
    else:
        source = [dict(item) for item in represented_spans]
    result: list[dict[str, Any]] = []
    for item in sorted(source, key=lambda value: (str(value["stream_id"]), int(value["span_index"]))):
        key = (str(item["stream_id"]), int(item["bar_index"]))
        row = projected.get(key)
        if row is None:
            raise ValueError(f"represented span is missing projected bar {key[0]}:{key[1]}")
        result.append({
            "stream_id": row.stream_id,
            "span_index": int(item["span_index"]),
            "bar_index": row.bar_index,
            "start_tick": int(item.get("start_tick", row.start_tick)),
            "end_tick": int(item.get("end_tick", row.end_tick)),
            "occupied": row.occupied,
            "segment_id": row.segment_id,
            "segment_position": row.segment_position,
            "source_bar_index": row.source_bar_index,
            "source_bar_count": row.source_bar_count,
            "source_position": row.source_position,
        })
    return result


def write_projection(
    rows: Iterable[LiveBar], segments: Iterable[LiveSegment], output_dir: Path,
    *, represented_spans: Iterable[Mapping[str, Any]] | None = None,
    eligible_window_indices: Iterable[int] | None = None,
    canonical_identity: Mapping[str, Any] | None = None,
) -> dict[str, object]:
    """Write deterministic JSONL projection artifacts and a sidecar manifest."""
    output_dir.mkdir(parents=True, exist_ok=True)
    segment_rows = [asdict(row) for row in segments]
    bar_rows = [asdict(row) for row in rows]
    span_rows = represented_span_rows(
        [LiveBar(**row) for row in bar_rows], represented_spans,
    )
    segments_path = output_dir / "segments.jsonl"
    bars_path = output_dir / "bars.jsonl"
    spans_path = output_dir / "represented-span-map.jsonl"
    write_jsonl(segments_path, segment_rows)
    write_jsonl(bars_path, bar_rows)
    write_jsonl(spans_path, span_rows)
    files: dict[str, Path] = {
        "segments": segments_path, "bars": bars_path, "represented_span_map": spans_path,
    }
    if eligible_window_indices is not None:
        index_path = output_dir / "eligible-base-window-indices.npy"
        write_int64_npy(index_path, eligible_window_indices)
        files["eligible_base_window_indices"] = index_path
    manifest = write_artifact_manifest(
        output_dir, schema=PROJECTION_SCHEMA, files=files,
        metadata={
            "bar_count": len(bar_rows), "segment_count": len(segment_rows),
            "represented_span_count": len(span_rows),
            **({"canonical_identity": dict(canonical_identity)} if canonical_identity else {}),
        },
    )
    return manifest
