"""Authority-gated builder for the V2 preview derived sidecar.

The builder accepts an already-frozen V1 snapshot only after checking its
known identities. It never creates token arrays, chooses membership, or writes
to canonical SQLite.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .v2_artifacts import canonical_identity, open_canonical_read_only, write_json
from .v2_features import extract_rows, write_feature_view
from .v2_projection import project_stream, write_projection


EXPECTED_ROOT_MANIFEST_SHA256 = "229709599f48b9fbd69fd0529db4b6d605ae2c46b30d778c43095ce909e3d4f3"
EXPECTED_CORPUS_MANIFEST_SHA256 = "28b1380b2ec712470f28c27ffc0d594c94b2d810153d99f6525ba1036f4ce804"
EXPECTED_ARRAY_SHAPE = (1848681, 4, 192)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_base_snapshot(snapshot: str | Path) -> dict[str, Any]:
    """Fail closed unless the exact frozen V1 preview payload is present."""
    root = Path(snapshot)
    root_manifest = root / "manifest.json"
    corpus_manifest = root / "canonical" / "manifest.json"
    arrays = (root / "training" / "input_ids.npy", root / "training" / "active_mask.npy")
    missing = [str(path) for path in (root_manifest, corpus_manifest, *arrays) if not path.is_file()]
    if missing:
        raise FileNotFoundError("V1 authority is incomplete: " + ", ".join(missing))
    root_hash = _sha256(root_manifest)
    corpus_hash = _sha256(corpus_manifest)
    if root_hash != EXPECTED_ROOT_MANIFEST_SHA256:
        raise ValueError(f"V1 root manifest identity mismatch: {root_hash}")
    if corpus_hash != EXPECTED_CORPUS_MANIFEST_SHA256:
        raise ValueError(f"V1 corpus manifest identity mismatch: {corpus_hash}")
    try:
        import numpy as np
    except ImportError as exc:
        raise RuntimeError("numpy is required to validate frozen packed arrays") from exc
    shapes = [tuple(np.load(path, mmap_mode="r").shape) for path in arrays]
    if any(shape != EXPECTED_ARRAY_SHAPE for shape in shapes):
        raise ValueError(f"V1 packed array shape mismatch: {shapes}")
    return {"snapshot_name": root.name, "path": str(root),
            "root_manifest_sha256": root_hash, "corpus_manifest_sha256": corpus_hash,
            "array_shapes": [list(shape) for shape in shapes]}


def build_derived_sidecar(*, base_snapshot: str | Path, canonical_db: str | Path,
                          output_dir: str | Path, stream_splits: dict[str, str] | None = None) -> dict[str, Any]:
    """Build projection/features only after exact V1 authority validation."""
    base = validate_base_snapshot(base_snapshot)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    conn = open_canonical_read_only(canonical_db)
    rows = []
    segments = []
    for (stream_id,) in conn.execute("select stream_id from canonical_streams order by stream_id"):
        projected, found = project_stream(conn, str(stream_id))
        rows.extend(projected); segments.extend(found)
    feature_rows = extract_rows(conn, split_by_stream=stream_splits)
    conn.close()
    projection = write_projection(rows, segments, output / "projection",
                                  canonical_identity=canonical_identity(canonical_db))
    features = write_feature_view(feature_rows, output / "features")
    write_json(output / "base-snapshot.json", base)
    manifest = {"schema": "everbar-motherlode.ev2-preview-live-30k-sidecar/v1",
                "base_snapshot": base, "canonical": canonical_identity(canonical_db),
                "projection_manifest_sha256": projection["manifest_sha256"],
                "features_manifest_sha256": features["manifest_sha256"],
                "conditioning_schema": None, "production": False}
    manifest["manifest_sha256"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    write_json(output / "manifest.json", manifest)
    return manifest
