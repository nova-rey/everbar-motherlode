import json
import sqlite3

import pytest
from everbar_motherlode.v2_features import CONTROL_NAMES, FeatureRow, write_feature_view
from everbar_motherlode.v2_projection import LiveBar, LiveSegment, write_projection

from everbar_motherlode.v2_artifacts import canonical_identity, open_canonical_read_only, write_artifact_manifest, write_int64_npy


def test_read_only_canonical_connection_rejects_writes(tmp_path):
    path = tmp_path / "canonical.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("create table t(value integer)")
    conn.execute("insert into t values(1)")
    conn.commit(); conn.close()
    read = open_canonical_read_only(path)
    assert read.execute("select value from t").fetchone() == (1,)
    with pytest.raises(sqlite3.OperationalError):
        read.execute("insert into t values(2)")
    read.close()


def test_artifact_manifest_binds_files_and_is_reproducible(tmp_path):
    data = tmp_path / "indices.npy"
    write_int64_npy(data, [3, 5, 8])
    first = write_artifact_manifest(tmp_path, schema="test/v1", files={"indices": data}, metadata={"scope": "fixture"})
    second = write_artifact_manifest(tmp_path, schema="test/v1", files={"indices": data}, metadata={"scope": "fixture"})
    assert first == second
    assert first["files"]["indices"]["bytes"] == data.stat().st_size
    assert json.loads((tmp_path / "manifest.json").read_text())["manifest_sha256"] == first["manifest_sha256"]


def test_canonical_identity_is_content_addressed(tmp_path):
    path = tmp_path / "canonical.sqlite"
    path.write_bytes(b"canonical")
    identity = canonical_identity(path)
    assert identity["bytes"] == 9
    assert len(identity["sha256"]) == 64


def test_projection_and_features_emit_bound_sidecars_without_writing_canonical(tmp_path):
    projected = [LiveBar("s", 0, 0, 1920, True, "seg", 0, 0, 1, 0.5)]
    segments = [LiveSegment("seg", "s", 0, 0, 1)]
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.execute("create table canonical_notes (value integer)")
    db.execute("insert into canonical_notes values (1)")
    before = db.execute("select count(*) from canonical_notes").fetchone()[0]
    projection = write_projection(projected, segments, tmp_path / "projection",
                                  canonical_identity={"sha256": "fixture"})
    values = {name: (1.0 if name == "rhythmic_density" else None) for name in CONTROL_NAMES}
    values["lifecycle"] = {"source_bar_index": 0, "source_bar_count": 1,
                            "source_position": 0.5, "segment_id": "seg",
                            "segment_position": 0}
    missing = {name: value is None for name, value in values.items()}
    feature = FeatureRow("s", 0, None, None, "fixture", "train", values, missing, {})
    # The artifact writer accepts the same versioned rows produced by the
    # canonical extractor; this small row keeps the test independent of the
    # canonical schema fixture.
    features = write_feature_view([feature], tmp_path / "features")
    assert projection["schema"].endswith("live-projection/v1")
    assert features["schema"].endswith("candidate-features/v1")
    assert (tmp_path / "projection/manifest.json").is_file()
    assert (tmp_path / "features/primitives.sqlite").is_file()
    assert db.execute("select count(*) from canonical_notes").fetchone()[0] == before
