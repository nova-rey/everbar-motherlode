import pytest

from everbar_motherlode.v2_sidecar import build_derived_sidecar, validate_base_snapshot


def test_sidecar_authority_gate_rejects_missing_snapshot_before_output(tmp_path):
    with pytest.raises(FileNotFoundError, match="V1 authority is incomplete"):
        validate_base_snapshot(tmp_path / "missing-v1")


def test_sidecar_builder_does_not_create_output_when_authority_is_missing(tmp_path):
    output = tmp_path / "sidecar"
    with pytest.raises(FileNotFoundError):
        build_derived_sidecar(base_snapshot=tmp_path / "missing-v1",
                              canonical_db=tmp_path / "missing.sqlite", output_dir=output)
    assert not output.exists()
