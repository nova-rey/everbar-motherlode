import json
from pathlib import Path

from everbar_motherlode.autopilot import Autopilot, classify_sources, safe_to_stage


def test_inventory_queues_only_remaining_automated_allowed_raw_sources():
    rows = classify_sources([
        {"id": "done", "training": "ALLOWED", "role": "raw", "method": "http"},
        {"id": "auto", "training": "ALLOWED", "role": "raw", "method": "http"},
        {"id": "manual", "training": "ALLOWED", "role": "raw", "method": "manual_gated"},
        {"id": "unknown", "training": "UNCLEAR", "role": "raw", "method": "http"},
        {"id": "overlay", "training": "NOT_APPLICABLE", "role": "overlay", "method": "manual_gated"},
    ], {"done"})
    assert {row["dataset_id"] for row in rows if row["disposition"] == "QUEUED"} == {"auto"}
    assert next(row for row in rows if row["dataset_id"] == "done")["disposition"] == "COMPLETED_SKIP"
    assert all(row["disposition"] == "GATED" for row in rows if row["dataset_id"] in {"manual", "unknown", "overlay"})


def test_storage_guard_reserves_extract_and_derivation_envelope(tmp_path: Path, monkeypatch):
    class Usage:
        free = 1000
    monkeypatch.setattr("everbar_motherlode.autopilot.shutil.disk_usage", lambda _: Usage())
    assert safe_to_stage(tmp_path, 100, 600, 3.0)[0] is True
    assert safe_to_stage(tmp_path, 200, 600, 3.0)[0] is False


def test_autopilot_inventory_writes_durable_completed_and_gate_records(tmp_path: Path, monkeypatch):
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"max_partition_workers": 2, "output_uri": "file:///durable", "run_id_prefix": "test",
                                "reserve_bytes": 1, "storage_envelope_multiplier": 1, "completed_source_ids": ["done"]}))
    cfg = tmp_path / "config.toml"; cfg.write_text('registry = "ignored"\n')
    monkeypatch.setattr("everbar_motherlode.autopilot.config", lambda _: {"registry": "ignored"})
    monkeypatch.setattr("everbar_motherlode.autopilot.registry", lambda _: [
        {"id": "done", "training": "ALLOWED", "role": "raw", "method": "http"},
        {"id": "run", "training": "ALLOWED", "role": "raw", "method": "http"},
        {"id": "gate", "training": "UNCLEAR", "role": "raw", "method": "manual_gated"},
    ])
    runner = Autopilot(root=tmp_path / "root", config_path=cfg, plan_path=plan)
    rows = runner.inventory()
    assert [row["dataset_id"] for row in rows if row["disposition"] == "QUEUED"] == ["run"]
    assert json.loads((tmp_path / "root" / "progress" / "remaining-sources-gates.json").read_text())["gates"][0]["dataset_id"] == "gate"
