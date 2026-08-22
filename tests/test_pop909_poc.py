from everbar_motherlode.pop909_poc import CAPS, TRACK_LABELS, _cap_report, _track_inventory, split_for_source_piece


def test_pop909_track_inventory_preserves_label_and_sibling_ready_identity():
    import mido

    track = mido.MidiTrack([
        mido.MetaMessage("track_name", name="MELODY", time=0),
        mido.Message("program_change", channel=0, program=0, time=0),
        mido.Message("note_on", channel=0, note=60, velocity=90, time=0),
        mido.Message("note_off", channel=0, note=60, velocity=0, time=480),
    ])
    row = _track_inventory("piece", 1, track)
    assert row["pop909_track_label"] == "MELODY"
    assert row["source_native_role"] == "MELODY"
    assert row["has_notes"] and not row["is_drum"]
    assert row["source_track_id"]


def test_source_piece_split_is_deterministic_and_sibling_safe():
    piece = "source_piece_pop909_001"
    assert split_for_source_piece(piece) == split_for_source_piece(piece)
    assert all(split_for_source_piece(piece) == split_for_source_piece(piece) for _ in range(3))


def test_cap_profile_reports_all_required_candidates_and_poc_selection():
    records = []
    for index, label in enumerate(TRACK_LABELS):
        records.append({
            "candidate_id": f"candidate-{index}",
            "pop909_track_label": label,
            "tokenization": {"bar_ids": [[4, 10] + [20] * (index + 1), [4]]},
        })
    report = _cap_report(records)
    assert tuple(int(value) for value in report["candidate_caps"]) == CAPS
    assert report["selected_cap"] == 64
    assert report["status"] == "POC_ONLY_CAP_SELECTED"
    assert report["candidate_caps"]["64"]["track_label_breakdown"]["PIANO"]["streams"] == 1


def test_profile_scope_is_not_motherlode_production():
    report = _cap_report([])
    assert report["scope"] == "POP909_POC_ONLY"
    assert report["status"] == "NO_ACCEPTED_STREAMS"
