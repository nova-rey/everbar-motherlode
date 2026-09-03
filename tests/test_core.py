import io, json, os, sqlite3, subprocess, tarfile, zipfile
from pathlib import Path
from types import SimpleNamespace
import pytest
from everbar_motherlode.core import config, init, partition_for, preflight, stable, extract, db, derive, performance_flattening_v1, progress, reconcile, shard, writej, _pdmx_partition_files, _partition_manifest_files, brick3_command
from everbar_motherlode.distributed import output_prefix, publish_shard, shard_label, stage_shard, verify_distributed_run
from everbar_motherlode.feature_base import backfill_canonical, extract_primitive_features

def cfg(): return config(Path("configs/motherlode-v1.toml"))
def test_ids_are_machine_and_path_independent():
    assert stable("piece","x","bytes","inside.mid") == stable("piece","x","bytes","inside.mid")
    assert stable("piece","x","bytes","inside.mid") != stable("piece","x","bytes","else.mid")
def test_partition_assignment_is_stable_and_complete():
    paths=[f"piece/{i}.mid" for i in range(100)]
    assignments=[partition_for("pdmx",path,3) for path in paths]
    assert assignments == [partition_for("pdmx",path,3) for path in paths]
    assert set(assignments) == {0,1,2}
def test_pdmx_partition_manifest_is_durable_and_exact(tmp_path, monkeypatch):
    root=tmp_path/"root"; folder=tmp_path/"extracted"; (folder/"mid"/"a").mkdir(parents=True)
    paths=[folder/"mid"/"a"/f"piece-{i}.mid" for i in range(8)]
    for path in paths: path.write_bytes(b"MThd")
    source={"id":"pdmx","subset_url":"unused"}
    monkeypatch.setattr("everbar_motherlode.core._pdmx_allowed_midi_paths",lambda *_:{p.relative_to(folder).as_posix() for p in paths})
    seen=[]
    for index in range(3):
        seen.extend(_pdmx_partition_files(root,source,folder,3,index))
    assert sorted(seen) == sorted(paths)
    manifest=root/"state"/"manifests"/"pdmx-partitions-00003"
    assert (manifest/"complete.json").exists()
    # Reload uses the durable manifest rather than rebuilding the official set.
    monkeypatch.setattr("everbar_motherlode.core._pdmx_allowed_midi_paths",lambda *_:pytest.fail("must not rebuild"))
    assert _pdmx_partition_files(root,source,folder,3,0) == [p for p in paths if partition_for("pdmx",p.relative_to(folder).as_posix(),3)==0]

def test_large_source_partition_manifest_is_durable_and_exact(tmp_path, monkeypatch):
    root=tmp_path/"root"; folder=tmp_path/"extracted"; (folder/"nested").mkdir(parents=True)
    paths=[folder/"nested"/f"piece-{i}.mid" for i in range(11)]
    for path in paths: path.write_bytes(b"MThd")
    source={"id":"gigamidi"}; seen=[]
    for index in range(4): seen.extend(_partition_manifest_files(root,source,folder,4,index))
    assert sorted(seen) == sorted(paths)
    manifest=root/"state"/"manifests"/"gigamidi-partitions-00004"
    assert (manifest/"complete.json").exists()
    monkeypatch.setattr("everbar_motherlode.core.midi_files",lambda *_:pytest.fail("must not rediscover"))
    assert _partition_manifest_files(root,source,folder,4,0) == [p for p in paths if partition_for("gigamidi",p.relative_to(folder).as_posix(),4)==0]

def test_direct_brick3_runner_is_checkout_bound_and_fails_closed(tmp_path):
    checkout=tmp_path/"everbar"; cli=checkout/".venv"/"bin"/"everbar-inspect-midi"; cli.parent.mkdir(parents=True); cli.write_text("#!/bin/sh\n"); cli.chmod(0o755)
    command=brick3_command({"everbar_checkout":str(checkout),"brick3_runner":"direct-venv"},tmp_path/"input.mid",tmp_path,"fixture")
    assert command == [str(cli),str(tmp_path/"input.mid"),"--root",str(tmp_path),"--corpus-id","fixture"]
    with pytest.raises(RuntimeError): brick3_command({"everbar_checkout":str(tmp_path/"missing"),"brick3_runner":"direct-venv"},tmp_path/"input.mid",tmp_path,"fixture")
def test_distributed_stage_is_run_scoped_and_retry_safe(tmp_path):
    c=cfg(); root=tmp_path/"root"; label=shard_label("pop909",1,2); shard_root=root/"state"/"shards"/label/"state"; shard_root.mkdir(parents=True)
    candidate=root/"derived"/"pop909"/"v1_x.mid"; candidate.parent.mkdir(parents=True); candidate.write_bytes(b"candidate")
    converted=root/"derived"/"pop909"/"prebrick3"/"v1_x.mid"; converted.parent.mkdir(parents=True); converted.write_bytes(b"converted")
    receipt=root/"receipts"/"conversion"/"v1_x.json"; writej(receipt,{"ok":True})
    d=db(root/"state"/"shards"/label); d.execute("insert into items values(?,?,?,?,?,?,?)",("v1_x","pop909","BRICK3_COMPLETE",str(candidate),"canonical","raw",json.dumps({"conversion":{"output_path":str(converted)}}))); d.commit(); d.close()
    writej(root/"progress"/"shards"/(label+".json"),{"state":"COMPLETE","dataset_id":"pop909","result":{"candidates":1}})
    stage,manifest=stage_shard(root,c,"pop909",1,2,"run-a",{"state":"COMPLETE"})
    assert manifest["item_count"] == 1 and (stage/"completion.json").exists()
    destination=tmp_path/"persistent"; published=publish_shard(stage,"file://"+str(destination),manifest)
    assert Path(published,"completion.json").exists() and output_prefix("run-a","pop909",1,2) in published
    with pytest.raises(FileExistsError): publish_shard(stage,"file://"+str(destination),manifest)
    incomplete=verify_distributed_run("file://"+str(destination),"run-a","pop909",2)
    assert incomplete["state"] == "INCOMPLETE" and incomplete["completed_shard_ids"] == [1]
    complete_root=tmp_path/"complete"/"runs"/"run-b"/"pop909"
    for index in (0,1):
        package=complete_root/f"shard-{index:05d}-of-00002"; writej(package/"completion.json",{"state":"COMPLETE","shard_index":index,"shard_count":2}); writej(package/"item-ids.json",{"item_ids":[f"v1_{index}"]})
    assert verify_distributed_run("file://"+str(tmp_path/"complete"),"run-b","pop909",2)["state"] == "COMPLETE"

def test_partition_worker_uses_distributed_publish_label(tmp_path, monkeypatch):
    root=tmp_path/"root"; (root/"raw"/"fixture").mkdir(parents=True)
    (root/"raw"/"fixture"/"fixture.download").write_bytes(b"archive")
    source={"id":"fixture","training":"ALLOWED","role":"raw"}
    monkeypatch.setattr("everbar_motherlode.core.registry",lambda cfg:[source])
    monkeypatch.setattr("everbar_motherlode.core.extract",lambda *args:tmp_path)
    monkeypatch.setattr("everbar_motherlode.core.derive",lambda *args,**kwargs:{"pieces":0,"tracks":0,"candidates":0,"accepts":0,"rejects":0})
    shard(root,{"registry":"ignored"},["fixture"],1,2)
    label=shard_label("fixture",1,2)
    assert (root/"state"/"shards"/label/"state"/"motherlode.sqlite").exists()
    assert (root/"progress"/"shards"/(label+".json")).exists()
def test_single_and_two_shards_have_identical_candidate_coverage(tmp_path, monkeypatch):
    import mido
    folder=tmp_path/"source"; folder.mkdir()
    for index,note in enumerate((60,64,67,72)):
        midi=mido.MidiFile(); track=mido.MidiTrack(); midi.tracks.append(track)
        track.extend([mido.Message("program_change",program=0,time=0),mido.Message("note_on",note=note,velocity=80,time=0),mido.Message("note_off",note=note,velocity=0,time=120)])
        midi.save(folder/f"piece-{index}.mid")
    monkeypatch.setattr("everbar_motherlode.core.subprocess.run",lambda *args,**kwargs: SimpleNamespace(returncode=0,stdout=json.dumps({"canonical":{"event_sha256":"fixture"}}),stderr=""))
    source={"id":"fixture","training":"ALLOWED","role":"raw"}; runtime_cfg={"everbar_sha":"fixture","everbar_checkout":"/fixture"}
    single=tmp_path/"single"; c=db(single); derive(single,c,source,folder,runtime_cfg); c.commit(); one={r[0] for r in c.execute("select id from items")}; c.close()
    sharded=tmp_path/"sharded"; two=set()
    for index in (0,1):
        c=db(sharded/"state"/"shards"/str(index)); derive(sharded,c,source,folder,runtime_cfg,index,2); c.commit(); two.update(r[0] for r in c.execute("select id from items")); c.close()
    single_bytes={p.name:p.read_bytes() for p in (single/"derived"/"fixture").glob("*.mid")}
    sharded_bytes={p.name:p.read_bytes() for p in (sharded/"derived"/"fixture").glob("*.mid")}
    single_converted={p.name:p.read_bytes() for p in (single/"derived"/"fixture"/"prebrick3").glob("*.mid")}
    sharded_converted={p.name:p.read_bytes() for p in (sharded/"derived"/"fixture"/"prebrick3").glob("*.mid")}
    assert one == two and len(one) == 4 and single_bytes == sharded_bytes and single_converted == sharded_converted

def test_v1_accept_keeps_v2_piece_siblings_and_drum_inventory(tmp_path, monkeypatch):
    import mido
    folder=tmp_path/"source"; folder.mkdir(); midi=mido.MidiFile()
    lead=mido.MidiTrack(); drums=mido.MidiTrack(); midi.tracks.extend([lead,drums])
    lead.extend([mido.MetaMessage("track_name",name="Lead",time=0),mido.Message("program_change",program=5,time=0),mido.Message("note_on",note=60,velocity=90,time=0),mido.Message("note_off",note=60,velocity=0,time=480)])
    drums.extend([mido.MetaMessage("track_name",name="Drums",time=0),mido.Message("note_on",channel=9,note=36,velocity=100,time=0),mido.Message("note_off",channel=9,note=36,velocity=0,time=480)])
    midi.save(folder/"piece.mid")
    receipt={"receipt_sha256":"r","canonical":{"event_sha256":"c","score":{"schema":"dreamstream-everbar.canonical-score/v1","tpq":480,"track":{"program":0,"is_drum":False,"notes":[[0,480,60,90]]},"tempo":[[0,500000]],"time_signature":[[0,4,4]]}}}
    monkeypatch.setattr("everbar_motherlode.core.subprocess.run",lambda *args,**kwargs: SimpleNamespace(returncode=0,stdout=json.dumps(receipt),stderr=""))
    root=tmp_path/"root"; c=db(root); derive(root,c,{"id":"fixture","version":"1","training":"ALLOWED","role":"raw"},folder,{"everbar_sha":"fixture","everbar_checkout":"/fixture"}); c.commit()
    detail=json.loads(c.execute("select detail from items").fetchone()[0]); siblings=detail["provenance"]["sibling_track_ids"]
    assert len(siblings) == 1 and c.execute("select is_drum,source_track_name from source_tracks where source_track_id=?",(siblings[0],)).fetchone() == (1,"Drums")
    assert c.execute("select count(*) from source_pieces").fetchone()[0] == 1
    c.close()
def test_registry_and_license_persist(tmp_path):
    init(tmp_path,cfg()); c=db(tmp_path); n=c.execute("select count(*) from datasets").fetchone()[0]; c.close()
    assert n >= 40
    assert (tmp_path/"progress"/"preflight.json").exists()
def test_safe_extraction_rejects_traversal(tmp_path):
    z=tmp_path/"bad.zip"
    with zipfile.ZipFile(z,"w") as f: f.writestr("../escape.mid",b"no")
    with pytest.raises(ValueError): extract(tmp_path,{"id":"bad"},z)
    tar=tmp_path/"bad.tar"
    with tarfile.open(tar,"w") as archive:
        info=tarfile.TarInfo("../escape.mid"); info.size=2
        archive.addfile(info,io.BytesIO(b"no"))
    with pytest.raises(ValueError): extract(tmp_path,{"id":"bad-tar"},tar)
def test_gigamidi_nested_zip_extraction_is_resumable(tmp_path):
    inner=io.BytesIO()
    with zipfile.ZipFile(inner,"w") as z: z.writestr("training/example.mid",b"MThd")
    outer=tmp_path/"giga.zip"
    with zipfile.ZipFile(outer,"w") as z: z.writestr("Final/training.zip",inner.getvalue())
    out=extract(tmp_path,{"id":"gigamidi"},outer)
    assert (out/"Final"/"training"/"example.mid").read_bytes() == b"MThd"
    assert (out/".gigamidi-nested-complete").exists()
    assert extract(tmp_path,{"id":"gigamidi"},outer) == out

def test_gigamidi_nested_extraction_skips_only_appledouble_zip_sidecars(tmp_path):
    inner=io.BytesIO()
    with zipfile.ZipFile(inner,"w") as z: z.writestr("piece.mid",b"MThd")
    outer=tmp_path/"giga.zip"
    with zipfile.ZipFile(outer,"w") as z:
        z.writestr("dataset/training.zip",inner.getvalue())
        z.writestr("dataset/._training.zip",b"appledouble-not-a-zip")
    out=extract(tmp_path,{"id":"gigamidi"},outer)
    assert (out/"dataset"/"piece.mid").read_bytes() == b"MThd"
    assert (out/"dataset"/"._training.zip.expanded").read_text() == "APPLEDOUBLE_SIDECAR\n"
def test_progress_and_disk_guard(tmp_path):
    c=cfg(); c["min_free_bytes"]=10**30
    assert not preflight(tmp_path,c)["ok"]
    init(tmp_path,cfg()); r=progress(tmp_path,cfg())
    assert r["eta"]["status"] == "INSUFFICIENT_DATA"
def test_reconcile_uses_everbar_event_identity(tmp_path):
    init(tmp_path,cfg()); c=db(tmp_path)
    c.execute("insert into items values(?,?,?,?,?,?,?)",("v1_x","pop909","BRICK3_COMPLETE","x.mid",None,"raw",json.dumps({"receipt":{"canonical":{"event_sha256":"event-hash"}}})))
    c.commit(); c.close(); reconcile(tmp_path,cfg())
    c=db(tmp_path); assert c.execute("select canonical_hash from items where id='v1_x'").fetchone()[0] == "event-hash"; c.close()

def test_canonical_event_backfill_and_features_need_no_midi_or_brick3(tmp_path):
    root=tmp_path/"root"; c=db(root)
    receipt={"receipt_sha256":"receipt","policy_id":"corpus-policy-v1","policy_sha256":"policy","language_id":"pertok-v1","language_sha256":"language","canonical":{"event_sha256":"canonical","score":{"schema":"dreamstream-everbar.canonical-score/v1","tpq":480,"track":{"program":0,"is_drum":False,"notes":[[0,2400,60,90],[0,480,60,80],[6000,480,72,100]]},"tempo":[[0,500000]],"time_signature":[[0,4,4]]}}}
    detail={"brick3":"ACCEPT","everbar_sha":"everbar","receipt":receipt,"provenance":{"dataset_version":"fixture-v1","source_piece_id":"piece","source_track_id":"track","sibling_track_ids":["drums"],"programs":[5],"is_drum":False,"source_track_name":"Lead","source_native_role":"melody","source_timing":{"source_tpq":960}}}
    c.execute("insert into items values(?,?,?,?,?,?,?)",("stream","fixture","BRICK3_COMPLETE","missing.mid","canonical","raw",json.dumps(detail))); c.commit(); c.close()
    report=backfill_canonical(root)
    assert report["materialized_streams"] == 1 and not report["used_raw_midi"] and not report["used_brick3"]
    c=sqlite3.connect(root/"state"/"motherlode.sqlite")
    notes=c.execute("select onset_tick,duration_ticks,pitch,velocity,onset_bar_index,end_bar_index from canonical_notes order by note_index").fetchall()
    assert notes[:2] == [(0,2400,60,90,0,1),(0,480,60,80,0,0)]  # same-pitch overlap + cross-bar duration
    assert c.execute("select is_empty from canonical_bars where bar_index=2").fetchone()[0] == 1
    assert c.execute("select source_piece_id,source_track_id,sibling_track_ids_json from canonical_streams").fetchone() == ("piece","track",'["drums"]')
    c.close()
    first=extract_primitive_features(root,"primitive-v1")
    second=extract_primitive_features(root,"primitive-v2")
    assert first["bar_rows"] == 4 and second["bar_rows"] == 4
    f=sqlite3.connect(root/"features"/"primitive-v1"/"features.sqlite")
    assert f.execute("select max_polyphony,round(mean_polyphony,2),round(occupied_fraction,2),round(rest_fraction,2) from bar_features where bar_index=0").fetchone() == (2,1.25,1.0,0.0)
    assert f.execute("select note_count,onset_count,occupied_fraction,rest_fraction from bar_features where bar_index=2").fetchone() == (0,0,0.0,1.0)
    f.close()
def test_performance_flattening_renders_pedals_and_drops_noops():
    import mido
    source=mido.MidiFile(); track=mido.MidiTrack(); source.tracks.append(track)
    track.extend([
        mido.Message("control_change",channel=0,control=64,value=127,time=0),
        mido.Message("note_on",channel=0,note=60,velocity=90,time=0),
        mido.Message("note_off",channel=0,note=60,velocity=0,time=10),
        mido.Message("control_change",channel=0,control=64,value=0,time=10),
        mido.Message("note_on",channel=0,note=62,velocity=90,time=0),
        mido.Message("control_change",channel=0,control=66,value=127,time=5),
        mido.Message("note_off",channel=0,note=62,velocity=0,time=10),
        mido.Message("control_change",channel=0,control=66,value=0,time=10),
        mido.Message("control_change",channel=0,control=67,value=127,time=0),
        mido.Message("note_on",channel=0,note=70,velocity=90,time=0),
        mido.Message("note_off",channel=0,note=70,velocity=0,time=0),
    ])
    flattened,counts=performance_flattening_v1(source)
    absolute=0; events=[]
    for msg in flattened.tracks[0]: absolute+=msg.time; events.append((absolute,msg))
    offs=[(tick,msg.note) for tick,msg in events if msg.type=="note_off"]
    assert (20,60) in offs and (45,62) in offs
    assert not any(getattr(msg,"control",None) in {64,66,67} for _,msg in events)
    assert not any(getattr(msg,"note",None)==70 for _,msg in events)
    assert counts == {"cc64_rendered":2,"cc66_rendered":2,"cc67_discarded":1,"cc121_resets_consumed":0,"zero_duration_notes_dropped":1,"end_of_track_noteoffs":0}
def test_performance_flattening_consumes_cc121_at_exact_tick():
    import mido
    source=mido.MidiFile(); track=mido.MidiTrack(); source.tracks.append(track)
    track.extend([
        mido.Message("control_change",channel=0,control=64,value=127,time=0),
        mido.Message("note_on",channel=0,note=60,velocity=90,time=0),
        mido.Message("note_off",channel=0,note=60,velocity=0,time=10),
        mido.Message("control_change",channel=0,control=121,value=0,time=5),
        mido.Message("note_on",channel=0,note=62,velocity=90,time=0),
        mido.Message("control_change",channel=0,control=66,value=127,time=0),
        mido.Message("note_off",channel=0,note=62,velocity=0,time=10),
        mido.Message("control_change",channel=0,control=121,value=0,time=5),
    ])
    flattened,counts=performance_flattening_v1(source)
    tick=0; offs=[]
    for msg in flattened.tracks[0]:
        tick+=msg.time
        if msg.type=="note_off": offs.append((tick,msg.note))
        assert not (msg.type=="control_change" and msg.control==121)
    assert offs == [(15,60),(30,62)]
    assert counts["cc121_resets_consumed"] == 2

def test_watchdog_escalates_only_for_a_debounced_fault(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"; fake_bin.mkdir()
    payload = tmp_path / "probe.json"; queue_log = tmp_path / "queue.log"
    ssh = fake_bin / "ssh"; ssh.write_text("#!/bin/sh\ncat \"$FAKE_SSH_PAYLOAD\"\n"); ssh.chmod(0o755)
    codex = fake_bin / "codex"; codex.write_text("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$FAKE_CODEX_LOG\"\n"); codex.chmod(0o755)
    config_file = tmp_path / "watchdog.env"
    config_file.write_text("\n".join((
        "CODEX_THREAD=fixture-thread", "MOTHERLODE_SSH=fixture@host", "MOTHERLODE_ROOT=/fixture/root",
        f"WATCHDOG_STATE_DIR={tmp_path / 'state'}", "MAX_PROGRESS_AGE_SECONDS=900",
        "NO_PROGRESS_GRACE_SECONDS=7200", "ALERT_COOLDOWN_SECONDS=14400", "",
    )))
    config_file.chmod(0o600)
    healthy = {"pids":{"queue-gigamidi-after-pdmx-chunks.pid":True,"monitor-pdmx-giga.pid":True,"pdmx-chunk-worker-0.pid":True},"progress_exists":True,"progress_age_seconds":1,"state":"RUNNING","stage":"PDMX_DERIVATION","converted_streams":12,"converted_by_dataset":{"pdmx":12}}
    payload.write_text(json.dumps(healthy))
    env = {**os.environ, "PATH":str(fake_bin) + os.pathsep + os.environ["PATH"], "FAKE_SSH_PAYLOAD":str(payload), "FAKE_CODEX_LOG":str(queue_log), "WATCHDOG_CONFIG":str(config_file)}
    command = ["bash", str(repo / "scripts" / "motherlode-watchdog.sh")]
    assert subprocess.run(command, env=env, check=False).returncode == 0
    assert not queue_log.exists()
    payload.write_text(json.dumps({"pids":{},"progress_exists":False,"state":"FAILED","converted_streams":12}))
    assert subprocess.run(command, env=env, check=False).returncode == 0
    assert "queue --thread fixture-thread" in queue_log.read_text()
    assert subprocess.run(command, env=env, check=False).returncode == 0
    assert len(queue_log.read_text().splitlines()) == 1
