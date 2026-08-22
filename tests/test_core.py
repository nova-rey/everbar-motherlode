import io, json, sqlite3, tarfile, zipfile
from pathlib import Path
import pytest
from everbar_motherlode.core import config, init, preflight, stable, extract, db, performance_flattening_v1, progress, reconcile

def cfg(): return config(Path("configs/motherlode-v1.toml"))
def test_ids_are_machine_and_path_independent():
    assert stable("piece","x","bytes","inside.mid") == stable("piece","x","bytes","inside.mid")
    assert stable("piece","x","bytes","inside.mid") != stable("piece","x","bytes","else.mid")
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
