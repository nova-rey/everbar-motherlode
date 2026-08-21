import json, zipfile, sqlite3
from pathlib import Path
import pytest
from everbar_motherlode.core import config, init, preflight, stable, extract, db, progress, reconcile

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
