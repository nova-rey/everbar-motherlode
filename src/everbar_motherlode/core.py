from __future__ import annotations
import hashlib, json, os, shutil, sqlite3, subprocess, sys, time, tomllib, urllib.request, zipfile
from collections import Counter
from pathlib import Path
from typing import Any

STAGES = ["DISCOVERED","LICENSE_VERIFIED","DOWNLOAD_PENDING","DOWNLOADING","DOWNLOADED","HASH_VERIFIED","EXTRACTED","INDEXED","DERIVED","BRICK3_COMPLETE","FINGERPRINTED","DEDUPE_COMPLETE","OVERLAY_COMPLETE","PROFILE_COMPLETE","DONE"]
TERMINAL = {"FAILED","RESOURCE_PAUSED","GATED_USER_ACTION_REQUIRED"}
def sha(b: bytes|str) -> str: return hashlib.sha256(b if isinstance(b,bytes) else b.encode()).hexdigest()
def stable(kind: str, *parts: object) -> str: return f"{kind}_{sha(json.dumps(parts,sort_keys=True,separators=(',',':')))[:24]}"
def writej(path: Path, data: Any):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+".tmp"); tmp.write_text(json.dumps(data,indent=2,sort_keys=True)+"\n"); tmp.replace(path)
def config(path: Path) -> dict: return tomllib.loads(path.read_text())
def db(root: Path):
    p=root/"state"/"motherlode.sqlite"; p.parent.mkdir(parents=True,exist_ok=True); c=sqlite3.connect(p); c.execute("pragma journal_mode=WAL"); c.execute("create table if not exists datasets(id text primary key, state text, data text, updated real)"); c.execute("create table if not exists items(id text primary key,dataset_id text,state text,source_path text,canonical_hash text,raw_hash text,detail text)"); c.commit(); return c
def preflight(root:Path,cfg:dict)->dict:
    usage=shutil.disk_usage(root if root.exists() else root.parent); free=usage.free; ok=free>=int(cfg["min_free_bytes"])
    return {"root":str(root),"filesystem_capacity_bytes":usage.total,"disk_free_bytes":free,"reserve_bytes":int(cfg["min_free_bytes"]),"ok":ok,"estimated_raw_bytes":sum(x.get("estimate",0) for x in registry(cfg)),"estimated_extracted_bytes":sum(x.get("estimate",0)*2 for x in registry(cfg)),"estimated_working_bytes":sum(x.get("estimate",0)*3 for x in registry(cfg))}
def registry(cfg):
    p=Path(cfg["registry"])
    if not p.is_absolute(): p=Path.cwd()/p
    return json.loads(p.read_text())["sources"]
def init(root:Path,cfg:dict):
    for n in "raw extracted derived receipts canonical indexes overlays reports logs progress state".split(): (root/n).mkdir(parents=True,exist_ok=True)
    c=db(root)
    for s in registry(cfg):
        c.execute("insert or ignore into datasets values(?,?,?,?)",(s["id"],"DISCOVERED",json.dumps(s,sort_keys=True),time.time()))
    c.commit(); c.close(); writej(root/"progress"/"preflight.json",preflight(root,cfg))
def action(root:Path,s:dict,reason:str):
    p=root/"progress"/"user-actions.json"; a=json.loads(p.read_text()) if p.exists() else []
    if not any(x["dataset_id"]==s["id"] for x in a): a.append({"dataset_id":s["id"],"action":reason,"official_source":s["url"]})
    writej(p,a); (root/"progress"/"user-actions.md").write_text("# User actions\n\n"+"\n".join(f"- `{x['dataset_id']}`: {x['action']} — {x['official_source']}" for x in a)+"\n")
def download(root:Path,s:dict)->Path:
    dest=root/"raw"/s["id"]/(s["id"]+".download"); dest.parent.mkdir(parents=True,exist_ok=True); part=dest.with_suffix(".part")
    if dest.exists(): return dest
    for attempt in range(3):
        try:
            headers={}; offset=part.stat().st_size if part.exists() else 0
            if offset: headers["Range"]=f"bytes={offset}-"
            with urllib.request.urlopen(urllib.request.Request(s["url"],headers=headers),timeout=60) as r, part.open("ab" if offset and r.status==206 else "wb") as f:
                while chunk:=r.read(1024*1024): f.write(chunk)
            part.replace(dest); return dest
        except Exception:
            if attempt==2: raise
            time.sleep(2**attempt)
    raise RuntimeError("unreachable")
def extract(root:Path,s:dict,artifact:Path)->Path:
    out=root/"extracted"/s["id"]; marker=out/".complete"
    if marker.exists(): return out
    out.mkdir(parents=True,exist_ok=True)
    if zipfile.is_zipfile(artifact):
        with zipfile.ZipFile(artifact) as z:
            for m in z.infolist():
                target=(out/m.filename).resolve()
                if not target.is_relative_to(out.resolve()): raise ValueError("unsafe archive member")
            z.extractall(out)
    else: shutil.copy2(artifact,out/artifact.name)
    marker.write_text(sha(artifact.read_bytes())+"\n"); return out
def midi_files(folder:Path): return sorted(p for p in folder.rglob("*") if p.suffix.lower() in {".mid",".midi"})
def derive(root:Path,c,ds:dict,folder:Path,cfg:dict):
    import mido
    result={"pieces":0,"tracks":0,"candidates":0,"accepts":0,"rejects":0}
    for p in midi_files(folder):
        raw=p.read_bytes(); rawid=stable("artifact",ds["id"],sha(raw)); piece=stable("piece",ds["id"],rawid,p.relative_to(folder).as_posix()); result["pieces"]+=1
        try: mid=mido.MidiFile(p)
        except Exception: continue
        for ti,track in enumerate(mid.tracks):
            notes=[]; programs=[]; drum=False; name=""
            for msg in track:
                if msg.type=="track_name": name=msg.name
                if msg.type=="program_change": programs.append(msg.program)
                if hasattr(msg,"channel") and msg.channel==9: drum=True
                if msg.type in {"note_on","note_off"}: notes.append(msg)
            result["tracks"]+=1
            trackid=stable("track",piece,ti,name,programs,drum)
            if drum or not notes: continue
            cand=stable("v1",trackid,sha(raw)); out=root/"derived"/ds["id"]/(cand+".mid"); out.parent.mkdir(parents=True,exist_ok=True)
            one=mido.MidiFile(type=1,ticks_per_beat=mid.ticks_per_beat); one.tracks.append(track.copy()); one.save(out)
            c.execute("insert or replace into items values(?,?,?,?,?,?,?)",(cand,ds["id"],"DERIVED",str(out),None,sha(raw),json.dumps({"source_piece_id":piece,"source_track_id":trackid,"sibling_track_ids":[stable('track',piece,x,'',[],False) for x in range(len(mid.tracks)) if x!=ti],"programs":programs,"is_drum":False,"source_track_name":name})))
            result["candidates"]+=1
            # Exact pinned boundary: upstream process owns acceptance semantics.
            cmd=["uv","run","--directory",cfg["everbar_checkout"],"everbar-inspect-midi",str(out),"--root",str(out.parent),"--corpus-id",ds["id"]]
            run=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
            if run.returncode==0 and run.stdout.strip():
                receipt=json.loads(run.stdout.splitlines()[-1]); canonical=receipt.get("canonical_score_sha256") or ((receipt.get("canonical") or {}).get("sha256"))
                c.execute("update items set state=?,canonical_hash=?,detail=? where id=?",("BRICK3_COMPLETE",canonical,json.dumps({"brick3":"ACCEPT","everbar_sha":cfg["everbar_sha"],"receipt":receipt}),cand)); result["accepts"]+=1
            else:
                c.execute("update items set state=?,detail=? where id=?",("BRICK3_COMPLETE",json.dumps({"brick3":"REJECT","everbar_sha":cfg["everbar_sha"],"diagnostics":run.stderr[-2000:]}),cand)); result["rejects"]+=1
    return result
def progress(root:Path,cfg:dict,state="RUNNING",stage="DISCOVERY"):
    c=db(root); datasets=c.execute("select state,data from datasets").fetchall(); items=c.execute("select state,canonical_hash from items").fetchall(); c.close(); raw=sum(p.stat().st_size for p in (root/"raw").rglob("*") if p.is_file()); starts=(root/"state"/"started"); elapsed=max(1,time.time()-float(starts.read_text())) if starts.exists() else 1
    accepted=sum(1 for _,h in items if h); output={"state":state,"current_stage":stage,"datasets_total":len(datasets),"datasets_complete":sum(x[0]=="DONE" for x in datasets),"datasets_started":sum(x[0] not in {"DISCOVERED","GATED_USER_ACTION_REQUIRED"} for x in datasets),"bytes_downloaded":raw,"source_pieces_indexed":len(items),"derived_streams":len(items),"brick3_accepts":accepted,"brick3_rejects":sum(1 for x,_ in items if x=="BRICK3_COMPLETE")-accepted,"canonical_duplicates":max(0,accepted-len({h for _,h in items if h})),"disk_free_bytes":shutil.disk_usage(root).free,"throughput":{"bytes_per_second":raw/elapsed,"streams_per_second":len(items)/elapsed},"eta":{"status":"ESTIMATING" if len(items)>=2 else "INSUFFICIENT_DATA","best_seconds":None,"likely_seconds":None,"worst_seconds":None}}
    writej(root/"progress"/"current.json",output); return output
def reports(root:Path,cfg:dict):
    c=db(root); rows=c.execute("select data,state from datasets").fetchall(); c.close(); src=[(json.loads(d),s) for d,s in rows]
    for name,body in {"licenses.md":"# Licenses\n\n"+"\n".join(f"- **{x['name']}**: {x['license']} / training {x['training']}" for x,_ in src),"acquisition.md":"# Acquisition\n\n"+"\n".join(f"- `{x['id']}`: {s}" for x,s in src),"attribution.md":"# Build attribution\n\n"+"\n".join(f"- {x['name']}: {x['citation']}" for x,s in src if s=="DONE"),"dedupe.md":"# Dedupe\n\nTier 0 raw SHA-256; Tier 1 Brick-3 canonical identity; later tiers cluster rather than erase provenance.\n","brick3.md":"# Brick 3\n\nPinned upstream SHA: `"+cfg['everbar_sha']+"`.\n","profile.md":"# Profile evidence\n\nCaps 64, 96, 128, 160, 192, 256 are evidence only; no cap is frozen.\n","failures.md":"# Failures\n\nSee state database and user action queue.\n"}.items(): (root/"reports"/name).write_text(body)
def run(root:Path,cfg:dict):
    init(root,cfg); (root/"state"/"started").write_text(str(time.time())); pf=preflight(root,cfg)
    if not pf["ok"]: return progress(root,cfg,"RESOURCE_PAUSED","RESOURCE_GUARD")
    progress(root,cfg,"RUNNING","DISCOVERY")
    c=db(root)
    resource_paused=False
    for s in registry(cfg):
        if s["role"] in {"superseded","documented","overlay","synthetic"}: continue
        if s["training"]!="ALLOWED" or s["method"]=="manual_gated": c.execute("update datasets set state=?,updated=? where id=?",("GATED_USER_ACTION_REQUIRED",time.time(),s["id"])); action(root,s,"license review or manual terms/credential action required"); continue
        # Never schedule an estimate that would breach the configured reserve.
        if s["estimate"] and s["estimate"] > shutil.disk_usage(root).free-int(cfg["min_free_bytes"]):
            c.execute("update datasets set state=?,updated=? where id=?",("RESOURCE_PAUSED",time.time(),s["id"])); c.commit(); progress(root,cfg,"RESOURCE_PAUSED","RESOURCE_GUARD"); resource_paused=True; break
        try:
            c.execute("update datasets set state=?,updated=? where id=?",("DOWNLOADING",time.time(),s["id"])); c.commit(); art=download(root,s); c.execute("update datasets set state=?,updated=? where id=?",("HASH_VERIFIED",time.time(),s["id"])); folder=extract(root,s,art); derive(root,c,s,folder,cfg); c.execute("update datasets set state=?,updated=? where id=?",("DONE",time.time(),s["id"])); c.commit(); progress(root,cfg,"RUNNING","BRICK3")
        except Exception as e: c.execute("update datasets set state=?,updated=? where id=?",("FAILED",time.time(),s["id"])); action(root,s,"automatic acquisition failed: "+str(e)[:300]); c.commit()
    c.close(); reports(root,cfg); return progress(root,cfg,"RESOURCE_PAUSED" if resource_paused else "PARTIAL","RESOURCE_GUARD" if resource_paused else "REPORTING")
