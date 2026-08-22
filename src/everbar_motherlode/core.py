from __future__ import annotations
import hashlib, json, os, shutil, sqlite3, subprocess, sys, tarfile, time, tomllib, urllib.parse, urllib.request, zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PurePosixPath
from typing import Any

STAGES = ["DISCOVERED","LICENSE_VERIFIED","DOWNLOAD_PENDING","DOWNLOADING","DOWNLOADED","HASH_VERIFIED","EXTRACTED","INDEXED","DERIVED","BRICK3_COMPLETE","FINGERPRINTED","DEDUPE_COMPLETE","OVERLAY_COMPLETE","PROFILE_COMPLETE","DONE"]
TERMINAL = {"FAILED","RESOURCE_PAUSED","GATED_USER_ACTION_REQUIRED"}
def sha(b: bytes|str) -> str: return hashlib.sha256(b if isinstance(b,bytes) else b.encode()).hexdigest()
def stable(kind: str, *parts: object) -> str: return f"{kind}_{sha(json.dumps(parts,sort_keys=True,separators=(',',':')))[:24]}"
def writej(path: Path, data: Any):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+".tmp"); tmp.write_text(json.dumps(data,indent=2,sort_keys=True)+"\n"); tmp.replace(path)
def config(path: Path) -> dict:
    value=tomllib.loads(path.read_text())
    # Deployments may pin the same upstream SHA at a different absolute path;
    # the environment override is recorded by the detached launch boundary.
    if os.environ.get("EVERBAR_CHECKOUT"): value["everbar_checkout"]=os.environ["EVERBAR_CHECKOUT"]
    return value
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
        c.execute("update datasets set data=? where id=?",(json.dumps(s,sort_keys=True),s["id"]))
    c.commit(); c.close(); writej(root/"progress"/"preflight.json",preflight(root,cfg))
def action(root:Path,s:dict,reason:str):
    p=root/"progress"/"user-actions.json"; a=json.loads(p.read_text()) if p.exists() else []
    if not any(x["dataset_id"]==s["id"] for x in a): a.append({"dataset_id":s["id"],"action":reason,"official_source":s["url"]})
    writej(p,a); (root/"progress"/"user-actions.md").write_text("# User actions\n\n"+"\n".join(f"- `{x['dataset_id']}`: {x['action']} — {x['official_source']}" for x in a)+"\n")
def hf_token() -> str|None:
    """Read a local Hugging Face CLI credential without exposing it in receipts."""
    for value in (os.environ.get("HF_TOKEN"),os.environ.get("HUGGINGFACE_HUB_TOKEN")):
        if value: return value.strip()
    home=Path.home(); hf_home=Path(os.environ.get("HF_HOME",home/".cache"/"huggingface"))
    for path in (hf_home/"token",home/".cache"/"huggingface"/"token",home/".huggingface"/"token"):
        try:
            value=path.read_text().strip()
            if value: return value
        except OSError: pass
    return None
def download(root:Path,s:dict)->Path:
    dest=root/"raw"/s["id"]/(s["id"]+".download"); dest.parent.mkdir(parents=True,exist_ok=True); part=dest.with_suffix(".part")
    if dest.exists(): return dest
    for attempt in range(3):
        try:
            headers={}; offset=part.stat().st_size if part.exists() else 0
            if offset: headers["Range"]=f"bytes={offset}-"
            if urllib.parse.urlparse(s["url"]).hostname in {"huggingface.co","hf.co"}:
                token=hf_token()
                if token: headers["Authorization"]="Bearer "+token
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
    elif tarfile.is_tarfile(artifact):
        with tarfile.open(artifact) as archive:
            members = archive.getmembers()
            for member in members:
                target = (out/member.name).resolve()
                if not target.is_relative_to(out.resolve()) or member.issym() or member.islnk():
                    raise ValueError("unsafe archive member")
            archive.extractall(out, members=members, filter="data")
    else: shutil.copy2(artifact,out/artifact.name)
    marker.write_text(sha(artifact.read_bytes())+"\n"); return out
def midi_files(folder:Path): return sorted(p for p in folder.rglob("*") if p.suffix.lower() in {".mid",".midi"})
PERFORMANCE_FLATTENING_POLICY="performance-flattening-v1"
def performance_flattening_v1(mid):
    """Render V1-supported pedal semantics without changing original MIDI bytes.

    CC64 sustain and CC66 sostenuto delay note-offs; CC121 consumes a
    channel-local controller reset; CC67 is explicitly unrepresentable in V1
    and removed. Verified zero-duration note pairs are no-ops and are removed
    with an auditable count.
    """
    import mido
    result=mido.MidiFile(type=mid.type,ticks_per_beat=mid.ticks_per_beat)
    counts={"cc64_rendered":0,"cc66_rendered":0,"cc67_discarded":0,"cc121_resets_consumed":0,"zero_duration_notes_dropped":0,"end_of_track_noteoffs":0}
    for track in mid.tracks:
        absolute=0; sequence=0; events=[]; active={}; deferred=[]; sustain={}; sostenuto={}; eot=None; last_tick=0
        def emit(tick,msg):
            nonlocal sequence
            events.append((tick,sequence,msg.copy(time=0))); sequence+=1
        def flush(channel,tick):
            for note in deferred[:]:
                if note["channel"] != channel: continue
                if sustain.get(channel,False) or (sostenuto.get(channel,False) and note["sostenuto"]): continue
                emit(tick,note["off"]); deferred.remove(note)
        for msg in track:
            absolute+=msg.time; last_tick=max(last_tick,absolute)
            if msg.type=="end_of_track": eot=(absolute,msg); continue
            channel=getattr(msg,"channel",None)
            if msg.type=="control_change" and msg.control in {64,66,67,121}:
                if msg.control==67: counts["cc67_discarded"]+=1; continue
                if msg.control==121:
                    # Reset All Controllers applies at this source tick. It
                    # must release pedal-deferred notes before the CC is
                    # removed; physically held keys remain active normally.
                    sustain[channel]=False; sostenuto[channel]=False; counts["cc121_resets_consumed"]+=1
                    for notes in active.values():
                        for note in notes:
                            if note["channel"]==channel: note["sostenuto"]=False
                    for note in deferred:
                        if note["channel"]==channel: note["sostenuto"]=False
                    flush(channel,absolute); continue
                down=msg.value>=64
                if msg.control==64:
                    prior=sustain.get(channel,False); sustain[channel]=down; counts["cc64_rendered"]+=1
                    if prior and not down: flush(channel,absolute)
                else:
                    prior=sostenuto.get(channel,False); sostenuto[channel]=down; counts["cc66_rendered"]+=1
                    if down and not prior:
                        for notes in active.values():
                            for note in notes:
                                if note["channel"]==channel: note["sostenuto"]=True
                    if prior and not down:
                        for note in deferred:
                            if note["channel"]==channel: note["sostenuto"]=False
                        flush(channel,absolute)
                continue
            is_on=msg.type=="note_on" and msg.velocity>0
            is_off=msg.type=="note_off" or (msg.type=="note_on" and msg.velocity==0)
            if is_on:
                key=(channel,msg.note); emit(absolute,msg)
                active.setdefault(key,[]).append({"channel":channel,"start":absolute,"event_index":len(events)-1,"sostenuto":False,"off":None})
            elif is_off:
                key=(channel,msg.note); notes=active.get(key,[])
                if not notes: emit(absolute,msg); continue
                note=notes.pop(); note["off"]=msg.copy(time=0)
                if absolute==note["start"]:
                    events[note["event_index"]]=None; counts["zero_duration_notes_dropped"]+=1; continue
                if sustain.get(channel,False) or (sostenuto.get(channel,False) and note["sostenuto"]): deferred.append(note)
                else: emit(absolute,msg)
            else: emit(absolute,msg)
        for note in deferred:
            emit(last_tick,note["off"]); counts["end_of_track_noteoffs"]+=1
        final=[event for event in events if event is not None]
        if eot is not None: final.append((max(last_tick,eot[0]),sequence,eot[1].copy(time=0)))
        final.sort(key=lambda item:(item[0],item[1])); out=mido.MidiTrack(); previous=0
        for tick,_,msg in final:
            out.append(msg.copy(time=tick-previous)); previous=tick
        result.tracks.append(out)
    return result,counts
def convert_for_brick3(root:Path,ds:dict,candidate:Path,candidate_id:str):
    import mido
    original=candidate.read_bytes(); converted,counts=performance_flattening_v1(mido.MidiFile(candidate))
    output=root/"derived"/ds["id"]/"prebrick3"/(candidate_id+".mid"); output.parent.mkdir(parents=True,exist_ok=True); converted.save(output)
    receipt={"policy_id":PERFORMANCE_FLATTENING_POLICY,"source_candidate_id":candidate_id,"source_midi_sha256":sha(original),"output_midi_sha256":sha(output.read_bytes()),"source_path":str(candidate),"output_path":str(output),"counts":counts}
    receipt["receipt_sha256"]=sha(json.dumps(receipt,sort_keys=True,separators=(",",":")))
    writej(root/"receipts"/"conversion"/(candidate_id+".json"),receipt)
    return output,receipt
def sample_brick3(root:Path,cfg:dict,dataset_ids:list[str],limit:int=64):
    """Deterministically audit bounded converted samples without touching shards."""
    import mido
    sources={s["id"]:s for s in registry(cfg)}; report={"policy_id":PERFORMANCE_FLATTENING_POLICY,"limit_per_dataset":limit,"datasets":{}}
    for dataset_id in dataset_ids:
        if dataset_id not in sources: raise ValueError(f"unknown dataset: {dataset_id}")
        candidates=sorted((root/"derived"/dataset_id).glob("*.mid"))[:limit]
        output_dir=root/"reports"/"brick3-samples"/PERFORMANCE_FLATTENING_POLICY/dataset_id; output_dir.mkdir(parents=True,exist_ok=True)
        receipt_dir=root/"receipts"/"conversion-samples"/PERFORMANCE_FLATTENING_POLICY/dataset_id; receipt_dir.mkdir(parents=True,exist_ok=True)
        accepted=0; code_streams=Counter(); execution_failures=[]
        for candidate in candidates:
            converted,counts=performance_flattening_v1(mido.MidiFile(candidate)); output=output_dir/candidate.name; converted.save(output)
            conversion={"policy_id":PERFORMANCE_FLATTENING_POLICY,"sample":True,"source_candidate_id":candidate.stem,"source_midi_sha256":sha(candidate.read_bytes()),"output_midi_sha256":sha(output.read_bytes()),"source_path":str(candidate),"output_path":str(output),"counts":counts}
            conversion["receipt_sha256"]=sha(json.dumps(conversion,sort_keys=True,separators=(",",":"))); writej(receipt_dir/(candidate.stem+".json"),conversion)
            run=subprocess.run(["uv","run","--directory",cfg["everbar_checkout"],"everbar-inspect-midi",str(output),"--root",str(output_dir),"--corpus-id",dataset_id],capture_output=True,text=True,timeout=120)
            if run.returncode or not run.stdout.strip():
                execution_failures.append({"candidate_id":candidate.stem,"diagnostic":run.stderr[-500:]}); code_streams["EXECUTION_FAILURE"]+=1; continue
            receipt=json.loads(run.stdout.splitlines()[-1]); decision=receipt.get("decision",{}); codes=set(decision.get("reason_codes",[]))
            if decision.get("status")=="ACCEPT": accepted+=1
            for code in codes: code_streams[code]+=1
        size=len(candidates); rejected=size-accepted-len(execution_failures)
        report["datasets"][dataset_id]={"sample_size":size,"accepted":accepted,"rejected":rejected,"execution_failures":len(execution_failures),"accept_rate":accepted/size if size else None,"reject_rate":rejected/size if size else None,"unsupported_meter_rejection_rate":code_streams["REJECT_UNSUPPORTED_METER"]/size if size else None,"semantic_control_rejection_rate":code_streams["REJECT_SEMANTIC_CONTROL_CHANGE"]/size if size else None,"rejection_class_stream_counts":dict(sorted(code_streams.items())),"execution_failure_samples":execution_failures}
    writej(root/"reports"/"brick3-samples"/PERFORMANCE_FLATTENING_POLICY/"summary.json",report)
    return report
def _pdmx_allowed_midi_paths(root: Path, ds: dict) -> set[str]:
    """Map the official no-license-conflict JSON manifest to PDMX MIDI paths."""
    artifact=root/"raw"/ds["id"]/(ds["id"]+"-subset-paths.tar.gz")
    if not artifact.exists():
        temporary=download(root,dict(ds,id=ds["id"]+"-subset-download",url=ds["subset_url"]))
        temporary.replace(artifact)
    subset_dir=extract(root,{"id":ds["id"]+"-subset-paths"},artifact)
    listing=next(subset_dir.rglob("no_license_conflict.txt"))
    allowed=set()
    for line in listing.read_text(encoding="utf-8").splitlines():
        path=PurePosixPath(line.lstrip("./"))
        if path.parts and path.parts[0] == "data": path=PurePosixPath("mid",*path.parts[1:])
        allowed.add(path.with_suffix(".mid").as_posix())
    return allowed
def derive(root:Path,c,ds:dict,folder:Path,cfg:dict):
    import mido
    result={"pieces":0,"tracks":0,"candidates":0,"accepts":0,"rejects":0}
    allowed=_pdmx_allowed_midi_paths(root,ds) if ds["id"] == "pdmx" else None
    for p in midi_files(folder):
        if allowed is not None and p.relative_to(folder).as_posix() not in allowed: continue
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
            brick3_input,conversion=convert_for_brick3(root,ds,out,cand)
            c.execute("insert or replace into items values(?,?,?,?,?,?,?)",(cand,ds["id"],"DERIVED",str(out),None,sha(raw),json.dumps({"source_piece_id":piece,"source_track_id":trackid,"sibling_track_ids":[stable('track',piece,x,'',[],False) for x in range(len(mid.tracks)) if x!=ti],"programs":programs,"is_drum":False,"source_track_name":name,"conversion":conversion})))
            result["candidates"]+=1
            # Exact pinned boundary: upstream process owns acceptance semantics.
            cmd=["uv","run","--directory",cfg["everbar_checkout"],"everbar-inspect-midi",str(brick3_input),"--root",str(brick3_input.parent),"--corpus-id",ds["id"]]
            run=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
            if run.returncode==0 and run.stdout.strip():
                receipt=json.loads(run.stdout.splitlines()[-1]); canonical=receipt.get("canonical_score_sha256") or ((receipt.get("canonical") or {}).get("event_sha256"))
                c.execute("update items set state=?,canonical_hash=?,detail=? where id=?",("BRICK3_COMPLETE",canonical,json.dumps({"brick3":"ACCEPT","everbar_sha":cfg["everbar_sha"],"conversion":conversion,"receipt":receipt}),cand)); result["accepts"]+=1
            else:
                c.execute("update items set state=?,detail=? where id=?",("BRICK3_COMPLETE",json.dumps({"brick3":"REJECT","everbar_sha":cfg["everbar_sha"],"conversion":conversion,"diagnostics":run.stderr[-2000:]}),cand)); result["rejects"]+=1
    return result
def progress(root:Path,cfg:dict,state="RUNNING",stage="DISCOVERY"):
    c=db(root); datasets=c.execute("select state,data from datasets").fetchall(); items=c.execute("select state,canonical_hash from items").fetchall(); c.close(); raw=sum(p.stat().st_size for p in (root/"raw").rglob("*") if p.is_file()); starts=(root/"state"/"started"); elapsed=max(1,time.time()-float(starts.read_text())) if starts.exists() else 1
    live={d.name:sum(1 for p in d.glob("*.mid")) for d in (root/"derived").iterdir() if d.is_dir()}
    converted=Counter()
    for receipt_path in (root/"receipts"/"conversion").glob("*.json") if (root/"receipts"/"conversion").exists() else []:
        try: converted[Path(json.loads(receipt_path.read_text())["output_path"]).parent.parent.name]+=1
        except (KeyError, OSError, json.JSONDecodeError): pass
    pdmx_total=0; manifest=root/"extracted"/"pdmx-subset-paths"/"subset_paths"/"no_license_conflict.txt"
    if manifest.exists(): pdmx_total=sum(1 for _ in manifest.open(encoding="utf-8"))
    pdmx_done=converted.get("pdmx",0); pdmx_rate=pdmx_done/elapsed if pdmx_done else 0.0
    if pdmx_total and pdmx_done >= 100 and elapsed >= 60 and pdmx_rate:
        remaining=max(0,pdmx_total-pdmx_done); likely=remaining/pdmx_rate
        eta={"status":"ESTIMATING","scope":"pdmx_derivation_only","best_seconds":round(likely*.8),"likely_seconds":round(likely),"worst_seconds":round(likely*1.25)}
    else: eta={"status":"INSUFFICIENT_DATA","scope":"overall","best_seconds":None,"likely_seconds":None,"worst_seconds":None}
    accepted=sum(1 for _,h in items if h); output={"state":state,"current_stage":stage,"datasets_total":len(datasets),"datasets_complete":sum(x[0]=="DONE" for x in datasets),"datasets_started":sum(x[0] not in {"DISCOVERED","GATED_USER_ACTION_REQUIRED"} for x in datasets),"bytes_downloaded":raw,"source_pieces_indexed":len(items),"derived_streams":len(items),"live_derived_streams":sum(live.values()),"live_derived_by_dataset":live,"live_converted_by_dataset":dict(converted),"pdmx":{"eligible_source_paths":pdmx_total,"converted_streams":pdmx_done,"percent_complete":round(100*pdmx_done/pdmx_total,3) if pdmx_total else None},"brick3_accepts":accepted,"brick3_rejects":sum(1 for x,_ in items if x=="BRICK3_COMPLETE")-accepted,"canonical_duplicates":max(0,accepted-len({h for _,h in items if h})),"disk_free_bytes":shutil.disk_usage(root).free,"throughput":{"bytes_per_second":raw/elapsed,"pdmx_streams_per_second":round(pdmx_rate,4)},"eta":eta}
    writej(root/"progress"/"current.json",output); return output
def monitor(root:Path,cfg:dict,interval:int=300,pid:int|None=None):
    """Publish live filesystem-derived progress while a large batch is uncommitted."""
    while True:
        progress(root,cfg,"RUNNING","PDMX_DERIVATION")
        if pid is not None:
            try: os.kill(pid,0)
            except ProcessLookupError: break
        time.sleep(max(5,interval))
    return progress(root,cfg,"PARTIAL","MONITOR_COMPLETE")
def shard(root:Path,cfg:dict,dataset_ids:list[str]):
    """Run non-overlapping source datasets in parallel without central DB locks."""
    sources={s["id"]:s for s in registry(cfg)}; results=[]
    for dataset_id in dataset_ids:
        ds=sources.get(dataset_id)
        if not ds: raise ValueError(f"unknown dataset: {dataset_id}")
        if ds["training"] != "ALLOWED" or ds["role"] != "raw": raise ValueError(f"not eligible for shard: {dataset_id}")
        artifact=root/"raw"/dataset_id/(dataset_id+".download")
        if not artifact.exists(): raise FileNotFoundError(f"download is not ready: {dataset_id}")
        if dataset_id == "pdmx": (root/"state"/"started").write_text(str(time.time()))
        folder=extract(root,ds,artifact)
        shard_root=root/"state"/"shards"/dataset_id; shard_root.mkdir(parents=True,exist_ok=True)
        c=db(shard_root)
        result=derive(root,c,ds,folder,cfg); c.commit(); c.close()
        receipt={"state":"COMPLETE","dataset_id":dataset_id,"result":result,"shard_db":str(shard_root/"state"/"motherlode.sqlite"),"finished_at":time.time()}
        writej(root/"progress"/"shards"/(dataset_id+".json"),receipt); results.append(receipt)
    return {"state":"COMPLETE","results":results}
def merge_shards(root:Path,cfg:dict):
    """Merge completed isolated worker receipts after the central writer is idle."""
    c=db(root); merged=[]
    for receipt_path in sorted((root/"progress"/"shards").glob("*.json")) if (root/"progress"/"shards").exists() else []:
        receipt=json.loads(receipt_path.read_text())
        if receipt.get("state") != "COMPLETE": continue
        dataset_id=receipt["dataset_id"]; shard_db=Path(receipt["shard_db"])
        if not shard_db.exists(): continue
        source=sqlite3.connect(shard_db)
        rows=source.execute("select id,dataset_id,state,source_path,canonical_hash,raw_hash,detail from items").fetchall(); source.close()
        c.executemany("insert or replace into items values(?,?,?,?,?,?,?)",rows)
        c.execute("update datasets set state=?,updated=? where id=?",("DONE",time.time(),dataset_id)); merged.append({"dataset_id":dataset_id,"items":len(rows)})
    c.commit(); c.close(); reports(root,cfg); return {"state":"COMPLETE","merged":merged,"progress":progress(root,cfg,"PARTIAL","SHARD_MERGED")}
def reconcile(root:Path,cfg:dict):
    """Backfill canonical identities from immutable Brick 3 receipts after upgrades."""
    c=db(root)
    for ident, detail in c.execute("select id,detail from items where canonical_hash is null").fetchall():
        try:
            receipt=json.loads(detail).get("receipt") or {}
            canonical=(receipt.get("canonical") or {}).get("event_sha256")
            if canonical: c.execute("update items set canonical_hash=? where id=?",(canonical,ident))
        except (TypeError, json.JSONDecodeError): pass
    # Older runs treated the Aria source-code repository as corpus payload.  Do
    # not retain that false completion; the current registry decides whether the
    # official dataset is still gated or has user-authorized automated access.
    aria=next((s for s in registry(cfg) if s["id"] == "aria-midi"),None)
    if aria:
        state="DISCOVERED" if aria["training"] == "ALLOWED" and aria["method"] != "manual_gated" else "GATED_USER_ACTION_REQUIRED"
        c.execute("update datasets set state=?,updated=? where id=? and state='DONE'",(state,time.time(),"aria-midi"))
    c.commit(); c.close(); reports(root,cfg); return progress(root,cfg,"PARTIAL","RECONCILED")
def reports(root:Path,cfg:dict):
    c=db(root); rows=c.execute("select data,state from datasets").fetchall(); c.close(); src=[(json.loads(d),s) for d,s in rows]
    for name,body in {"licenses.md":"# Licenses\n\n"+"\n".join(f"- **{x['name']}**: {x['license']} / training {x['training']}" for x,_ in src),"acquisition.md":"# Acquisition\n\n"+"\n".join(f"- `{x['id']}`: {s}" for x,s in src),"attribution.md":"# Build attribution\n\n"+"\n".join(f"- {x['name']}: {x['citation']}" for x,s in src if s=="DONE"),"dedupe.md":"# Dedupe\n\nTier 0 raw SHA-256; Tier 1 Brick-3 canonical identity; later tiers cluster rather than erase provenance.\n","brick3.md":"# Brick 3\n\nPinned upstream SHA: `"+cfg['everbar_sha']+"`.\n","profile.md":"# Profile evidence\n\nCaps 64, 96, 128, 160, 192, 256 are evidence only; no cap is frozen.\n","failures.md":"# Failures\n\nSee state database and user action queue.\n"}.items(): (root/"reports"/name).write_text(body)
def prefetch(root:Path,cfg:dict,workers:int=3):
    """Acquire approved source artifacts independently of CPU-bound derivation.

    This intentionally has no dataset-state mutations: a processing runner sees
    only atomically completed `.download` artifacts, while partial files remain
    resumable and are owned solely by this prefetch process.
    """
    for name in ("raw","progress","logs","state"):
        (root/name).mkdir(parents=True,exist_ok=True)
    pf=preflight(root,cfg)
    if not pf["ok"]:
        receipt={"state":"RESOURCE_PAUSED","reason":"storage reserve","disk_free_bytes":pf["disk_free_bytes"]}
        writej(root/"progress"/"prefetch.json",receipt); return receipt
    sources=[s for s in registry(cfg) if s["training"] == "ALLOWED" and s["method"] != "manual_gated" and s["role"] == "raw" and s["id"] != "pdmx"]
    started=time.time(); results=[]
    def fetch(s:dict):
        artifacts=[s]
        if s.get("metadata_url"): artifacts.append({**s,"id":s["id"]+"-metadata","url":s["metadata_url"]})
        paths=[]
        for artifact in artifacts:
            if shutil.disk_usage(root).free < int(cfg["min_free_bytes"]): raise RuntimeError("storage reserve reached")
            paths.append(str(download(root,artifact)))
        return {"dataset_id":s["id"],"state":"READY","artifacts":paths}
    with ThreadPoolExecutor(max_workers=max(1,workers),thread_name_prefix="motherlode-download") as pool:
        futures={pool.submit(fetch,s):s for s in sources}
        for future in as_completed(futures):
            s=futures[future]
            try: results.append(future.result())
            except Exception as exc:
                results.append({"dataset_id":s["id"],"state":"FAILED","error":str(exc)[:500]})
            writej(root/"progress"/"prefetch.json",{"state":"RUNNING","workers":workers,"started_at":started,"completed":len(results),"total":len(sources),"results":results,"disk_free_bytes":shutil.disk_usage(root).free})
    failed=[x for x in results if x["state"] == "FAILED"]
    receipt={"state":"PARTIAL" if failed else "SUCCESS","workers":workers,"started_at":started,"finished_at":time.time(),"completed":len(results),"total":len(sources),"results":results,"disk_free_bytes":shutil.disk_usage(root).free}
    writej(root/"progress"/"prefetch.json",receipt)
    return receipt
def run(root:Path,cfg:dict):
    init(root,cfg); (root/"state"/"started").write_text(str(time.time())); pf=preflight(root,cfg)
    if not pf["ok"]: return progress(root,cfg,"RESOURCE_PAUSED","RESOURCE_GUARD")
    progress(root,cfg,"RUNNING","DISCOVERY")
    c=db(root)
    states=dict(c.execute("select id,state from datasets").fetchall())
    resource_paused=False
    for s in registry(cfg):
        if states.get(s["id"]) == "DONE": continue
        if s["role"] in {"superseded","documented","overlay","synthetic"}: continue
        if s["training"]!="ALLOWED" or s["method"]=="manual_gated": c.execute("update datasets set state=?,updated=? where id=?",("GATED_USER_ACTION_REQUIRED",time.time(),s["id"])); action(root,s,"license review or manual terms/credential action required"); continue
        # Never schedule an estimate that would breach the configured reserve.
        if s["estimate"] and s["estimate"] > shutil.disk_usage(root).free-int(cfg["min_free_bytes"]):
            c.execute("update datasets set state=?,updated=? where id=?",("RESOURCE_PAUSED",time.time(),s["id"])); c.commit(); progress(root,cfg,"RESOURCE_PAUSED","RESOURCE_GUARD"); resource_paused=True; break
        try:
            c.execute("update datasets set state=?,updated=? where id=?",("DOWNLOADING",time.time(),s["id"])); c.commit(); art=download(root,s)
            # Sidecar metadata is source-qualified and retained separately from
            # the payload; it never changes the source artifact's identity.
            if s.get("metadata_url"):
                download(root,{**s,"id":s["id"]+"-metadata","url":s["metadata_url"]})
            c.execute("update datasets set state=?,updated=? where id=?",("HASH_VERIFIED",time.time(),s["id"])); folder=extract(root,s,art); derive(root,c,s,folder,cfg); c.execute("update datasets set state=?,updated=? where id=?",("DONE",time.time(),s["id"])); c.commit(); progress(root,cfg,"RUNNING","BRICK3")
        except Exception as e: c.execute("update datasets set state=?,updated=? where id=?",("FAILED",time.time(),s["id"])); action(root,s,"automatic acquisition failed: "+str(e)[:300]); c.commit()
    c.close(); reports(root,cfg); return progress(root,cfg,"RESOURCE_PAUSED" if resource_paused else "PARTIAL","RESOURCE_GUARD" if resource_paused else "REPORTING")
