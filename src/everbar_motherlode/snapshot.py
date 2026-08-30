"""Immutable, read-only V1 preview snapshots from completed Motherlode shards."""
from __future__ import annotations

import argparse, hashlib, json, os, shutil, sqlite3, sys
from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .feature_base import ensure_feature_schema, materialize_canonical_stream

CAPS = (64, 96, 128, 160, 192, 256)
WINDOW_BARS = 4

def _sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()

def _write(path: Path, value: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True); temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    temp.replace(path); return hashlib.sha256(path.read_bytes()).hexdigest()

def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True); temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w") as handle:
        for row in rows: handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    temp.replace(path); return hashlib.sha256(path.read_bytes()).hexdigest()

class _UnionFind:
    def __init__(self): self.parent: dict[str, str] = {}
    def find(self, key: str) -> str:
        self.parent.setdefault(key, key)
        if self.parent[key] != key: self.parent[key] = self.find(self.parent[key])
        return self.parent[key]
    def join(self, left: str, right: str) -> None:
        left, right = self.find(left), self.find(right)
        if left != right: self.parent[max(left, right)] = min(left, right)

def _split(family_id: str) -> str:
    bucket = int(hashlib.sha256(family_id.encode()).hexdigest()[:8], 16) % 1000
    return "train" if bucket < 800 else "validation" if bucket < 900 else "test"

def _completed_pdmx_dbs(root: Path) -> list[Path]:
    paths=[]
    for receipt in sorted((root / "progress" / "shards").glob("pdmx-part-*-of-00096.json")):
        try: raw=json.loads(receipt.read_text())
        except json.JSONDecodeError: continue
        if raw.get("state") == "COMPLETE": paths.append(Path(raw["shard_db"]))
    return sorted(set(paths))

def _rows(db_path: Path, dataset: str) -> list[dict[str, Any]]:
    conn=sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    rows=[]
    for stream_id, canonical_hash, raw_hash, detail in conn.execute("select id,canonical_hash,raw_hash,detail from items where dataset_id=? and state='BRICK3_COMPLETE' and canonical_hash is not null", (dataset,)):
        value=json.loads(detail); receipt=value.get("receipt") or {}
        if value.get("brick3") != "ACCEPT" or receipt.get("canonical", {}).get("event_sha256") != canonical_hash: continue
        provenance=value.get("provenance") or value
        rows.append({"stream_id":stream_id,"dataset_id":dataset,"canonical_hash":canonical_hash,"raw_hash":raw_hash,"detail":value,"provenance":provenance,"db_path":str(db_path)})
    conn.close(); return rows

def _poc_rows(root: Path) -> list[dict[str, Any]]:
    """Load provenance-rich POP909 POC candidate receipts."""
    rows=[]
    for path in sorted((root / "records" / "candidates").glob("*.json")):
        value=json.loads(path.read_text())
        if value.get("brick3_status") != "ACCEPT" or not value.get("receipt"): continue
        receipt=dict(value["receipt"]); receipt.update({key:value[key] for key in ("policy_id","policy_sha256","language_id","language_sha256") if value.get(key) is not None})
        detail=dict(value); detail.update({"brick3":"ACCEPT","receipt":receipt,"everbar_sha":value.get("everbar_sha"),"provenance":{"source_piece_id":value.get("source_piece_id"),"source_track_id":value.get("source_track_id"),"dataset_version":"2020","sibling_track_ids":value.get("sibling_track_ids",[]),"programs":value.get("programs",[]),"is_drum":False,"source_track_name":value.get("source_track_name"),"source_native_role":value.get("source_native_role"),"source_timing":{}}})
        rows.append({"stream_id":value["candidate_id"],"dataset_id":"pop909","canonical_hash":value["canonical_score_sha256"],"raw_hash":value.get("candidate_sha256"),"detail":detail,"provenance":detail["provenance"],"db_path":str(root/"state"/"canonical.sqlite"),"poc_tokenization":value.get("tokenization")})
    return rows

def _copy_source_rows(destination: sqlite3.Connection, db_paths: list[Path]) -> None:
    for path in db_paths:
        source=sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        tables={row[0] for row in source.execute("select name from sqlite_master where type='table'")}
        for table in ("source_pieces", "source_tracks"):
            if table not in tables: continue
            values=source.execute(f"select * from {table}").fetchall()
            if values:
                marks=",".join("?" for _ in values[0]); destination.executemany(f"insert or ignore into {table} values({marks})", values)
        source.close()

def _score(payload: dict[str, Any]):
    from symusic import Note, Score, Tempo, TimeSignature, Track
    result=Score(int(payload["tpq"])); track=Track(program=int(payload["track"]["program"]), is_drum=bool(payload["track"]["is_drum"]))
    for onset, duration, pitch, velocity in payload["track"]["notes"]: track.notes.append(Note(int(onset), int(duration), int(pitch), int(velocity)))
    result.tracks.append(track)
    for tick, mspq in payload["tempo"]: result.tempos.append(Tempo(int(tick), 60000000 / int(mspq)))
    for tick, numerator, denominator in payload["time_signature"]: result.time_signatures.append(TimeSignature(int(tick), int(numerator), int(denominator)))
    return result

def _tokenize(entry: dict[str, Any], everbar: Path) -> dict[str, Any]:
    if str(everbar / "src") not in sys.path: sys.path.insert(0, str(everbar / "src"))
    from dreamstream_everbar.corpus.receipt import CorpusReceipt, CorpusResult, canonical_score_hash
    from dreamstream_everbar.packing import tokenize_corpus_result
    receipt=entry["detail"]["receipt"]; score_payload=receipt["canonical"]["score"]; score=_score(score_payload)
    if canonical_score_hash(score)[1] != entry["canonical_hash"]: raise ValueError(f"receipt score mismatch: {entry['stream_id']}")
    piece=tokenize_corpus_result(CorpusResult(CorpusReceipt(receipt), score), config_path=everbar / "configs" / "pertok-v1.json", manifest_path=everbar / "artifacts" / "vocab" / "pertok-v1-vocab.json")
    bars=piece.sequence.split_per_bars()
    return {"stream_id":entry["stream_id"],"canonical_hash":entry["canonical_hash"],"source_piece_id":entry["provenance"].get("source_piece_id"),"source_track_id":entry["provenance"].get("source_track_id"),"tokenization_sha256":piece.tokenization_sha256,"ids":list(piece.ids),"tokens":list(piece.tokens),"bar_ticks":list(piece.bar_ticks),"bar_ids":[list(x.ids) for x in bars],"bar_tokens":[list(x.tokens) for x in bars]}

def _percentiles(values: list[int]) -> dict[str, int | None]:
    if not values: return {key:None for key in ("p50","p75","p90","p95","p99","max")}
    values=sorted(values); n=len(values)
    return {key:values[min(n-1, (p*n+99)//100-1)] for key,p in (("p50",50),("p75",75),("p90",90),("p95",95),("p99",99))} | {"max":values[-1]}

def _profile(records: list[dict[str, Any]], *, scope: str = "EV1_PREVIEW_ONLY") -> dict[str, Any]:
    lengths=[len(ids) for row in records for ids in row["bar_ids"]]; output={"schema":"dreamstream-everbar.bar-profile/v1","scope":scope,"token_length_distribution":_percentiles(lengths),"candidate_caps":{}}
    chosen=None
    for cap in CAPS:
        bars_fit=sum(x<=cap for x in lengths); streams_fit=sum(all(len(x)<=cap for x in row["bar_ids"]) for row in records); padding=sum(cap-x for x in lengths)
        row={"cap":cap,"represented_bar_coverage":bars_fit/len(lengths),"accepted_whole_stream_coverage":streams_fit/len(records),"bars_fit":bars_fit,"bars_total":len(lengths),"whole_streams_fit":streams_fit,"whole_streams_total":len(records),"overflow_bar_count":len(lengths)-bars_fit,"overflow_stream_count":len(records)-streams_fit,"pad_ratio_estimate_if_all_fit":padding/(cap*len(lengths)),"p50":output["token_length_distribution"]["p50"],"p75":output["token_length_distribution"]["p75"],"p90":output["token_length_distribution"]["p90"],"p95":output["token_length_distribution"]["p95"],"p99":output["token_length_distribution"]["p99"],"max_token_length":output["token_length_distribution"]["max"]}
        output["candidate_caps"][str(cap)]=row
        if chosen is None and row["represented_bar_coverage"]>=.99 and row["accepted_whole_stream_coverage"]>=.99: chosen=cap
    output["selected_cap"]=chosen or max(CAPS, key=lambda x:(output["candidate_caps"][str(x)]["accepted_whole_stream_coverage"],output["candidate_caps"][str(x)]["represented_bar_coverage"],-x))
    output["selection_criterion"]="smallest cap with >=99% represented-bar and whole-stream coverage; otherwise best whole-stream coverage, then bar coverage, then smallest cap"
    return output

def build(*, motherlode_root: Path, output_root: Path, everbar_checkout: Path, motherlode_sha: str,
          snapshot_name: str | None = None,
          snapshot_scope: str = "EV1_PREVIEW_ONLY",
          everbar_sha: str | None = None) -> dict[str, Any]:
    poc_db=motherlode_root/"state"/"canonical.sqlite"
    if poc_db.is_file() and (motherlode_root/"records"/"candidates").is_dir():
        pdmx_dbs=[]; pop_db=poc_db; all_rows=_poc_rows(motherlode_root); authority_kind="POP909_POC_PROVENANCE_RICH"
    else:
        pdmx_dbs=_completed_pdmx_dbs(motherlode_root); pop_db=motherlode_root/"state"/"motherlode.sqlite"; all_rows=[entry for path in pdmx_dbs for entry in _rows(path,"pdmx")] + _rows(pop_db,"pop909"); authority_kind="CURRENT_MOTHERLODE_RUNTIME"
    if not all_rows: raise RuntimeError("no completed clean accepted streams available")
    # Exact canonical content defines the preview's only collapse policy.  Build source-piece components first so no exact-family leaks splits.
    families=_UnionFind(); by_hash: dict[str,list[dict[str,Any]]]=defaultdict(list)
    for entry in all_rows:
        piece=entry["provenance"].get("source_piece_id") or entry["stream_id"]; families.find(piece); by_hash[entry["canonical_hash"]].append(entry)
    for edges in by_hash.values():
        pieces=sorted({x["provenance"].get("source_piece_id") or x["stream_id"] for x in edges})
        for piece in pieces[1:]: families.join(pieces[0],piece)
    components: dict[str,list[str]]=defaultdict(list)
    for entry in all_rows:
        piece=entry["provenance"].get("source_piece_id") or entry["stream_id"]; components[families.find(piece)].append(piece)
    family_ids={root:_sha({"kind":"exact-canonical-source-family/v1","source_piece_ids":sorted(set(pieces))}) for root,pieces in components.items()}
    selected=[]; provenance=[]
    for canonical_hash, edges in sorted(by_hash.items()):
        winner=min(edges,key=lambda x:(x["dataset_id"],x["stream_id"])); piece=winner["provenance"].get("source_piece_id") or winner["stream_id"]
        winner={**winner,"source_family_id":family_ids[families.find(piece)],"split":_split(family_ids[families.find(piece)])}; selected.append(winner)
        for edge in edges: provenance.append({"canonical_hash":canonical_hash,"canonical_stream_id":winner["stream_id"],"dataset_id":edge["dataset_id"],"source_stream_id":edge["stream_id"],"source_piece_id":edge["provenance"].get("source_piece_id"),"source_track_id":edge["provenance"].get("source_track_id"),"brick3_receipt_sha256":edge["detail"]["receipt"].get("receipt_sha256")})
    name=snapshot_name or f"ev1-preview-clean-{len(selected)//1000}k-v1"; final=output_root/name; stage=output_root/("."+name+".building")
    v2 = snapshot_name is not None
    scope = snapshot_scope if v2 else "EV1_PREVIEW_ONLY"
    if final.exists(): raise FileExistsError(f"immutable snapshot exists: {final}")
    if stage.exists(): shutil.rmtree(stage)
    stage.mkdir(parents=True)
    membership=[{"stream_id":x["stream_id"],"canonical_hash":x["canonical_hash"],"dataset_id":x["dataset_id"],"source_piece_id":x["provenance"].get("source_piece_id"),"source_track_id":x["provenance"].get("source_track_id"),"source_family_id":x["source_family_id"],"split":x["split"]} for x in selected]
    membership_sha=_write_jsonl(stage/"membership.jsonl",membership); _write_jsonl(stage/"canonical"/"provenance-edges.jsonl",provenance)
    canonical=sqlite3.connect(stage/"canonical"/"streams.sqlite"); ensure_feature_schema(canonical); _copy_source_rows(canonical, pdmx_dbs+[pop_db])
    canonical.execute("create table snapshot_provenance(canonical_stream_id text,canonical_hash text,dataset_id text,source_stream_id text,source_piece_id text,source_track_id text,brick3_receipt_sha256 text)")
    canonical.executemany("insert into snapshot_provenance values(:canonical_stream_id,:canonical_hash,:dataset_id,:source_stream_id,:source_piece_id,:source_track_id,:brick3_receipt_sha256)",provenance)
    for entry in selected:
        if not materialize_canonical_stream(canonical,stream_id=entry["stream_id"],dataset_id=entry["dataset_id"],detail=entry["detail"]): raise RuntimeError("canonical receipt materialization failed")
    canonical.commit(); canonical.close()
    check=sqlite3.connect(f"file:{stage/'canonical/streams.sqlite'}?mode=ro&immutable=1", uri=True)
    empty_by_stream={stream_id:{int(bar) for (bar,) in check.execute("select bar_index from canonical_bars where stream_id=? and is_empty=1",(stream_id,))} for stream_id in (x["stream_id"] for x in selected)}; check.close()
    # This is new PerTok packing only: it restores the accepted score from the persisted Brick-3 receipt and never opens source MIDI or invokes Brick 3.
    tokens=[]
    for number,entry in enumerate(selected,1):
        token=entry.get("poc_tokenization") or _tokenize(entry,everbar_checkout); token={**token,"stream_id":entry["stream_id"],"canonical_hash":entry["canonical_hash"],"source_piece_id":entry["provenance"].get("source_piece_id"),"source_track_id":entry["provenance"].get("source_track_id"),"source_family_id":entry["source_family_id"],"split":entry["split"],"empty_bar_indices":sorted(empty_by_stream.get(entry["stream_id"],set()))}; tokens.append(token)
        if number % 250 == 0: _write(stage/"progress.json",{"state":"TOKENIZING","completed":number,"total":len(selected)})
    profile=_profile(tokens, scope=scope); cap=int(profile["selected_cap"]); _write(stage/"profiles"/"brick4.json",profile)
    from numpy.lib.format import open_memmap
    import numpy as np
    kept=[x for x in tokens if all(len(ids)<=cap for ids in x["bar_ids"])]
    bars=sum(len(x["bar_ids"]) for x in kept); windows=sum(sum(1 for start in range(max(0,len(x["bar_ids"])-WINDOW_BARS+1)) if not any(bar in x["empty_bar_indices"] for bar in range(start,start+WINDOW_BARS))) for x in kept)
    packed=stage/"training"; packed.mkdir(); ids=open_memmap(packed/"input_ids.npy",mode="w+",dtype=np.int64,shape=(windows,WINDOW_BARS,cap)); masks=open_memmap(packed/"active_mask.npy",mode="w+",dtype=np.bool_,shape=(windows,WINDOW_BARS,cap))
    split_indices={"train":[],"validation":[],"test":[]}; windows_manifest=[]; at=0
    for row in kept:
        for start in range(max(0,len(row["bar_ids"])-WINDOW_BARS+1)):
            if any(bar in row["empty_bar_indices"] for bar in range(start,start+WINDOW_BARS)): continue
            ids[at,:,:]=0; masks[at,:,:]=False
            for block,values in enumerate(row["bar_ids"][start:start+WINDOW_BARS]): ids[at,block,:len(values)]=values; masks[at,block,:len(values)]=True
            split_indices[row["split"]].append(at); windows_manifest.append({"window_index":at,"stream_id":row["stream_id"],"source_piece_id":row["source_piece_id"],"source_family_id":row["source_family_id"],"split":row["split"],"start_bar":start}); at+=1
    del ids,masks
    for split,values in split_indices.items(): np.save(packed/f"{split}-indices.npy",np.asarray(values,dtype=np.int64))
    window_sha=_write_jsonl(packed/"window-manifest.jsonl",windows_manifest)
    from dreamstream_everbar.packing.format import PackingFormat
    fmt=PackingFormat(format_id=f"{name}-block-format-v1-cap-{cap}",cap=cap,production=False); block=fmt.to_dict()|{"format_sha256":fmt.format_sha256,"scope":scope,"corpus_membership_sha256":membership_sha}; _write(packed/"block-format.json",block)
    # Keep corpus construction CPU-only.  The profile JSON is the frozen Brick
    # 6 schema; importing its sampling class would unnecessarily import Torch.
    lengths=Counter(len(ids) for row in kept for ids in row["bar_ids"][1:])
    length={"schema":"dreamstream-everbar.length-profile/v1","profile_id":f"{name}-active-length-v1-cap-{cap}","block_format":{"format_id":fmt.format_id,"format_sha256":fmt.format_sha256,"cap":cap},"counts":[{"active_length":key,"count":value} for key,value in sorted(lengths.items())],"status":"TEST_ONLY","source_profile_sha256":_sha(profile)}
    length["profile_sha256"]=_sha(length); length|={"scope":scope,"corpus_membership_sha256":membership_sha}; _write(packed/"active-length-profile.json",length)
    stream_counts=Counter(x["split"] for x in kept); bar_counts=Counter(); token_counts=Counter()
    for row in kept: bar_counts[row["split"]]+=len(row["bar_ids"]); token_counts[row["split"]]+=sum(map(len,row["bar_ids"]))
    source_versions={"pdmx":"Zenodo 15571083 / CC0-1.0","pop909":"2020 / MIT"}
    semantic_payload={"schema":"everbar-motherlode.semantic-preview-corpus/v1","membership":membership,"tokenization":[{"stream_id":x["stream_id"],"tokenization_sha256":x["tokenization_sha256"]} for x in tokens],"windows":windows_manifest,"cap":cap}
    semantic_hash=_sha(semantic_payload)
    manifest={"schema":"everbar-motherlode.v2-development-preview/v1" if v2 else "everbar-motherlode.ev1-preview-snapshot/v1","snapshot_name":name,"scope":("V2_DEVELOPMENT_PREVIEW_CURRENT_MOTHERLODE" if v2 else "EV1_PREVIEW_ONLY_CLEAN_PERMISSIVE_PD"),"historical_v1_snapshot":"ev1-preview-clean-30k-v1" if v2 else None,"provenance_note":"New deterministic V2 development preview from surviving current Motherlode accepted canonical receipts; not the historical V1 training snapshot." if v2 else None,"motherlode_sha":motherlode_sha,"everbar_sha":selected[0]["detail"].get("everbar_sha"),"brick3":{"policy_id":selected[0]["detail"]["receipt"]["policy_id"],"policy_sha256":selected[0]["detail"]["receipt"]["policy_sha256"]},"pertok":{"language_id":selected[0]["detail"]["receipt"]["language_id"],"language_sha256":selected[0]["detail"]["receipt"]["language_sha256"]},"sources":source_versions,"license_lane":"CLEAN_PERMISSIVE_PD","dedupe":"raw SHA-256 provenance retained; Brick-3 canonical event SHA-256 collapsed; no near-duplicate clustering","membership_sha256":membership_sha,"semantic_corpus_hash":semantic_hash,"accepted_rows":len(all_rows),"canonical_duplicates_collapsed":len(all_rows)-len(selected),"unique_canonical_streams":len(selected),"training_streams":len(kept),"source_family_count":len(set(x["source_family_id"] for x in selected)),"selected_cap":cap,"block_format_sha256":fmt.format_sha256,"active_length_profile_sha256":length["profile_sha256"],"split":{"policy":"exact-canonical source-family SHA-256 mod 1000: train<800 validation<900 test>=900","streams":dict(stream_counts),"bars":dict(bar_counts),"active_tokens":dict(token_counts),"windows":{key:len(value) for key,value in split_indices.items()}},"canonical":{"database":"canonical/streams.sqlite","notes":sqlite3.connect(stage/"canonical"/"streams.sqlite").execute("select count(*) from canonical_notes").fetchone()[0],"bars":sqlite3.connect(stage/"canonical"/"streams.sqlite").execute("select count(*) from canonical_bars").fetchone()[0]},"training_view":"training","window_manifest_sha256":window_sha}
    manifest["scope"]=scope
    if everbar_sha is not None: manifest["everbar_sha"]=everbar_sha
    manifest["authority_kind"]=authority_kind
    manifest["manifest_sha256"]=_sha(manifest); _write(stage/"manifest.json",manifest)
    # Match the already-existing Brick 8 packed-view loader contract exactly.
    _write(packed/"corpus-manifest.json",manifest)
    view={"schema":"dreamstream-everbar.training-view/v1","scope":scope,"production":False,"corpus_manifest_sha256":manifest["manifest_sha256"],"block_format_id":fmt.format_id,"block_format_sha256":fmt.format_sha256,"cap":cap,"active_length_profile_id":length["profile_id"],"active_length_profile_sha256":length["profile_sha256"],"pertok_language_id":manifest["pertok"]["language_id"],"pertok_language_sha256":manifest["pertok"]["language_sha256"],"input_ids_shape":[windows,WINDOW_BARS,cap],"active_mask_shape":[windows,WINDOW_BARS,cap],"packed_bar_count":bars,"window_count":windows,"window_bars":WINDOW_BARS,"split_window_counts":{key:len(value) for key,value in split_indices.items()},"window_manifest_sha256":window_sha,"loader":"dreamstream_everbar.training.loader.SmokeBatchLoader.from_directory","authority":scope}
    view["manifest_hash"]=_sha(view); _write(packed/"training-view.json",view)
    _write(packed/"window-manifest.json",{"schema":"everbar-motherlode.snapshot-window-manifest/v1","sha256":window_sha,"window_count":windows,"source":"window-manifest.jsonl"})
    _write(packed/"model-config.json",{"schema":"brick8-training-model/v1","vocab_size":686,"hidden_size":192,"conditioning_dim":192,"num_layers":6,"num_heads":6,"mlp_ratio":4,"dropout":0.0,"block_size":cap,"num_blocks":WINDOW_BARS,"model_length":WINDOW_BARS*cap,"test_only":True})
    _write(packed/"trainer-config.json",{"schema":"brick8-training-config/v1","learning_rate":0.0005,"betas":[0.9,0.95],"weight_decay":0.0,"epsilon":1e-8,"warmup_steps":1,"max_steps":1,"training_minutes":1,"effective_batch_size":32,"precision":"fp32","seed":8,"checkpoint_minutes":[]})
    _write(stage/"attribution.json",{"snapshot":name,"sources":source_versions,"citations":{"pdmx":"Long et al., PDMX, ICASSP 2025, https://arxiv.org/abs/2409.10831","pop909":"Wang et al., POP909, ISMIR 2020, https://arxiv.org/abs/2008.07142"},"transformations":"persisted Brick-3 accepted canonical score -> frozen PerTok ids -> Brick-4 right-padded blocks; raw upstream payloads are not redistributed"})
    _write(stage/"progress.json",{"state":"COMPLETE","snapshot":name,"manifest_sha256":manifest["manifest_sha256"]}); stage.replace(final); return manifest | {"path":str(final)}

def main(argv: list[str] | None = None) -> int:
    p=argparse.ArgumentParser(); p.add_argument("--motherlode-root",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--everbar-checkout",type=Path,required=True); p.add_argument("--motherlode-sha",required=True); args=p.parse_args(argv)
    print(json.dumps(build(motherlode_root=args.motherlode_root,output_root=args.output_root,everbar_checkout=args.everbar_checkout,motherlode_sha=args.motherlode_sha),indent=2,sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
