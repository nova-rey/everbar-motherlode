from __future__ import annotations
import argparse, hashlib, os, subprocess, sys
from pathlib import Path
from .core import config, init, merge_shards, monitor, prefetch, preflight, progress, run, sample_brick3, shard, writej
from .distributed import distributed_shard, verify_distributed_run
from .feature_base import backfill_canonical, extract_primitive_features
from .v2_features import attach_projection, extract_rows, write_feature_view
from .v2_projection import project_stream, write_projection
def main(argv=None):
 p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="cmd",required=True)
 for x in ("preflight","build","status","reconcile","prefetch","monitor","shard","merge-shards","sample-brick3","distributed-shard","verify-distributed-run","backfill-canonical","extract-features","project-v2","characterize-v2"): q=sub.add_parser(x); q.add_argument("--root",type=Path,required=True); q.add_argument("--config",type=Path,default=Path("configs/motherlode-v1.toml")); q.add_argument("--resume",action="store_true"); q.add_argument("--detach",action="store_true")
 p_snapshot=sub.add_parser("snapshot-preview"); p_snapshot.add_argument("--motherlode-root",type=Path,required=True); p_snapshot.add_argument("--output-root",type=Path,required=True); p_snapshot.add_argument("--everbar-checkout",type=Path,required=True); p_snapshot.add_argument("--motherlode-sha",required=True)
 p_prefetch=sub.choices["prefetch"]; p_prefetch.add_argument("--workers",type=int,default=3)
 p_monitor=sub.choices["monitor"]; p_monitor.add_argument("--interval",type=int,default=300); p_monitor.add_argument("--pid",type=int)
 p_shard=sub.choices["shard"]; p_shard.add_argument("--dataset",action="append",required=True); p_shard.add_argument("--partition-index",type=int,default=0); p_shard.add_argument("--partitions",type=int,default=1)
 p_sample=sub.choices["sample-brick3"]; p_sample.add_argument("--dataset",action="append",required=True); p_sample.add_argument("--limit",type=int,default=64)
 p_distributed=sub.choices["distributed-shard"]; p_distributed.add_argument("--dataset",required=True); p_distributed.add_argument("--shard-index",type=int,required=True); p_distributed.add_argument("--shard-count",type=int,required=True); p_distributed.add_argument("--run-id",required=True); p_distributed.add_argument("--input-uri",default=""); p_distributed.add_argument("--output-uri",required=True); p_distributed.add_argument("--force",action="store_true")
 p_verify=sub.choices["verify-distributed-run"]; p_verify.add_argument("--dataset",required=True); p_verify.add_argument("--shard-count",type=int,required=True); p_verify.add_argument("--run-id",required=True); p_verify.add_argument("--output-uri",required=True)
 p_features=sub.choices["extract-features"]; p_features.add_argument("--extractor-id",default="primitive-v1")
 p_project=sub.choices["project-v2"]; p_project.add_argument("--canonical-db",type=Path,required=True); p_project.add_argument("--output-dir",type=Path,required=True)
 p_characterize=sub.choices["characterize-v2"]; p_characterize.add_argument("--canonical-db",type=Path,required=True); p_characterize.add_argument("--output-dir",type=Path,required=True); p_characterize.add_argument("--limit-streams",type=int); p_characterize.add_argument("--projection-dir",type=Path)
 a=p.parse_args(argv)
 if a.cmd=="snapshot-preview":
  from .snapshot import build
  print(__import__('json').dumps(build(motherlode_root=a.motherlode_root,output_root=a.output_root,everbar_checkout=a.everbar_checkout,motherlode_sha=a.motherlode_sha),indent=2)); return 0
 cfg=config(a.config)
 if a.cmd=="preflight": print(__import__('json').dumps(preflight(a.root,cfg),indent=2)); return 0
 if a.cmd=="status": print(__import__('json').dumps(progress(a.root,cfg),indent=2)); return 0
 if a.cmd=="reconcile":
  from .core import reconcile
  print(__import__('json').dumps(reconcile(a.root,cfg),indent=2)); return 0
 if a.cmd=="prefetch":
  if a.detach:
   log=a.root/"logs"/"prefetch.log"; f=log.open("a"); child=subprocess.Popen([sys.executable,"-m","everbar_motherlode.cli","prefetch","--root",str(a.root),"--config",str(a.config.resolve()),"--workers",str(a.workers)],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
   writej(a.root/"progress"/"prefetch-launch.json",{"pid":child.pid,"workers":a.workers,"log_path":str(log),"progress_path":str(a.root/'progress/prefetch.json')}); print(child.pid); return 0
  print(__import__('json').dumps(prefetch(a.root,cfg,a.workers),indent=2)); return 0
 if a.cmd=="monitor":
  if a.detach:
   log=a.root/"logs"/"monitor.log"; f=log.open("a"); child=subprocess.Popen([sys.executable,"-m","everbar_motherlode.cli","monitor","--root",str(a.root),"--config",str(a.config.resolve()),"--interval",str(a.interval),*( ["--pid",str(a.pid)] if a.pid else [])],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
   writej(a.root/"progress"/"monitor-launch.json",{"pid":child.pid,"watched_pid":a.pid,"interval_seconds":a.interval,"log_path":str(log),"progress_path":str(a.root/'progress/current.json')}); print(child.pid); return 0
  print(__import__('json').dumps(monitor(a.root,cfg,a.interval,a.pid),indent=2)); return 0
 if a.cmd=="shard":
  if a.detach:
   label="-".join(a.dataset)+f"-part-{a.partition_index}-of-{a.partitions}"; log=a.root/"logs"/("shard-"+label+".log"); f=log.open("a"); child=subprocess.Popen([sys.executable,"-m","everbar_motherlode.cli","shard","--root",str(a.root),"--config",str(a.config.resolve()),"--partition-index",str(a.partition_index),"--partitions",str(a.partitions),*[part for dataset in a.dataset for part in ("--dataset",dataset)]],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
   writej(a.root/"progress"/"shards"/(label+"-launch.json"),{"pid":child.pid,"datasets":a.dataset,"partition_index":a.partition_index,"partitions":a.partitions,"log_path":str(log)}); print(child.pid); return 0
  print(__import__('json').dumps(shard(a.root,cfg,a.dataset,a.partition_index,a.partitions),indent=2)); return 0
 if a.cmd=="merge-shards":
  print(__import__('json').dumps(merge_shards(a.root,cfg),indent=2)); return 0
 if a.cmd=="sample-brick3":
  print(__import__('json').dumps(sample_brick3(a.root,cfg,a.dataset,a.limit),indent=2)); return 0
 if a.cmd=="distributed-shard":
  print(__import__('json').dumps(distributed_shard(a.root,a.config,a.dataset,a.shard_index,a.shard_count,a.run_id,a.input_uri,a.output_uri,a.force),indent=2)); return 0
 if a.cmd=="verify-distributed-run":
  report=verify_distributed_run(a.output_uri,a.run_id,a.dataset,a.shard_count); print(__import__('json').dumps(report,indent=2)); return 0 if report["state"] == "COMPLETE" else 1
 if a.cmd=="backfill-canonical": print(__import__('json').dumps(backfill_canonical(a.root),indent=2)); return 0
 if a.cmd=="extract-features": print(__import__('json').dumps(extract_primitive_features(a.root,a.extractor_id),indent=2)); return 0
 if a.cmd=="project-v2":
  conn=__import__('sqlite3').connect(f"file:{a.canonical_db}?mode=ro", uri=True)
  rows=[]; segments=[]
  for (stream_id,) in conn.execute("select stream_id from canonical_streams order by stream_id"):
   projected, found=project_stream(conn, stream_id); rows.extend(projected); segments.extend(found)
  conn.close(); print(__import__('json').dumps(write_projection(rows, segments, a.output_dir), indent=2, sort_keys=True)); return 0
 if a.cmd=="characterize-v2":
  conn=__import__('sqlite3').connect(f"file:{a.canonical_db}?mode=ro", uri=True)
  ids=None if a.limit_streams is None else [row[0] for row in conn.execute("select stream_id from canonical_streams order by stream_id limit ?", (a.limit_streams,))]
  rows=extract_rows(conn, stream_ids=ids); conn.close()
  if a.projection_dir is not None:
   from types import SimpleNamespace
   projection_path=a.projection_dir / "bars.jsonl"
   projection=[SimpleNamespace(**__import__('json').loads(line)) for line in projection_path.read_text().splitlines()]
   rows=attach_projection(rows, projection)
  print(__import__('json').dumps(write_feature_view(rows, a.output_dir), indent=2, sort_keys=True)); return 0
 init(a.root,cfg)
 if a.detach:
  log=a.root/"logs"/"build.log"; f=log.open("a"); child=subprocess.Popen([sys.executable,"-m","everbar_motherlode.cli","build","--root",str(a.root),"--config",str(a.config.resolve()),"--resume"],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  repo_sha=os.environ.get("MOTHERLODE_REPO_SHA") or os.popen("git rev-parse HEAD").read().strip()
  writej(a.root/"progress"/"launch.json",{"pid":child.pid,"repo_sha":repo_sha,"config_sha256":hashlib.sha256(a.config.read_bytes()).hexdigest(),"log_path":str(log),"progress_path":str(a.root/'progress/current.json'),"terminal_receipt_path":str(a.root/'progress/terminal.json')}); print(child.pid); return 0
 out=run(a.root,cfg); writej(a.root/"progress"/"terminal.json",out); print(__import__('json').dumps(out)); return 0
if __name__=="__main__": raise SystemExit(main())
