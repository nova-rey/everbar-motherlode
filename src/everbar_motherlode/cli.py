from __future__ import annotations
import argparse, hashlib, os, subprocess, sys
from pathlib import Path
from .core import config, init, prefetch, preflight, progress, run, writej
def main(argv=None):
 p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="cmd",required=True)
 for x in ("preflight","build","status","reconcile","prefetch"): q=sub.add_parser(x); q.add_argument("--root",type=Path,required=True); q.add_argument("--config",type=Path,default=Path("configs/motherlode-v1.toml")); q.add_argument("--resume",action="store_true"); q.add_argument("--detach",action="store_true")
 p_prefetch=sub.choices["prefetch"]; p_prefetch.add_argument("--workers",type=int,default=3)
 a=p.parse_args(argv); cfg=config(a.config)
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
 init(a.root,cfg)
 if a.detach:
  log=a.root/"logs"/"build.log"; f=log.open("a"); child=subprocess.Popen([sys.executable,"-m","everbar_motherlode.cli","build","--root",str(a.root),"--config",str(a.config.resolve()),"--resume"],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  repo_sha=os.environ.get("MOTHERLODE_REPO_SHA") or os.popen("git rev-parse HEAD").read().strip()
  writej(a.root/"progress"/"launch.json",{"pid":child.pid,"repo_sha":repo_sha,"config_sha256":hashlib.sha256(a.config.read_bytes()).hexdigest(),"log_path":str(log),"progress_path":str(a.root/'progress/current.json'),"terminal_receipt_path":str(a.root/'progress/terminal.json')}); print(child.pid); return 0
 out=run(a.root,cfg); writej(a.root/"progress"/"terminal.json",out); print(__import__('json').dumps(out)); return 0
if __name__=="__main__": raise SystemExit(main())
