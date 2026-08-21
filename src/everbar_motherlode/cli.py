from __future__ import annotations
import argparse, hashlib, os, subprocess, sys
from pathlib import Path
from .core import config, init, preflight, progress, run, writej
def main(argv=None):
 p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="cmd",required=True)
 for x in ("preflight","build","status"): q=sub.add_parser(x); q.add_argument("--root",type=Path,required=True); q.add_argument("--config",type=Path,default=Path("configs/motherlode-v1.toml")); q.add_argument("--resume",action="store_true"); q.add_argument("--detach",action="store_true")
 a=p.parse_args(argv); cfg=config(a.config)
 if a.cmd=="preflight": print(__import__('json').dumps(preflight(a.root,cfg),indent=2)); return 0
 if a.cmd=="status": print(__import__('json').dumps(progress(a.root,cfg),indent=2)); return 0
 init(a.root,cfg)
 if a.detach:
  log=a.root/"logs"/"build.log"; f=log.open("a"); child=subprocess.Popen([sys.executable,"-m","everbar_motherlode.cli","build","--root",str(a.root),"--config",str(a.config.resolve()),"--resume"],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  writej(a.root/"progress"/"launch.json",{"pid":child.pid,"repo_sha":os.popen("git rev-parse HEAD").read().strip(),"config_sha256":hashlib.sha256(a.config.read_bytes()).hexdigest(),"log_path":str(log),"progress_path":str(a.root/'progress/current.json'),"terminal_receipt_path":str(a.root/'progress/terminal.json')}); print(child.pid); return 0
 out=run(a.root,cfg); writej(a.root/"progress"/"terminal.json",out); print(__import__('json').dumps(out)); return 0
if __name__=="__main__": raise SystemExit(main())
