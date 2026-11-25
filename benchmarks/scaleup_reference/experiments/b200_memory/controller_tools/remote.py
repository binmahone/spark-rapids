#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Control only this fork's B200 runtime and retained evidence."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((ROOT/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
remote=owner['remote']
ssh=['ssh','-F',os.environ['B200_SSH_CONFIG'],'-o','BatchMode=yes',
     '-o','ConnectTimeout=8','mahonem@10.34.1.4']
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('action',choices=['sync','prepare','start','status','stop','collect','archive-failed'])
args=parser.parse_args()
here=remote+'/source/benchmarks/scaleup_reference/experiments/b200_memory'
if args.action=='sync':
    subprocess.run(ssh+[f'git -C {remote}/source pull --ff-only && git -C {remote}/source status --porcelain'],check=True)
elif args.action in ['prepare','start']:
    script='prepare.sh' if args.action=='prepare' else 'start_probe.sh'
    log='prepare' if args.action=='prepare' else 'probe'
    subprocess.run(ssh+[f'nohup bash {here}/{script} {remote} > {remote}/evidence/{log}-launch.log '
        f'2>&1 < /dev/null & echo $! > {remote}/evidence/{log}.pid'],check=True)
elif args.action=='stop':
    subprocess.run(ssh+[f'bash {here}/stop_probe.sh {remote}'],check=True)
elif args.action=='archive-failed':
    program=f'''from pathlib import Path
import datetime,json,os,shutil,sys
w=Path("{remote}")
assert w.stat().st_uid==os.getuid()
sys.path.insert(0,"{here}")
sys.path.insert(0,"{remote}/source/benchmarks/scaleup_reference/scripts")
import b200_services as services
assert (w/"evidence/probe.exit").read_text().strip()!="0"
for p in (w/"results").glob("*/*/services/*/*.json"):
 if not p.name.endswith(".stopped.json"):
  for record in json.loads(p.read_text()):assert not services.group_members(record),str(p)
for p in (w/"results").glob("*/*/applications/suite/driver-service.json"):
 for record in json.loads(p.read_text()):assert not services.group_members(record),str(p)
target=w/"failed"/datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
target.mkdir(parents=True)
for name in ["results","scratch"]:
 p=w/name
 if p.exists():shutil.move(str(p),str(target/name))
for name in ["probe.log","probe.exit","host-monitor.jsonl","host-prerequisites.json"]:
 p=w/"evidence"/name
 if p.exists():shutil.move(str(p),str(target/name))
print(target)
'''
    subprocess.run(ssh+['python3 -'],input=program,text=True,check=True)
elif args.action=='status':
    program=f'''from pathlib import Path
import json
w=Path("{remote}")
for name in ["prepare.exit","probe.exit"]:
 p=w/"evidence"/name
 print(name,p.read_text().strip() if p.exists() else "pending")
for name in ["prepare.log","probe.log"]:
 p=w/"evidence"/name
 if p.exists():print(name,"\\n".join(p.read_text(errors="replace").splitlines()[-8:]))
for p in sorted((w/"results").glob("*/matrix.json")):
 d=json.loads(p.read_text());print(str(p),d['status'],[(x['arm'],x['status']) for x in d.get('arms',[])],d.get('error'))
for p in sorted((w/"results").glob("*/*/applications/suite/result.json")):
 d=json.loads(p.read_text());print(str(p),d["status"],[(s["query"],s["iteration"],round(s["sql_seconds"],3)) for s in d.get("samples",[])],d.get("error"))
'''
    subprocess.run(ssh+['python3 -'],input=program,text=True,check=True)
else:
    target=ROOT/'retained';target.mkdir(exist_ok=True)
    subprocess.run(['scp','-r','-F',os.environ['B200_SSH_CONFIG'],
        'mahonem@10.34.1.4:'+remote+'/evidence',str(target)],check=True)
    subprocess.run(['scp','-r','-F',os.environ['B200_SSH_CONFIG'],
        'mahonem@10.34.1.4:'+remote+'/results',str(target)],check=True)
