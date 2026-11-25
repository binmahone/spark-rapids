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

#!/usr/bin/env python3
"""Retain this workstream's diagnostic evidence before removing its scratch data."""
import argparse,hashlib,json,os,shlex,subprocess
from pathlib import Path
ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((ROOT/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
remote=owner['remote'];ssh=['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config','-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4']
p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['retain','cleanup']);p.add_argument('--labels',nargs='+',default=['trial-q9-0356','trial-preagg-0356','trial-q15-retry-0356','trial-q9-reuse-0356','trial-q9-reuse-control-0356','trial-q15-0356']);a=p.parse_args()
def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as stream:
  for b in iter(lambda:stream.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def execute(program):
 r=subprocess.run(ssh+['python3 -'],input=program.replace('REMOTE',repr(remote)).replace('TRIAL_LABELS',repr(a.labels)),text=True,capture_output=True,check=True,timeout=240)
 return json.loads(r.stdout)
archive=ROOT/'retained/time-trial-evidence.tar.gz';receipt=ROOT/'evidence/time-trial-retention.json'
if a.action=='retain':
 reports=['time-trial-comparison.json','time-trial-audit.json']
 subprocess.run(ssh+['mkdir -p '+shlex.quote(remote+'/evidence/time-trial-final')],check=True)
 subprocess.run(['scp','-F',ssh[2],*[str(ROOT/'evidence'/name) for name in reports],
                 'mahonem@10.34.1.4:'+remote+'/evidence/time-trial-final/'],check=True)
 d=execute(r'''
from pathlib import Path
import hashlib,json,os,tarfile
w=Path(REMOTE);assert w.stat().st_uid==os.getuid()
labels=TRIAL_LABELS
target=w/'time-trial-evidence.tar.gz'
assert not target.exists()
files=set()
for label in labels:
 for folder in [w/'results'/label,w/'evidence'/label]:
  files.update(p for p in folder.rglob('*') if p.is_file())
 for run in (w/'results'/label).glob('*'):
  if not run.is_dir():continue
  scratch=w/'scratch'/run.name
  files.update(p for p in (scratch/'events').rglob('*') if p.is_file())
  files.update(p for p in (scratch/'suite').rglob('*') if p.is_file() and (p.name in ['stdout','stderr','gpu-inventory.json'] or p.suffix=='.log'))
for folder in ['evidence/time-trial-final']:
 files.update(p for p in (w/folder).rglob('*') if p.is_file())
for query in [3,9,10,15]:
 files.update((w/'evidence/suite-references').glob(f'q{query}*.json'))
for name in ['evidence/owner.json','evidence/deployment.json','evidence/catalog-statistics.json','artifacts/manifest.json','evidence/trial-q15-startup-hang.json','evidence/time-trials-launch.log','evidence/q15-retry-launch.log','evidence/q9-reuse-launch.log','evidence/q9-reuse-control-launch.log']:
 if (w/name).exists():files.add(w/name)
with tarfile.open(target,'w:gz',compresslevel=3) as tar:
 for path in sorted(files):tar.add(path,arcname=str(path.relative_to(w)),recursive=False)
h=hashlib.sha256()
with target.open('rb') as stream:
 for data in iter(lambda:stream.read(1024*1024),b''):h.update(data)
print(json.dumps({'remote_archive':str(target),'sha256':h.hexdigest(),'archive_bytes':target.stat().st_size,'members':len(files)}))
''')
 subprocess.run(['scp','-F',ssh[2],'mahonem@10.34.1.4:'+d['remote_archive'],str(archive)],check=True)
 assert digest(archive)==d['sha256']
 d.update(local_archive=str(archive),local_sha256_verified=True)
 receipt.write_text(json.dumps(d,indent=2)+'\n');print(json.dumps(d))
else:
 d=json.loads(receipt.read_text());assert d['local_sha256_verified'] and digest(archive)==d['sha256']
 q=json.loads((ROOT/'evidence/time-trial-audit.json').read_text());assert not q['remaining_recorded_service_processes']
 report=execute(r'''
from pathlib import Path
import json,os,shutil,subprocess,sys
w=Path(REMOTE);assert w.stat().st_uid==os.getuid()
sys.path[:0]=[str(w/'source/benchmarks/scaleup_reference/scripts'),str(w/'source/benchmarks/scaleup_reference/experiments/b200_memory')]
import b200_services as services
for p in w.glob('results/*/*/applications/suite/driver-service.json'):
 for record in json.loads(p.read_text()):assert not services.group_members(record)
for p in w.glob('results/*/*/services/*/*.json'):
 if p.name.endswith('.stopped.json'):continue
 for record in json.loads(p.read_text()):assert not services.group_members(record)
def size():return int(subprocess.check_output(['du','-sx','--block-size=1',str(w)],text=True).split()[0])
before=size();removed=[]
path=w/'scratch'
if path.exists():shutil.rmtree(path);removed.append(str(path))
path=w/'cpu-source'
if path.exists():
 assert not subprocess.check_output(['git','-C',str(path),'status','--porcelain'],text=True).strip()
 subprocess.run(['git','-C',str(w/'source'),'worktree','remove',str(path)],check=True)
 removed.append(str(path))
path=w/'time-trial-evidence.tar.gz'
if path.exists():path.unlink();removed.append(str(path))
after=size();free=os.statvfs(w).f_bavail*os.statvfs(w).f_frsize
print(json.dumps({'before_allocated_bytes':before,'after_allocated_bytes':after,'recovered_bytes':before-after,
 'removed':removed,'root_free_bytes_after':free,'dataset_modified':False}))
''')
 (ROOT/'evidence/time-trial-cleanup.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
