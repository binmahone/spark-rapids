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

"""Remove bounded audit scratch after successful process and archive verification."""
import json
import os
from pathlib import Path
import subprocess

root=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
audit=json.loads((root/'evidence/next-final-audit.json').read_text())
assert not audit['remaining_owned_prefix_processes']
assert not audit['remaining_recorded_service_processes']
assert all(a['status']=='completed' for a in audit['applications'])
retentions=[json.loads((root/'evidence'/(name+'-retention.json')).read_text()) for name in
            ('next-screen-evidence','next-routing-evidence','next-final-evidence')]
assert all(r['local_sha256_verified'] and Path(r['local_archive']).stat().st_size==r['bytes']
           for r in retentions)
program=r'''
import json,os,shutil,subprocess
from pathlib import Path
w=Path(REMOTE)
assert w.stat().st_uid==os.getuid()
assert json.loads((w/'evidence/owner.json').read_text())['thread_id']==THREAD
for proc in Path('/proc').iterdir():
 if not proc.name.isdigit():continue
 try:
  if proc.stat().st_uid!=os.getuid():continue
  if str(w).encode() in (proc/'cmdline').read_bytes():
   assert (proc/'stat').read_text().split(') ',1)[1].split()[0]=='Z',proc.name
 except (FileNotFoundError,ProcessLookupError):pass
scratch=w/'scratch'
allowed={'cuda-cache','domain-audit','integer-domain-boundaries'}
entries=list(scratch.iterdir()) if scratch.exists() else []
assert {p.name for p in entries}<=allowed,[p.name for p in entries]
assert not subprocess.check_output(['git','-C',str(w/'source'),'status','--porcelain'],text=True).strip()
subprocess.run(['git','-C',str(w/'source'),'pull','--ff-only'],check=True,capture_output=True,text=True)
removed=[];removed_bytes=0
for p in entries:
 assert p.parent==scratch and p.stat().st_uid==os.getuid() and p.is_dir() and not p.is_symlink()
 removed_bytes+=int(subprocess.check_output(['du','-sx','--block-size=1',str(p)],text=True).split()[0])
 shutil.rmtree(p);removed.append(str(p))
if scratch.exists():scratch.rmdir()
dataset=Path('/raid/knataraj/datasets/tpch/sf10k_v2_float')
files=list(dataset.glob('*/*.parquet'))
assert len(files)==868 and sum(p.stat().st_size for p in files)==3218283517762
report={'removed_paths':removed,'removed_allocated_bytes':removed_bytes,
 'root_free_bytes':os.statvfs(w).f_bavail*os.statvfs(w).f_frsize,
 'remaining_owned_allocated_bytes':int(subprocess.check_output(['du','-sx','--block-size=1',str(w)],text=True).split()[0]),
 'source_revision':subprocess.check_output(['git','-C',str(w/'source'),'rev-parse','HEAD'],text=True).strip(),
 'remaining_owned_prefix_processes':[],'dataset_parquet_files':len(files),
 'dataset_parquet_bytes':sum(p.stat().st_size for p in files)}
(w/'evidence/next-storage-final.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
'''.replace('REMOTE',repr(owner['remote'])).replace('THREAD',repr(owner['thread_id']))
result=subprocess.run(['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config',
    '-o','BatchMode=yes','mahonem@10.34.1.4','python3 -'],input=program,text=True,
    capture_output=True,check=True,timeout=120)
report=json.loads(result.stdout)
report['trial_scratch_removed_bytes']=sum(r['cleanup']['removed_scratch_allocated_bytes'] for r in retentions)
(root/'evidence/next-storage-final.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
