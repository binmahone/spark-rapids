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

"""Remove owned build and Spark scratch after verified local evidence retention."""
import json
import os
from pathlib import Path
import subprocess

root=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
qualification=json.loads((root/'evidence/qualification.json').read_text())
retention=json.loads((root/'evidence/retention.json').read_text())
assert qualification['query_execution_status']=='passed'
assert qualification['remaining_owned_service_processes']==[]
assert retention['probe_exit']=='0'
program=r'''
from pathlib import Path
import hashlib,json,os,shutil,subprocess,sys
w=Path(REMOTE)
assert w.stat().st_uid==os.getuid() and w.name=='mahonem-b200-261125-01a21183'
owner=json.loads((w/'evidence/owner.json').read_text());assert owner['thread_id']==THREAD
assert hashlib.sha256((w/'runtime-evidence.tar.gz').read_bytes()).hexdigest()==ARCHIVE_HASH
sys.path.insert(0,str(w/'source/benchmarks/scaleup_reference/scripts'))
sys.path.insert(0,str(w/'source/benchmarks/scaleup_reference/experiments/b200_memory'))
import b200_services as services
for p in (w/'results').glob('*/*/services/*/*.json'):
 if not p.name.endswith('.stopped.json'):
  for r in json.loads(p.read_text()):assert not services.group_members(r)
for p in (w/'results').glob('*/*/applications/suite/driver-service.json'):
 for r in json.loads(p.read_text()):assert not services.group_members(r)
before=int(subprocess.check_output(['du','-sx','--block-size=1',str(w)],text=True).split()[0])
removed=[]
for name in ['scratch','failed','ucx-source','runtime/packages','runtime-evidence.tar.gz']:
 p=w/name
 if p.exists():
  if p.is_dir():shutil.rmtree(p)
  else:p.unlink()
  removed.append(name)
after=int(subprocess.check_output(['du','-sx','--block-size=1',str(w)],text=True).split()[0])
report={'before_allocated_bytes':before,'after_allocated_bytes':after,'removed_owned_paths':removed,
        'remaining_owned_service_processes':[],
        'filesystems':subprocess.check_output(['df','-hT','/','/raid'],text=True),
        'gpu_processes':subprocess.check_output(['nvidia-smi','--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory','--format=csv,noheader'],text=True)}
(w/'evidence/cleanup.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
'''.replace('REMOTE',repr(owner['remote'])).replace('THREAD',repr(owner['thread_id'])).replace('ARCHIVE_HASH',repr(retention['sha256']))
ssh=['ssh','-F',os.environ['B200_SSH_CONFIG'],'-o','BatchMode=yes',
     '-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
reply=subprocess.run(ssh,input=program,text=True,capture_output=True,timeout=120)
if reply.returncode:raise RuntimeError(reply.stderr)
report=json.loads(reply.stdout)
(root/'evidence/cleanup.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
