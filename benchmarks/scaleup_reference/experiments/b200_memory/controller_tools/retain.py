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

"""Retain compact runtime evidence while excluding native extracts and build trees."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

root=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
remote=owner['remote'];target=root/'retained';target.mkdir(exist_ok=True)
ssh=['ssh','-F',os.environ['B200_SSH_CONFIG'],'-o','BatchMode=yes',
     '-o','ConnectTimeout=8','mahonem@10.34.1.4']
program=r'''
from pathlib import Path
import hashlib,json,os,tarfile
w=Path(REMOTE);assert (w/'evidence/probe.exit').exists()
archive=w/'runtime-evidence.tar.gz'
assert not archive.exists()
with tarfile.open(archive,'w:gz') as t:
    for root in ['evidence','results']:
        t.add(w/root,arcname=root)
    for p in (w/'scratch').rglob('*'):
        if p.is_file() and (p.name in ['stdout','stderr','worker.log','master.log'] or p.parent.name=='events'):
            t.add(p,arcname=str(p.relative_to(w)))
record={'bytes':archive.stat().st_size,'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
        'source_revision':os.popen('git -C '+str(w/'source')+' rev-parse HEAD').read().strip(),
        'probe_exit':(w/'evidence/probe.exit').read_text().strip()}
(w/'retention.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record))
'''.replace('REMOTE',repr(remote))
reply=subprocess.run(ssh+['python3 -'],input=program,text=True,capture_output=True,check=True,timeout=180)
record=json.loads(reply.stdout)
subprocess.run(['scp','-F',os.environ['B200_SSH_CONFIG'],
    'mahonem@10.34.1.4:'+remote+'/runtime-evidence.tar.gz',str(target)],check=True)
archive=target/'runtime-evidence.tar.gz'
assert hashlib.sha256(archive.read_bytes()).hexdigest()==record['sha256']
(root/'evidence/retention.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record,indent=2))
