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

"""Archive completed trial evidence and remove only those trials' stopped scratch trees."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id'] == os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--name', required=True)
parser.add_argument('labels', nargs='+')
args = parser.parse_args()
assert all(re.fullmatch('[A-Za-z0-9_-]+', x) for x in [args.name, *args.labels])
ssh = ['ssh', '-F', '/home/nvidia/mahone_gluten/cluster_ssh_config',
       '-o', 'BatchMode=yes', 'mahonem@10.34.1.4']
program = r'''
from pathlib import Path
import hashlib,json,os,sys,tarfile
w=Path(REMOTE)
assert w.stat().st_uid==os.getuid()
sys.path[:0]=[str(w/'source/benchmarks/scaleup_reference/scripts'),
              str(w/'source/benchmarks/scaleup_reference/experiments/b200_memory')]
import b200_services as services
files=set();scratch=[]
for label in LABELS:
 assert (w/'evidence'/label/'controller.exit').exists()
 for state in (w/'results'/label).glob('*/applications/suite/driver-service.json'):
  for record in json.loads(state.read_text()):assert not services.group_members(record)
 for state in (w/'results'/label).glob('*/services/*/*.json'):
  if state.name.endswith('.stopped.json'):continue
  for record in json.loads(state.read_text()):assert not services.group_members(record)
 for folder in [w/'results'/label,w/'evidence'/label]:
  files.update(p for p in folder.rglob('*') if p.is_file())
 for run in (w/'results'/label).iterdir():
  if not run.is_dir():continue
  path=w/'scratch'/run.name
  scratch.append(str(path))
  files.update(p for p in (path/'events').rglob('*') if p.is_file())
  files.update(p for p in (path/'suite').rglob('*') if p.is_file() and
               (p.name in ('stdout','stderr','gpu-inventory.json') or p.suffix=='.log'))
for name in ['evidence/owner.json','evidence/deployment.json','evidence/catalog-statistics.json',
             'artifacts/manifest.json','evidence/gds-wiring-NativeFileRangeReader.txt',
             'evidence/gds-wiring-HybridScanRangeReader.txt',
             'evidence/integer-domains.json','evidence/integer-domain-audit.log']:
 if (w/name).exists():files.add(w/name)
files.update(p for p in (w/'evidence/suite-references').rglob('*') if p.is_file())
target=w/(NAME+'.tar.gz');assert not target.exists()
with tarfile.open(target,'w:gz',compresslevel=3) as tar:
 for path in sorted(files):tar.add(path,arcname=str(path.relative_to(w)),recursive=False)
digest=hashlib.sha256()
with target.open('rb') as stream:
 for data in iter(lambda:stream.read(1024*1024),b''):digest.update(data)
sha=digest.hexdigest()
print(json.dumps({'archive':str(target),'sha256':sha,'bytes':target.stat().st_size,
                  'members':len(files),'stopped_scratch_trees':scratch}))
'''.replace('REMOTE', repr(owner['remote'])).replace('LABELS', repr(args.labels)).replace('NAME', repr(args.name))
result = subprocess.run(ssh+['python3 -'], input=program, text=True,
                        capture_output=True, check=True, timeout=240)
receipt = json.loads(result.stdout)
local = root/'retained'/(args.name+'.tar.gz')
assert not local.exists()
subprocess.run(['scp', '-F', ssh[2], 'mahonem@10.34.1.4:'+receipt['archive'], str(local)], check=True)
with local.open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == receipt['sha256']
receipt['local_archive'] = str(local)
receipt['local_sha256_verified'] = True
receipt['labels'] = args.labels
path = root/'evidence'/(args.name+'-retention.json')
path.write_text(json.dumps(receipt, indent=2)+'\n')
cleanup = r'''
import json,os,shutil,subprocess
from pathlib import Path
w=Path(REMOTE)
removed_bytes=0
for value in SCRATCH:
 p=Path(value);assert p.parent==w/'scratch' and p.stat().st_uid==os.getuid()
 removed_bytes+=int(subprocess.check_output(['du','-sx','--block-size=1',str(p)]).split()[0])
 shutil.rmtree(p)
Path(ARCHIVE).unlink()
print(json.dumps({'removed_scratch_allocated_bytes':removed_bytes,
                  'root_free_bytes':os.statvfs(w).f_bavail*os.statvfs(w).f_frsize}))
'''.replace('REMOTE', repr(owner['remote'])).replace('SCRATCH', repr(receipt['stopped_scratch_trees'])).replace('ARCHIVE', repr(receipt['archive']))
result = subprocess.run(ssh+['python3 -'], input=cleanup, capture_output=True,
                        text=True, check=True, timeout=120)
receipt['cleanup'] = json.loads(result.stdout)
path.write_text(json.dumps(receipt, indent=2)+'\n')
print(json.dumps(receipt), flush=True)
