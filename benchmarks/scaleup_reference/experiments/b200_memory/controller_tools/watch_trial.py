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

"""Watch retained trial progress without altering the running application."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id'] == os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('label')
args = parser.parse_args()
assert re.fullmatch('[A-Za-z0-9_-]+', args.label)
program = r'''
from pathlib import Path
import json
w=Path(REMOTE);p=w/'results'/LABEL
m=json.loads((p/'matrix.json').read_text());out={'status':m['status'],'error':m.get('error'),'samples':[],'apps':[]}
for f in sorted(p.glob('*/applications/suite/result.json')):
 d=json.loads(f.read_text());out['apps'].append({'arm':f.parents[2].name,'status':d['status'],'error':d.get('error')})
 for s in d.get('samples',[]):
  out['samples'].append({k:s[k] for k in ('application_id','probe_arm','query','iteration','sql_seconds','validation')})
print(json.dumps(out))
'''.replace('REMOTE', repr(owner['remote'])).replace('LABEL', repr(args.label))
ssh = ['ssh', '-F', '/home/nvidia/mahone_gluten/cluster_ssh_config', '-o', 'BatchMode=yes',
       '-o', 'ConnectTimeout=8', 'mahonem@10.34.1.4', 'python3 -']
seen = set()
heartbeat = 0
deadline = time.monotonic() + 7200
while True:
    result = subprocess.run(ssh, input=program, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(result.stderr)
    data = json.loads(result.stdout)
    for sample in data['samples']:
        key = (sample['application_id'], sample['query'], sample['iteration'])
        if key not in seen:
            print(sample['probe_arm'], 'Q'+str(sample['query']), sample['iteration'],
                  round(sample['sql_seconds'], 3), sample['validation']['status'], flush=True)
            seen.add(key)
    if time.monotonic() >= heartbeat:
        print('progress', len(seen), data['status'], data['apps'], flush=True)
        heartbeat = time.monotonic() + 30
    if data['status'] != 'running':
        if data['status'] != 'completed':
            raise RuntimeError(data['error'])
        break
    if time.monotonic() >= deadline:
        raise TimeoutError('Progress watcher deadline reached; application left untouched')
    time.sleep(10)
