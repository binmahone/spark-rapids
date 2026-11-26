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

"""Inspect exact timed task windows and physical GPU tails of serialized SQL actions."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--query',type=int,required=True)
parser.add_argument('--runs',nargs='+',required=True,help='LABEL:ARM pairs')
args = parser.parse_args()
runs = [item.split(':') for item in args.runs]
assert all(len(pair)==2 and all(re.fullmatch('[A-Za-z0-9_-]+',s) for s in pair) for pair in runs)
program = r'''
from collections import Counter,defaultdict
import json
from pathlib import Path
w=Path(REMOTE);report=[]
for label,arm in RUNS:
 for app in (w/'results'/label).glob('*/applications/suite'):
  run=app.parents[1];config=json.loads((run/'config.json').read_text())
  if config['probe_arm']!=arm:continue
  d=json.loads((app/'result.json').read_text());assert d['status']=='completed'
  samples=[s for s in d['samples'] if s['query']==QUERY]
  scratch=w/'scratch'/run.name
  gpu={p.parent.name:int(next(x for x in p.parts if x.startswith('worker-')).split('-')[1])
       for p in (scratch/'suite').glob('worker-*/work/*/*/stderr')}
  assert len(gpu)==4,gpu
  tasks=defaultdict(list)
  for event in (scratch/'events').glob('app-*'):
   if not event.is_file():continue
   for raw in event.open('rb'):
    if b'SparkListenerTaskEnd' not in raw:continue
    v=json.loads(raw)
    info=v['Task Info'];finish=info['Finish Time']
    for s in samples:
     lo=s['started_unix_ns']/1e6;hi=s['ended_unix_ns']/1e6
     if not lo<=finish<=hi:continue
     assert v['Task End Reason']['Reason']=='Success'
     assert info['Launch Time']>=lo-1
     m=v.get('Task Metrics') or {}
     tasks[s['iteration']].append({'stage':v['Stage ID'],'launch':info['Launch Time'],
      'finish':finish,'gpu':gpu[info['Executor ID']],
      'gc_ms':m.get('JVM GC Time',0),'run_ms':m.get('Executor Run Time',0),
      'shuffle_bytes':(m.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written',0)})
  for s in samples:
   selected=tasks[s['iteration']];assert selected
   lo=s['started_unix_ns']/1e6;hi=s['ended_unix_ns']/1e6
   stages=[]
   for stage in sorted({t['stage'] for t in selected}):
    group=[t for t in selected if t['stage']==stage]
    start=min(t['launch'] for t in group);end=max(t['finish'] for t in group)
    stages.append({'stage':stage,'tasks':len(group),'start_offset_ms':start-lo,
     'end_offset_ms':end-lo,'task_window_ms':end-start,
     'gc_ms':sum(t['gc_ms'] for t in group),
     'shuffle_bytes':sum(t['shuffle_bytes'] for t in group),
     'gpu_task_counts':dict(Counter(t['gpu'] for t in group)),
     'gpu_last_finish_offset_ms':{index:max(t['finish'] for t in group if t['gpu']==index)-lo
                                for index in sorted({t['gpu'] for t in group})},
     'longest_tasks':sorted(group,key=lambda t:t['finish']-t['launch'],reverse=True)[:4]})
   first=min(t['launch'] for t in selected);last=max(t['finish'] for t in selected)
   report.append({'label':label,'arm':arm,'query':QUERY,'iteration':s['iteration'],
    'sql_seconds':s['sql_seconds'],'before_first_task_ms':first-lo,
    'after_last_task_ms':hi-last,'gc_ms':sum(t['gc_ms'] for t in selected),
    'stages':stages})
print(json.dumps(report))
'''.replace('REMOTE',repr(owner['remote'])).replace('RUNS',repr(runs)).replace('QUERY',repr(args.query))
result = subprocess.run(['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config',
    '-o','BatchMode=yes','mahonem@10.34.1.4','python3 -'],input=program,text=True,
    capture_output=True,check=True,timeout=120)
report={'task_scope':'Successful Spark tasks completing inside the exact query timer, excluding later cleanup tasks; each application executes one query at a time.',
        'causal_limit':'Stage task windows and placement identify where time differs; they do not isolate CUDA kernels or establish a cause.',
        'rows':json.loads(result.stdout)}
target=root/'evidence'/('q'+str(args.query)+'-schedule.json')
assert not target.exists()
target.write_text(json.dumps(report,indent=2)+'\n')
for r in report['rows']:
    print(r['arm'],r['iteration'],round(r['sql_seconds'],3),'pre/post_task_ms',
          round(r['before_first_task_ms']),round(r['after_last_task_ms']),
          'gc_ms',r['gc_ms'],'largest_windows',
          [(s['stage'],s['tasks'],round(s['task_window_ms']),s['gpu_task_counts'])
           for s in sorted(r['stages'],key=lambda s:s['task_window_ms'],reverse=True)[:4]])
print(target)
