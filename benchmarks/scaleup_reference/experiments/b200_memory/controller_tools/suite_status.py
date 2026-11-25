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
"""Read the isolated B200 all-query diagnostics without changing running work."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((ROOT/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('labels',nargs='*',default=['all22-baseline'])
p.add_argument('--analyze',action='store_true')
a=p.parse_args()
program=r'''
from pathlib import Path
import collections,json,os,re
w=Path(REMOTE)
report={'classification':'four-GPU SF10K float opportunity diagnostics',
        'independent_cpu_reference':False,'matrices':[],'rows':[]}
for label in LABELS:
 matrix=w/'results'/label/'matrix.json'
 if not matrix.exists():continue
 m=json.loads(matrix.read_text());report['matrices'].append(m)
 for app in sorted(matrix.parent.glob('*/applications/suite')):
  result=app/'result.json'
  if not result.exists():continue
  d=json.loads(result.read_text());run=app.parents[1]
  conf=d.get('effective_conf',{})
  assert conf.get('spark.executor.instances')=='4'
  assert 'spark.rapids.memory.gpu.allocSize' not in conf
  stages=collections.defaultdict(collections.Counter);groups=collections.defaultdict(set)
  if ANALYZE:
   events=list((w/'scratch'/run.name/'events').glob('app-*'))
   for event in events:
    if not event.is_file():continue
    with event.open('rb') as stream:
     for raw in stream:
      try:
       if b'SparkListenerJobStart' in raw:
        v=json.loads(raw);group=(v.get('Properties') or {}).get('spark.jobGroup.id')
        for stage in v['Stage IDs']:groups[stage].add(group)
       elif b'SparkListenerTaskEnd' in raw:
        v=json.loads(raw);s=stages[v['Stage ID']];metrics=v.get('Task Metrics') or {}
        s['tasks']+=1
        s['success_tasks']+=int(v.get('Task End Reason',{}).get('Reason')=='Success')
        for dest,key in [('memory_spill_bytes','Memory Bytes Spilled'),('disk_spill_bytes','Disk Bytes Spilled'),('executor_cpu_ns','Executor CPU Time'),('jvm_gc_ms','JVM GC Time')]:s[dest]+=metrics.get(key,0)
        s['shuffle_write_bytes']+=(metrics.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written',0)
      except json.JSONDecodeError:continue
  for sample in d.get('samples',[]):
   row=dict(sample,label=label,arm=run.name)
   counter=collections.Counter();group=f'q{sample["query"]}-iteration{sample["iteration"]}'
   for stage,group_set in groups.items():
    if group in group_set:counter.update(stages[stage])
   row['task_metrics']=dict(counter)
   row['shared_stages']={str(stage):sorted(str(g) for g in group_set) for stage,group_set in groups.items() if group in group_set and len(group_set)>1}
   row['plan_text']=(app/f'q{sample["query"]}-iteration{sample["iteration"]}.executed.txt').read_text()
   report['rows'].append(row)
  report.setdefault('applications',[]).append({'path':str(app),'status':d['status'],'error':d.get('error'),'last_log_lines':(app/'driver.log').read_text(errors='replace').splitlines()[-3:]})
report['root_free_bytes']=os.statvfs(w).f_bavail*os.statvfs(w).f_frsize
print(json.dumps(report))
'''.replace('REMOTE',repr(owner['remote'])).replace('LABELS',repr(a.labels)).replace('ANALYZE',repr(a.analyze))
ssh=['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config','-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
r=subprocess.run(ssh,input=program,text=True,capture_output=True,check=True,timeout=120)
d=json.loads(r.stdout)
if a.analyze:
 path=ROOT/'evidence'/('suite-'+ '-'.join(a.labels)+'.json')
 path.write_text(json.dumps(d,indent=2)+'\n')
for matrix in d['matrices']:print(matrix['status'],matrix.get('error'),[(x['arm'],x['status']) for x in matrix['arms']])
for row in d['rows']:
 m=row['task_metrics'];print(row['arm'],row['query'],row['iteration'],round(row['sql_seconds'],3),row['validation']['status'],
   {k:round(m.get(k,0)/1e9,3) for k in ('shuffle_write_bytes','memory_spill_bytes','disk_spill_bytes')} if a.analyze else '')
for app in d.get('applications',[]):print(app['status'],app['error'],app['last_log_lines'][-1:])
print('root_free_GiB',round(d['root_free_bytes']/2**30,2))
