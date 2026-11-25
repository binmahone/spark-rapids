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

"""Read retained SQL and event evidence without changing the running benchmark."""
import json
import os
from pathlib import Path
import subprocess

root=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
remote=owner['remote']
program=r'''
from pathlib import Path
import collections,json,os
w=Path(REMOTE)
report={'classification':'four B200 GPUs; unchanged reference RMM fractions; SQL hint diagnostic',
        'score_eligible':False,'independent_cpu_reference':False,'rows':[],'matrices':[]}
for p in sorted((w/'results').glob('*/matrix.json')):
    matrix=json.loads(p.read_text());report['matrices'].append(matrix)
for app in sorted((w/'results/default').glob('*/applications/suite')):
    result=json.loads((app/'result.json').read_text())
    run=app.parents[1]
    events=list((w/'scratch'/run.name/'events').glob('app-*'))
    grouped=collections.defaultdict(set);stage=collections.defaultdict(collections.Counter)
    if events:
        for raw in events[0].open('rb'):
            try:
                if b'SparkListenerJobStart' in raw:
                    v=json.loads(raw);group=(v.get('Properties') or {}).get('spark.jobGroup.id')
                    for sid in v['Stage IDs']:grouped[sid].add(group)
                elif b'SparkListenerTaskEnd' in raw:
                    v=json.loads(raw);m=v.get('Task Metrics') or {};counter=stage[v['Stage ID']]
                    counter['memory_spill_bytes']+=m.get('Memory Bytes Spilled',0)
                    counter['disk_spill_bytes']+=m.get('Disk Bytes Spilled',0)
                    counter['executor_cpu_ns']+=m.get('Executor CPU Time',0)
                    counter['jvm_gc_ms']+=m.get('JVM GC Time',0)
                    counter['shuffle_write_bytes']+=(m.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written',0)
                    counter['tasks']+=1
            except json.JSONDecodeError:
                continue
    conf=result.get('effective_conf',{})
    assert conf.get('spark.executor.instances')=='4'
    assert 'spark.rapids.memory.gpu.allocSize' not in conf
    for s in result.get('samples',[]):
        group='q'+str(s['query'])+'-iteration'+str(s['iteration']);counter=collections.Counter()
        for sid,groups in grouped.items():
            if group in groups:counter.update(stage[sid])
        row={key:s[key] for key in ['query','iteration','sql_seconds','validation','reader_metrics','rows','schema','sql_sha256','reference_sql_sha256','semi_join_plan_lines','probe_arm']}
        row.update(arm=run.name,task_metrics=dict(counter),
                   lineitem_scan_count=sum(x['node'].startswith('GpuScan ') and x['node'].endswith('.lineitem') for x in s['plan_metrics']))
        report['rows'].append(row)
free=os.statvfs(w);report['root_free_bytes']=free.f_bavail*free.f_frsize
print(json.dumps(report))
'''.replace('REMOTE',repr(remote))
ssh=['ssh','-F',os.environ['B200_SSH_CONFIG'],'-o','BatchMode=yes',
     '-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
reply=subprocess.run(ssh,input=program,text=True,capture_output=True,check=True,timeout=120)
report=json.loads(reply.stdout)
(root/'evidence/comparison.json').write_text(json.dumps(report,indent=2)+'\n')
for row in report['rows']:
    t=row['task_metrics']
    print(row['arm'],'Q'+str(row['query']),row['iteration'],
          'SQL_s',round(row['sql_seconds'],3),'read_GB',round(row['reader_metrics']['hybridScanNativeHostBytesRead']/1e9,3),
          'shuffle_GB',round(t.get('shuffle_write_bytes',0)/1e9,3),
          'host_spill_GB',round(t.get('memory_spill_bytes',0)/1e9,3),
          'disk_spill_GB',round(t.get('disk_spill_bytes',0)/1e9,3),
          row['validation']['status'])
print('root_free_GiB',round(report['root_free_bytes']/2**30,2))
