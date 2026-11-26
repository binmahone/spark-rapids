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
p.add_argument('--recent',type=int,default=0)
p.add_argument('--retained-root',type=Path)
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
  sql_plans={};sql_groups={};acc_updates=collections.Counter();jobs=[];sql_roots={};root_groups={}
  if ANALYZE:
   events=list((w/'scratch'/run.name/'events').glob('app-*'))
   if d.get('samples') and not events:raise RuntimeError('Raw event logs are archived; use retained results or restore the evidence archive before analysis')
   for event in events:
    if not event.is_file():continue
    with event.open('rb') as stream:
     for raw in stream:
      try:
       if b'SparkListenerJobStart' in raw:
        v=json.loads(raw);group=(v.get('Properties') or {}).get('spark.jobGroup.id')
        execution=(v.get('Properties') or {}).get('spark.sql.execution.id')
        root=(v.get('Properties') or {}).get('spark.sql.execution.root.id',execution)
        jobs.append((v['Stage IDs'],group,str(execution) if execution is not None else None,
                     str(root) if root is not None else None))
        if root is not None and group and re.fullmatch(r'q\d+-iteration\d+',group):
         assert str(root) not in root_groups or root_groups[str(root)]==group
         root_groups[str(root)]=group
       elif b'SparkListenerSQLExecutionStart' in raw:
        v=json.loads(raw);execution=str(v['executionId']);sql_plans[execution]=v['sparkPlanInfo']
        root=str(v.get('rootExecutionId',v['executionId']));sql_roots[execution]=root
        if re.fullmatch(r'q\d+-iteration\d+',v.get('description','')):
         assert root not in root_groups or root_groups[root]==v['description']
         root_groups[root]=v['description']
       elif b'SparkListenerTaskEnd' in raw:
        v=json.loads(raw);s=stages[v['Stage ID']];metrics=v.get('Task Metrics') or {}
        s['tasks']+=1
        s['success_tasks']+=int(v.get('Task End Reason',{}).get('Reason')=='Success')
        for dest,key in [('memory_spill_bytes','Memory Bytes Spilled'),('disk_spill_bytes','Disk Bytes Spilled'),('executor_cpu_ns','Executor CPU Time'),('jvm_gc_ms','JVM GC Time')]:s[dest]+=metrics.get(key,0)
        for accumulator in v['Task Info'].get('Accumulables',[]):
         if accumulator.get('Metadata')=='sql' and accumulator.get('Update') is not None:
          try:acc_updates[accumulator['ID']]+=int(accumulator['Update'])
          except (ValueError,TypeError):pass
        s['shuffle_write_bytes']+=(metrics.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written',0)
      except json.JSONDecodeError:continue
   for execution,root in sql_roots.items():sql_groups[execution]=root_groups.get(root)
   for stage_ids,group,execution,root in jobs:
    canonical=root_groups.get(root,group)
    for stage in stage_ids:groups[stage].add(canonical)
  for sample in d.get('samples',[]):
   row=dict(sample,label=label,arm=run.name)
   counter=collections.Counter();group=f'q{sample["query"]}-iteration{sample["iteration"]}'
   for stage,group_set in groups.items():
    if group in group_set:counter.update(stages[stage])
   row['task_metrics']=dict(counter)
   row['task_metric_scope']='All jobs in the query SQL root, including asynchronous broadcast jobs with UUID groups; explicit query cleanup jobs included; stage IDs deduplicated.'
   metric_ids={};event_nodes=[];scan_ids=set()
   for execution,execution_group in sql_groups.items():
    if execution_group!=group or execution not in sql_plans:continue
    pending=[sql_plans[execution]]
    while pending:
     node=pending.pop();pending.extend(node.get('children',[]))
     for metric in node.get('metrics',[]):metric_ids[metric['accumulatorId']]=metric
     if 'Scan' in node['nodeName']:
      identity=tuple(sorted(metric['accumulatorId'] for metric in node.get('metrics',[])))
      if identity not in scan_ids:
       scan_ids.add(identity)
       event_nodes.append({'node':node['nodeName'],'simple_string':node.get('simpleString'),
         'metrics':{metric['name']:acc_updates[metric['accumulatorId']] for metric in node.get('metrics',[])}})
   descriptions={
    'hybrid scan splits read':'hybridScanSplits',
    'hybrid scan bytes read to host with KvikIO':'hybridScanNativeHostBytesRead',
    'hybrid scan bytes read to device with GDS, including alignment padding':'hybridScanDeviceBytesRead',
    'hybrid scan bytes read to host instead of with GDS while the GPU was busy':'hybridScanGpuBusyHostBytesRead'}
   all_reader=collections.Counter()
   for aid,metric in metric_ids.items():
    if metric['name'] in descriptions:all_reader[descriptions[metric['name']]]+=acc_updates[aid]
   row['reader_metrics_all_branches']=dict(all_reader)
   shuffle_metrics=[aid for aid,metric in metric_ids.items() if metric['name']=='shuffle bytes written']
   row['sql_accumulator_shuffle_write_bytes']=sum(acc_updates[aid] for aid in shuffle_metrics)
   row['sql_shuffle_crosscheck_available']=bool(shuffle_metrics)
   if shuffle_metrics and row['sql_accumulator_shuffle_write_bytes']!=counter.get('shuffle_write_bytes',0):
    raise AssertionError('Query-root task and SQL shuffle counters differ: '+str((label,run.name,group,row['sql_accumulator_shuffle_write_bytes'],counter)))
   row['event_scan_metrics']=event_nodes
   row['reader_metric_scope']='all SQL event-plan branches; shared/reused SQL accumulator IDs counted once; includes scalar subqueries'

   row['shared_stages']={str(stage):sorted(str(g) for g in group_set) for stage,group_set in groups.items() if group in group_set and len(group_set)>1}
   row['plan_text']=(app/f'q{sample["query"]}-iteration{sample["iteration"]}.executed.txt').read_text()
   report['rows'].append(row)
  report.setdefault('applications',[]).append({'path':str(app),'status':d['status'],'error':d.get('error'),'last_log_lines':(app/'driver.log').read_text(errors='replace').splitlines()[-3:]})
report['root_free_bytes']=os.statvfs(w).f_bavail*os.statvfs(w).f_frsize
print(json.dumps(report))
'''.replace('REMOTE',repr(str(a.retained_root.resolve()) if a.retained_root else owner['remote'])).replace('LABELS',repr(a.labels)).replace('ANALYZE',repr(a.analyze))
ssh=['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config','-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
r=subprocess.run(['python3','-'] if a.retained_root else ssh,input=program,text=True,capture_output=True,timeout=120)
if r.returncode:raise RuntimeError(r.stderr.strip())
d=json.loads(r.stdout)
if a.analyze:
 d['evidence_source']='retained local archive' if a.retained_root else 'B200 retained runtime files'
 if a.retained_root:
  d['analysis_filesystem_free_bytes']=d.pop('root_free_bytes')
 prefix='recounted-suite-' if a.retained_root else 'suite-'
 path=ROOT/'evidence'/(prefix+ '-'.join(a.labels)+'.json')
 path.write_text(json.dumps(d,indent=2)+'\n')
for matrix in d['matrices']:print(matrix['status'],matrix.get('error'),[(x['arm'],x['status']) for x in matrix['arms']])
for row in (d['rows'][-a.recent:] if a.recent else d['rows']):
 m=row['task_metrics'];print(row['arm'],row['query'],row['iteration'],round(row['sql_seconds'],3),row['validation']['status'],
   {k:round(m.get(k,0)/1e9,3) for k in ('shuffle_write_bytes','memory_spill_bytes','disk_spill_bytes')} if a.analyze else '')
for app in d.get('applications',[]):print(app['status'],app['error'],app['last_log_lines'][-1:])
print('analysis_filesystem_free_GiB' if a.retained_root else 'root_free_GiB',round(d.get('root_free_bytes',d.get('analysis_filesystem_free_bytes',0))/2**30,2))
