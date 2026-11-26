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

"""Inspect unchanged SQL controls with sampled executor I/O and recorded clock evidence."""
import argparse
import collections
import difflib
import json
import os
from pathlib import Path
import re
import subprocess

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id'] == os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('label')
args = parser.parse_args()
data = json.loads((root/'evidence'/('sol-'+args.label+'.json')).read_text())
program = r'''
from pathlib import Path
import bisect,json,os
w=Path(REMOTE);label=LABEL
points=[json.loads(line) for line in (w/'evidence'/label/'host-monitor.jsonl').open()]
times=[p['time_unix_ns'] for p in points];out={}
for app in (w/'results'/label).glob('*/applications/suite'):
 d=json.loads((app/'result.json').read_text())
 for s in d['samples']:
  if s['query'] not in (6,19,21):continue
  left=bisect.bisect_left(times,s['started_unix_ns']);right=bisect.bisect_right(times,s['ended_unix_ns'])
  result={}
  for name,start,end in [('inside',left,right-1),('bracketed',left-1,right)]:
   a=points[max(0,start)];b=points[min(len(points)-1,end)]
   aa={x['pid']:x for x in a['executors']};bb={x['pid']:x for x in b['executors']}
   pids=aa.keys()&bb.keys();assert len(pids)==4
   result[name]={'sampled_seconds':(b['time_unix_ns']-a['time_unix_ns'])/1e9,
     'executor_read_bytes':sum(bb[p]['io']['read_bytes']-aa[p]['io']['read_bytes'] for p in pids),
     'executor_rchar_bytes':sum(bb[p]['io']['rchar']-aa[p]['io']['rchar'] for p in pids),
     'executor_cpu_seconds':sum(sum(bb[p]['cpu_ticks'].values())-sum(aa[p]['cpu_ticks'].values())
                                for p in pids)/os.sysconf('SC_CLK_TCK'),
     'file_refault_pages':b['vmstat']['workingset_refault_file']-a['vmstat']['workingset_refault_file']}
  out[d['application_id']+':'+str(s['query'])+':'+str(s['iteration'])]=result
print(json.dumps(out))
'''.replace('REMOTE',repr(owner['remote'])).replace('LABEL',repr(args.label))
process = subprocess.run(['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config',
    '-o','BatchMode=yes','mahonem@10.34.1.4','python3 -'], input=program,
    capture_output=True,text=True,check=True,timeout=120)
samples = json.loads(process.stdout)
out = {'sampling_limit': 'Executor I/O counters are sampled at 1 Hz. Inside and bracketed intervals bound sample alignment; neither is an exact query-boundary attribution.',
       'clock_limit': 'Recorded active SM clocks and operator task-time are diagnostics, not CUDA kernel utilization or isolated causal effects.',
       'rows': [], 'normalized_plan_diffs': {}}
out['q21_final_order_aggregate'] = []
for row in data['rows']:
    if row['query'] not in (6,19,21):
        continue
    sample = {k:row[k] for k in ('query','iteration','probe_arm','sql_seconds','task_metrics',
               'reader_metrics_all_branches','thermal')}
    sample['sampled_executor_io'] = samples[row['application_id']+':'+str(row['query'])+':'+str(row['iteration'])]
    metrics = collections.defaultdict(collections.Counter)
    for node in row['plan_metrics']:
        metrics[node['node']].update({k:v for k,v in node['metrics'].items()
            if k in ('opTimeNew','numOutputRows','numTasksSkippedAgg','numTasksRepartitioned')})
    sample['summed_operator_task_metrics'] = dict(metrics)
    out['rows'].append(sample)
    if row['query'] == 21:
        nodes = [n for n in row['plan_metrics'] if n['node'] == 'GpuHashAggregate' and
                 '__rapids_corragg_exists_min' in n['output']]
        assert len(nodes) == 1
        out['q21_final_order_aggregate'].append({
            'arm': row['probe_arm'], 'iteration': row['iteration'], 'metrics': nodes[0]['metrics']})
out['q21_source_trace'] = {
    'engine_revision': '68d906668dcaf51ce436a897e4326b8fd14fc002',
    'source': 'sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuAggregateExec.scala',
    'path': 'GpuMergeAggregateIterator.next -> AggregateUtils.iterateAndRepartition -> repartitionAndClose',
    'observable': 'numTasksRepartitioned is incremented after internal bucket repartition; a single merged input batch skips that repartition.',
    'causal_limit': 'The extra branch is demonstrated. Total wall-time differences also contain disk-cache and thermal-state differences.'}
for query in (6,19,21):
    def plan(arm):
        row=next(r for r in data['rows'] if r['query']==query and r['probe_arm']==arm and r['iteration']==2)
        text=re.sub(r'#\d+L?', '#attribute',row['plan_text'])
        return re.sub(r'plan_id=\d+','plan_id=identity',text).splitlines()
    before=plan('baseline');after=plan('combined')
    out['normalized_plan_diffs'][str(query)] = {'identical':before==after,
        'unified_diff':list(difflib.unified_diff(before,after,fromfile='baseline',tofile='combined'))}
path=root/'evidence'/('diagnostics-'+args.label+'.json')
path.write_text(json.dumps(out,indent=2)+'\n')
for row in out['rows']:
    print(row['query'],row['probe_arm'],row['iteration'],round(row['sql_seconds'],3),
          'read_GB_inside/bracketed',*[round(row['sampled_executor_io'][v]['executor_read_bytes']/1e9,3)
                                     for v in ('inside','bracketed')],
          'gpu0_SM_MHz',row['thermal'][0]['median_sm_clock_mhz'],
          'spill_GB',round(row['task_metrics'].get('memory_spill_bytes',0)/1e9,3))
print('identical_normalized_plans',{q:v['identical'] for q,v in out['normalized_plan_diffs'].items()})
print(path)
