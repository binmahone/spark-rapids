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

"""Retain sampled I/O and executed branch metrics for complete candidate trials."""
import argparse
import json
import os
from pathlib import Path
import subprocess

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id'] == os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('label')
parser.add_argument('--source', help='Read this combined evidence set and select the named label')
args = parser.parse_args()
source = root/'evidence'/('sol-'+(args.source or args.label)+'.json')
data = json.loads(source.read_text())
assert all(m['status']=='completed' for m in data['matrices'])
data['rows'] = [r for r in data['rows'] if r['label']==args.label]
assert data['rows'], 'No completed samples for the requested label'
assert all(r['validation']['status']=='passed' for r in data['rows'])
program = r'''
import bisect,json,os
from pathlib import Path
w=Path(REMOTE)
points=[json.loads(line) for line in (w/'evidence'/LABEL/'host-monitor.jsonl').open()]
times=[p['time_unix_ns'] for p in points];out={}
for sample in SAMPLES:
 left=bisect.bisect_left(times,sample['started_unix_ns'])
 right=bisect.bisect_right(times,sample['ended_unix_ns'])
 result={}
 for name,start,end in [('inside',left,right-1),('bracketed',left-1,right)]:
  a=points[max(0,start)];b=points[min(len(points)-1,end)]
  aa={x['pid']:x for x in a['executors']};bb={x['pid']:x for x in b['executors']}
  pids=aa.keys()&bb.keys();assert len(pids)==4,(sample,pids)
  result[name]={'sampled_seconds':(b['time_unix_ns']-a['time_unix_ns'])/1e9,
   'executor_read_bytes':sum(bb[p]['io']['read_bytes']-aa[p]['io']['read_bytes'] for p in pids),
   'executor_rchar_bytes':sum(bb[p]['io']['rchar']-aa[p]['io']['rchar'] for p in pids),
   'executor_cpu_seconds':sum(sum(bb[p]['cpu_ticks'].values())-sum(aa[p]['cpu_ticks'].values())
                            for p in pids)/os.sysconf('SC_CLK_TCK')}
 out[sample['key']]=result
print(json.dumps(out))
'''
samples = [dict(key=r['arm']+':'+str(r['query'])+':'+str(r['iteration']),
                started_unix_ns=r['started_unix_ns'], ended_unix_ns=r['ended_unix_ns'])
           for r in data['rows']]
program = program.replace('REMOTE',repr(owner['remote'])).replace('LABEL',repr(args.label))
program = program.replace('SAMPLES',repr(samples))
result = subprocess.run(['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config',
    '-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -'],
    input=program,text=True,capture_output=True,check=True,timeout=120)
observed = json.loads(result.stdout)
report = {'source_evidence':str(source),'rows':[],
    'sampling_limit':'One-second executor samples bracket query boundaries approximately; these are not exact query-attributed disk bytes.',
    'operator_limit':'Operator counters sum task time and do not measure CUDA kernel utilization.',
    'causal_limit':'Shared node with GPU0 thermal throttling; work counters and timings must be considered separately.'}
for row in data['rows']:
    key = row['arm']+':'+str(row['query'])+':'+str(row['iteration'])
    entry = {k:row[k] for k in ('arm','probe_arm','query','iteration','sql_seconds',
        'task_metrics','reader_metrics_all_branches','thermal','validation')}
    entry['sampled_executor_io'] = observed[key]
    entry['native_broadcasts'] = [n for n in row['plan_metrics']
                                 if n['node']=='GpuDriverlessBroadcastHashJoin']
    entry['order_state_aggregates'] = [n for n in row['plan_metrics']
        if n['node']=='GpuHashAggregate' and
        any(name in n['output'] for name in ('__state_all_min','__rapids_corragg_exists_min'))]
    report['rows'].append(entry)
    print('Q'+str(row['query']),row['probe_arm'],row['iteration'],round(row['sql_seconds'],3),
        'physical_read_GB_inside/bracketed',*[round(observed[key][name]['executor_read_bytes']/1e9,3)
                                          for name in ('inside','bracketed')],
        'native_builds',[n['metrics'].get('executorBuilds',0) for n in entry['native_broadcasts']])
target = root/'evidence'/('diagnostics-'+args.label+'.json')
target.write_text(json.dumps(report,indent=2)+'\n')
print(target)
