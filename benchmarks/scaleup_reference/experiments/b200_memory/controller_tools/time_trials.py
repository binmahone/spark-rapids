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
"""Summarize only matched timing cohorts and attach per-action thermal evidence."""
import argparse,json,os,statistics,subprocess,sys
from pathlib import Path
ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((ROOT/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('labels',nargs='*',default=['trial-q9-0356','trial-preagg-0356','trial-q15-0356'])
a=p.parse_args()
r=subprocess.run([sys.executable,str(ROOT/'scripts/suite_status.py'),*a.labels,'--analyze','--recent','1'],capture_output=True,text=True,check=True)
raw_path=ROOT/'evidence'/('suite-'+'-'.join(a.labels)+'.json')
raw=json.loads(raw_path.read_text())
program=r'''
from pathlib import Path
import json,statistics
w=Path(REMOTE);out={}
for label in LABELS:
 monitor=w/'evidence'/label/'host-monitor.jsonl'
 if not monitor.exists():continue
 telemetry=[]
 for line in monitor.open():
  try:telemetry.append(json.loads(line))
  except json.JSONDecodeError:pass
 for app in (w/'results'/label).glob('*/applications/suite'):
  p=app/'result.json'
  if not p.exists():continue
  d=json.loads(p.read_text());config=json.loads((app.parents[1]/'config.json').read_text())
  assert config['gpu_indices']==[0,3,5,6]
  conf=d.get('effective_conf',{})
  if conf:
   assert conf['spark.executor.instances']=='4' and 'spark.rapids.memory.gpu.allocSize' not in conf
  checks=app/'rewrite-counterexamples.json'
  mini=json.loads(checks.read_text()) if checks.exists() else []
  if config['probe_arm']=='aggregate-reuse':
   import re
   assert all(re.search(r'Gpu\w*Window',check['gpu_plan']) for check in mini)
  for sample in d.get('samples',[]):
   key=app.parents[1].name+':'+str(sample['query'])+':'+str(sample['iteration'])
   rows=[x for x in telemetry if sample['started_unix_ns']<=x['time_unix_ns']<=sample['ended_unix_ns']]
   gpus=[]
   for position,index in enumerate(config['gpu_indices']):
    active=[x['gpus'][position] for x in rows if x['gpus'][position].get('util_gpu',0)>=20]
    gpus.append({'physical_index':index,'active_samples':len(active),
      'sw_thermal_samples':sum(bool(g.get('throttle_reasons',0)&32) for g in active),
      'maximum_temperature_c':max((g['temperature_c'] for g in active),default=None),
      'median_active_sm_clock_mhz':statistics.median(g['sm_clock_mhz'] for g in active) if active else None})
   out[key]={'gpu_indices':config['gpu_indices'],'thermal':gpus,'gpu_counterexample_checks':len(mini),
             'runtime_source_revision':json.loads((w/'results'/label/'matrix.json').read_text())['source_revision']}
print(json.dumps(out))
'''.replace('REMOTE',repr(owner['remote'])).replace('LABELS',repr(a.labels))
ssh=['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config','-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
r=subprocess.run(ssh,input=program,text=True,capture_output=True,check=True,timeout=120)
meta=json.loads(r.stdout)
rows=[]
for sample in raw['rows']:
 key=sample['arm']+':'+str(sample['query'])+':'+str(sample['iteration'])
 row=dict(sample,**meta[key]);row['part_scan_count']=sum(x['node'].endswith('.part') for x in sample['event_scan_metrics']);row['lineitem_scan_count']=sum(x['node'].endswith('.lineitem') for x in sample['event_scan_metrics'])
 rows.append(row)
groups=[]
for label in a.labels:
 for query in sorted({r['query'] for r in rows if r['label']==label}):
  group={'label':label,'query':query,'arms':{},'healthy_node_performance_qualified':False}
  for arm in sorted({r['probe_arm'] for r in rows if r['label']==label and r['query']==query}):
   selected=[r for r in rows if r['label']==label and r['query']==query and r['probe_arm']==arm]
   warm=[r['sql_seconds'] for r in selected if r['iteration']==2]
   group['arms'][arm]={'runs':[(r['arm'],r['iteration'],r['sql_seconds']) for r in selected],
     'second_pass_seconds':warm,'second_pass_median_seconds':statistics.median(warm) if warm else None,
     'reader_bytes':[r['reader_metrics_all_branches'].get('hybridScanNativeHostBytesRead',0) for r in selected],
     'shuffle_bytes':[r['task_metrics'].get('shuffle_write_bytes',0) for r in selected],
     'part_scan_counts':[r['part_scan_count'] for r in selected],
     'lineitem_scan_counts':[r['lineitem_scan_count'] for r in selected]}
  baseline=group['arms'].get('baseline',{}).get('second_pass_median_seconds')
  if baseline:
   group['observed_second_pass_time_reduction_fraction']={arm:1-values['second_pass_median_seconds']/baseline
     for arm,values in group['arms'].items() if arm!='baseline' and values['second_pass_median_seconds'] is not None}
  groups.append(group)
report={'classification':'matched four-GPU timing experiments; shared node, GPU0 thermal throttling',
 'gpu_indices':[0,3,5,6],'extra_rmm_alloc_size_limit':False,'independent_sf10k_cpu_reference':False,
 'limits':['Compare only within each cohort; do not compare with the previous GPU0-3 observations.',
  'Observed timing effects are specific to this environment; no healthy-node speedup qualification.',
  'Native host reader counters include page-cache reads; they are not cold physical disk traffic.'],
 'matrices':raw['matrices'],'groups':groups,'rows':rows}
(ROOT/'evidence/time-trial-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
for g in groups:
 print(g['label'],'Q'+str(g['query']))
 for arm,values in g['arms'].items():print(arm,'second_pass', [round(v,3) for v in values['second_pass_seconds']])
 print('observed_reduction',g.get('observed_second_pass_time_reduction_fraction'))
