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
"""Audit the complete all-query collection without upgrading thermal-affected timings."""
import json,os,subprocess
from pathlib import Path
ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((ROOT/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
program=r'''
import collections,hashlib,json,os,re,subprocess,sys
from pathlib import Path
w=Path(REMOTE)
sys.path[:0]=[str(w/'source/benchmarks/scaleup_reference/scripts'),str(w/'source/benchmarks/scaleup_reference/experiments/b200_memory')]
import b200_services as services
labels=['all22-baseline','all22-remainder','q3-checked','topn-prefix']
remaining=[];reasons=collections.Counter();receipts=[];errors=[];shutdown=[];apps=[]
for label in labels:
 for app in sorted((w/'results'/label).glob('*/applications/suite')):
  d=json.loads((app/'result.json').read_text());run=app.parents[1]
  assert d['effective_conf']['spark.executor.instances']=='4'
  assert 'spark.rapids.memory.gpu.allocSize' not in d['effective_conf']
  assert len({x['uuid'] for x in d['gpu_assignments']})==4
  assert all(s['validation']['status'] in ('passed','reference-generated') for s in d['samples'])
  apps.append({'label':label,'application_id':d['application_id'],'status':d['status'],'error':d.get('error'),'retained_samples':len(d['samples'])})
  for state in [app/'driver-service.json',*run.glob('services/*/*.json')]:
   if state.name.endswith('.stopped.json'):continue
   for record in json.loads(state.read_text()):remaining.extend(services.group_members(record))
  for p in app.glob('*.cleanup.json'):
   rows=json.loads(p.read_text());assert len(rows)==4;receipts.extend(rows)
  events=list((w/'scratch'/run.name/'events').glob('app-*'))
  for event in events:
   if not event.is_file():continue
   for raw in event.open('rb'):
    if b'SparkListenerTaskEnd' not in raw:continue
    v=json.loads(raw);reasons[v.get('Task End Reason',{}).get('Reason','unknown')]+=1
  logs=[app/'driver.log',*(w/'scratch'/run.name/'suite').glob('worker-*/work/*/*/stderr')]
  for path in logs:
   stopped=False
   for line in path.open(errors='replace'):
    if 'Driver commanded a shutdown' in line or 'Shutting down ShuffleCleanupEndpoint' in line:stopped=True
    if re.match(r'^\d\d/\d\d/\d\d .* ERROR ',line) and 'RECEIVED SIGNAL TERM' not in line:
     record={'file':str(path),'line':line.strip()}
     if stopped and 'CudaMemoryBuffer: A CUDA BUFFER WAS LEAKED' in line:shutdown.append(record)
     else:errors.append(record)
assert not remaining,remaining
assert set(reasons)=={'Success'},dict(reasons)
assert all(a['status']=='completed' for a in apps if a['label']!='all22-baseline')
assert next(a for a in apps if a['label']=='all22-baseline')['error']=="AssertionError('Q3: exact row identities differ')"
files=[p for table in ['customer','lineitem','nation','orders','part','partsupp','region','supplier'] for p in (Path('/raid/knataraj/datasets/tpch/sf10k_v2_float')/table).glob('*.parquet')]
cp=json.loads((w/'evidence/cpu-variant-checks/result.json').read_text());assert cp['status']=='passed' and len(cp['checks'])==14
assert (w/'evidence/cpu-variant-checks/exit').read_text().strip()=='0'
print(json.dumps({'query_execution_status':'completed_with_retained_prior_validation_failure',
 'performance_qualified':False,'timing_limit':'GPU0-2 thermal throttling; no healthy-node speedup qualification',
 'applications':apps,'task_end_reasons':dict(reasons),'remaining_recorded_service_processes':remaining,
 'cleanup_acknowledgements':len(receipts),'maximum_rmm_after_cleanup_bytes':max(r['rmm_after'] for r in receipts),
 'runtime_error_log_findings':errors,'shutdown_buffer_leak_reports':shutdown,
 'shutdown_cleanup_implementation_qualified':not shutdown,
 'cpu_counterexamples_passed':14,'independent_sf10k_cpu_reference':False,
 'dataset':{'path':'/raid/knataraj/datasets/tpch/sf10k_v2_float','parquet_files':len(files),'parquet_bytes':sum(p.stat().st_size for p in files)},
 'metadata_note':'Retained run configs inherited an unused AWS dataset_contract. Actual B200 table locations and sizes are audited here; execution used the B200 path. The future runner populates this metadata from B200 files.',
 'gpu_indices':[0,1,2,3],'extra_rmm_alloc_size_limit':False,
 'gpu_processes_after':subprocess.check_output(['nvidia-smi','--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory','--format=csv,noheader'],text=True),
 'root_free_bytes':os.statvfs(w).f_bavail*os.statvfs(w).f_frsize,
 'engine_sha256':hashlib.sha256((w/'artifacts/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar').read_bytes()).hexdigest()}))
'''.replace('REMOTE',repr(owner['remote']))
ssh=['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config','-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
r=subprocess.run(ssh,input=program,text=True,capture_output=True,timeout=120)
if r.returncode:raise RuntimeError(r.stderr)
d=json.loads(r.stdout)
(ROOT/'evidence/suite-qualification.json').write_text(json.dumps(d,indent=2)+'\n')
print(json.dumps({k:v for k,v in d.items() if k not in ['shutdown_buffer_leak_reports','runtime_error_log_findings']},indent=2))
print('runtime_error_findings',len(d['runtime_error_log_findings']),'shutdown_findings',len(d['shutdown_buffer_leak_reports']))
