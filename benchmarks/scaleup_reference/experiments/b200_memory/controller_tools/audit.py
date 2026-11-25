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

"""Qualify completed samples and verify that all owned service groups have exited."""
import json
import os
from pathlib import Path
import subprocess

root=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
program=r'''
from pathlib import Path
import collections,hashlib,json,os,re,subprocess,sys
w=Path(REMOTE)
sys.path.insert(0,str(w/'source/benchmarks/scaleup_reference/scripts'))
sys.path.insert(0,str(w/'source/benchmarks/scaleup_reference/experiments/b200_memory'))
import b200_services as services
assert (w/'evidence/probe.exit').read_text().strip()=='0'
state=json.loads((w/'results/default/matrix.json').read_text());assert state['status']=='completed'
samples=[];remaining=[];receipts=[];errors=[];shutdown_findings=[];reasons=collections.Counter()
for path in sorted((w/'results').glob('*/*/services/*/*.json')):
    if path.name.endswith('.stopped.json'):continue
    for record in json.loads(path.read_text()):remaining.extend(services.group_members(record))
for path in sorted((w/'results').glob('*/*/applications/suite/driver-service.json')):
    for record in json.loads(path.read_text()):remaining.extend(services.group_members(record))
assert not remaining
for app in sorted((w/'results/default').glob('*/applications/suite')):
    d=json.loads((app/'result.json').read_text());assert d['status']=='completed'
    assert d['effective_conf']['spark.executor.instances']=='4'
    assert 'spark.rapids.memory.gpu.allocSize' not in d['effective_conf']
    assert len({r['uuid'] for r in d['gpu_assignments']})==4
    assert len(d['samples'])==4
    for s in d['samples']:
        assert s['validation']['status'] in ['passed','reference-generated']
        r=s['reader_metrics'];assert r['hybridScanNativeHostBytesRead']>0
        assert r['hybridScanDeviceBytesRead']==r['hybridScanGdsFallbackFiles']==0
        samples.append(s)
    for p in app.glob('*.cleanup.json'):
        rows=json.loads(p.read_text());assert len(rows)==4
        receipts.extend(rows)
    for path in [app/'driver.log',*(w/'scratch'/app.parents[1].name/'suite').glob('worker-*/work/*/*/stderr')]:
        shutdown_started=False
        for line in path.read_text(errors='replace').splitlines():
            if 'Driver commanded a shutdown' in line or 'Shutting down ShuffleCleanupEndpoint' in line:
                shutdown_started=True
            if re.match(r'^\d\d/\d\d/\d\d .* ERROR ',line) and 'RECEIVED SIGNAL TERM' not in line:
                finding={'file':str(path),'line':line}
                if shutdown_started and 'CudaMemoryBuffer: A CUDA BUFFER WAS LEAKED' in line:
                    shutdown_findings.append(finding)
                else:
                    errors.append(finding)
    events=next((w/'scratch'/app.parents[1].name/'events').glob('app-*'))
    for line in events.open('rb'):
        if b'SparkListenerTaskEnd' in line:
            v=json.loads(line);reason=v.get('Task End Reason',{}).get('Reason','unknown');reasons[reason]+=1
assert len(samples)==16 and len(receipts)==64
assert not errors,errors[:5]
assert set(reasons)=={'Success'},dict(reasons)
for q in [18,20]:
    assert len({s['reader_metrics']['hybridScanNativeHostBytesRead'] for s in samples if s['query']==q})==1
report={'status':'limited_by_shutdown_findings' if shutdown_findings else 'passed',
        'query_execution_status':'passed','complete_timed_actions':16,'q18_reference_generated_separately':True,
        'validation_states':dict(collections.Counter(s['validation']['status'] for s in samples)),
        'cleanup_acknowledgements':len(receipts),'remaining_owned_service_processes':remaining,
        'maximum_rmm_after_cleanup_bytes':max(r['rmm_after'] for r in receipts),
        'runtime_errors':errors,'task_end_reasons':dict(reasons),
        'shutdown_buffer_leak_reports':shutdown_findings,
        'shutdown_cleanup_implementation_qualified':not shutdown_findings,
        'independent_cpu_reference':False,'extra_rmm_alloc_size_limit':False,
        'gpu_indices':[0,1,2,3],'source_revision':state['source_revision'],
        'engine_sha256':hashlib.sha256((w/'artifacts/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar').read_bytes()).hexdigest(),
        'filesystem_before_cleanup':subprocess.check_output(['df','-hT','/', '/raid'],text=True),
        'owned_directory_allocated_bytes':int(subprocess.check_output(['du','-sx','--block-size=1',str(w)],text=True).split()[0])}
print(json.dumps(report))
'''.replace('REMOTE',repr(owner['remote']))
ssh=['ssh','-F',os.environ['B200_SSH_CONFIG'],'-o','BatchMode=yes',
     '-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
reply=subprocess.run(ssh,input=program,text=True,capture_output=True,timeout=120)
if reply.returncode:
    raise RuntimeError(reply.stderr)
report=json.loads(reply.stdout)
(root/'evidence/qualification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
