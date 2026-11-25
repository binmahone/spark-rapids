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
"""Reassess timing qualification from retained per-GPU thermal telemetry."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import tarfile

ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
owner=json.loads((ROOT/'evidence/owner.json').read_text())
assert owner['thread_id']==os.environ['CODEX_THREAD_ID']
def summarize(rows):
 result=[]
 for i in range(4):
  active=[r['gpus'][i] for r in rows if isinstance(r['gpus'][i].get('util_gpu'),int) and r['gpus'][i]['util_gpu']>=20]
  result.append({'gpu_index':i,'active_samples':len(active),
   'sw_thermal_slowdown_active_samples':sum(bool(g.get('throttle_reasons',0)&32) for g in active),
   'maximum_temperature_c':max((g['temperature_c'] for g in active),default=None),
   'median_active_sm_clock_mhz':statistics.median(g['sm_clock_mhz'] for g in active) if active else None})
 return result
archive=ROOT/'retained/runtime-evidence.tar.gz'
with tarfile.open(archive) as stream:
 rows=[json.loads(line) for line in stream.extractfile('evidence/host-monitor.jsonl')]
program=r'''
from pathlib import Path
import json,statistics
w=Path(REMOTE);result={}
for p in sorted((w/'evidence').glob('*/host-monitor.jsonl')):
 rows=[]
 for line in p.open():
  try:rows.append(json.loads(line))
  except json.JSONDecodeError:pass
 result[str(p)]=summarize(rows)
print(json.dumps(result))
'''.replace('REMOTE',repr(owner['remote']))
import inspect
program='import statistics\n'+inspect.getsource(summarize)+program
ssh=['ssh','-F','/home/nvidia/mahone_gluten/cluster_ssh_config','-o','BatchMode=yes','-o','ConnectTimeout=8','mahonem@10.34.1.4','python3 -']
r=subprocess.run(ssh,input=program,text=True,capture_output=True,check=True,timeout=60)
report={'reviewed_at_utc':datetime.now(timezone.utc).isoformat(),
 'performance_qualified':False,'classification':'thermal-throttling-affected diagnostics',
 'sampling_scope':'1 Hz NVML samples with GPU utilization >=20%; not exact duration or per-query live allocation',
 'prior_q18_q20':{'archive':str(archive),'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
                  'telemetry_member':'evidence/host-monitor.jsonl','gpus':summarize(rows)},
 'current_sweeps':json.loads(r.stdout),
 'limits':'Retain measured times as observations only. Work counts, byte counters and result comparisons remain useful; no healthy-node speedup is established.'}
(ROOT/'evidence/thermal-qualification.json').write_text(json.dumps(report,indent=2)+'\n')
for name in ('qualification.json','comparison.json'):
 path=ROOT/'evidence'/name
 d=json.loads(path.read_text());snapshot=path.with_name(path.stem+'-before-thermal-review.json')
 if not snapshot.exists():snapshot.write_bytes(path.read_bytes())
 d.update(performance_qualified=False,performance_qualification='limited_by_thermal_throttling',
          thermal_evidence=str(ROOT/'evidence/thermal-qualification.json'))
 path.write_text(json.dumps(d,indent=2)+'\n')
print(json.dumps(report,indent=2))
