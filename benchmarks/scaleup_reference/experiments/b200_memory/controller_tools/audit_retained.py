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

"""Audit archived tasks, cleanup receipts and errors without restoring scratch files."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import tarfile

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('name')
args = parser.parse_args()
receipt = json.loads((root/'evidence'/(args.name+'-retention.json')).read_text())
archive = Path(receipt['local_archive'])
with archive.open('rb') as stream:
    assert hashlib.file_digest(stream,'sha256').hexdigest()==receipt['sha256']
report = {'archive':str(archive),'sha256':receipt['sha256'],'matrices':{},'applications':[],
          'task_end_reasons':{},'cleanup_acknowledgements':0,'maximum_rmm_after_cleanup_bytes':0,
          'shutdown_buffer_leak_reports':[],'other_error_log_findings':[],
          'small_cpu_gpu_counterexamples':0,
          'process_scope':'The retention receipt verified stopped service groups before deleting scratch; this offline audit does not establish current host idleness.'}
with tarfile.open(archive,'r:gz') as tar:
    members = {m.name:m for m in tar.getmembers() if m.isfile()}
    def read(name):
        return json.load(tar.extractfile(members[name]))
    for name in members:
        if name.startswith('results/') and name.endswith('/matrix.json'):
            report['matrices'][name.split('/')[1]]=read(name)
    complete = {label for label,matrix in report['matrices'].items() if matrix['status']=='completed'}
    runs = {Path(arm['run']).name:label for label,matrix in report['matrices'].items()
            for arm in matrix['arms']}
    reasons = {label:Counter() for label in report['matrices']}
    for name,member in members.items():
        parts = name.split('/')
        if parts[0]=='results':
            label=parts[1]
            if name.endswith('/result.json'):
                app=read(name)
                if label in complete:
                    assert app['status']=='completed'
                    assert all(s['validation']['status']=='passed' for s in app['samples'])
                report['applications'].append({'label':label,'path':name,
                    'status':app['status'],'samples':len(app['samples'])})
            if name.endswith('.cleanup.json'):
                rows=read(name);assert len(rows)==4
                report['cleanup_acknowledgements']+=len(rows)
                report['maximum_rmm_after_cleanup_bytes']=max(
                    report['maximum_rmm_after_cleanup_bytes'],max(r['rmm_after'] for r in rows))
            if name.endswith('/rewrite-counterexamples.json'):
                checks=read(name)
                assert all(c['status']=='passed' and c['cpu_rewrite_equal'] and
                           c['gpu_rewrite_equal'] for c in checks)
                report['small_cpu_gpu_counterexamples']+=len(checks)
        elif parts[0]=='scratch':
            label=runs[parts[1]]
            if '/events/app-' in name:
                for raw in tar.extractfile(member):
                    if b'SparkListenerTaskEnd' in raw:
                        value=json.loads(raw)
                        reasons[label][value.get('Task End Reason',{}).get('Reason','unknown')]+=1
        else:
            continue
        if (name.endswith('/driver.log') or name.endswith('/stderr')) and label in complete:
            stopped=False
            for raw in tar.extractfile(member):
                line=raw.decode(errors='replace').strip()
                if 'Driver commanded a shutdown' in line or 'Shutting down ShuffleCleanupEndpoint' in line:
                    stopped=True
                if re.match(r'^\d\d/\d\d/\d\d .* ERROR ',line) and 'RECEIVED SIGNAL TERM' not in line:
                    target='shutdown_buffer_leak_reports' if stopped and 'CudaMemoryBuffer: A CUDA BUFFER WAS LEAKED' in line else 'other_error_log_findings'
                    report[target].append({'label':label,'file':name,'line':line})
    for label in complete:
        assert set(reasons[label])=={'Success'},(label,reasons[label])
    report['task_end_reasons']={label:dict(counts) for label,counts in reasons.items()}
report['shutdown_cleanup_qualified']=not report['shutdown_buffer_leak_reports']
target=root/'evidence'/(args.name+'-audit.json')
target.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ('task_end_reasons','cleanup_acknowledgements',
    'maximum_rmm_after_cleanup_bytes','small_cpu_gpu_counterexamples','shutdown_cleanup_qualified')},indent=2))
print('shutdown_leak_lines',len(report['shutdown_buffer_leak_reports']),
      'other_error_lines',len(report['other_error_log_findings']))
print(target)
