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

"""Summarize complete timing cohorts without combining different hardware selections."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root / 'evidence/owner.json').read_text())
assert owner['thread_id'] == os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('labels', nargs='+')
args = parser.parse_args()
subprocess.run([sys.executable, str(root / 'scripts/suite_status.py'),
                *args.labels, '--analyze', '--recent', '1'], check=True)
path = root / 'evidence' / ('suite-' + '-'.join(args.labels) + '.json')
report = json.loads(path.read_text())
program = r'''
import json, statistics
from pathlib import Path
work = Path(REMOTE)
out = {}
for label in LABELS:
    telemetry = [json.loads(line) for line in
                 (work/'evidence'/label/'host-monitor.jsonl').open()]
    for app in (work/'results'/label).glob('*/applications/suite'):
        result = app/'result.json'
        if not result.exists():
            continue
        data = json.loads(result.read_text())
        config = json.loads((app.parents[1]/'config.json').read_text())
        checks = app/'rewrite-counterexamples.json'
        checks = json.loads(checks.read_text()) if checks.exists() else []
        for sample in data.get('samples', []):
            active = [t for t in telemetry if sample['started_unix_ns'] <=
                      t['time_unix_ns'] <= sample['ended_unix_ns']]
            gpu_metrics = []
            for position, index in enumerate(config['gpu_indices']):
                gpu = [t['gpus'][position] for t in active
                       if t['gpus'][position].get('util_gpu', 0) >= 20]
                gpu_metrics.append({'physical_index': index, 'active_samples': len(gpu),
                    'thermal_samples': sum(bool(g['throttle_reasons'] & 32) for g in gpu),
                    'maximum_temperature_c': max((g['temperature_c'] for g in gpu), default=None),
                    'median_sm_clock_mhz': statistics.median(g['sm_clock_mhz'] for g in gpu) if gpu else None})
            key = app.parents[1].name + ':' + str(sample['query']) + ':' + str(sample['iteration'])
            out[key] = {'gpu_indices': config['gpu_indices'], 'thermal': gpu_metrics,
                        'semantic_cases': len(checks)}
print(json.dumps(out))
'''.replace('REMOTE', repr(owner['remote'])).replace('LABELS', repr(args.labels))
process = subprocess.run(['ssh', '-F', '/home/nvidia/mahone_gluten/cluster_ssh_config',
    '-o', 'BatchMode=yes', 'mahonem@10.34.1.4', 'python3 -'], input=program,
    capture_output=True, text=True, check=True, timeout=120)
metadata = json.loads(process.stdout)
for row in report['rows']:
    key = row['arm'] + ':' + str(row['query']) + ':' + str(row['iteration'])
    row.update(metadata[key])
    row['lineitem_scan_count'] = sum(x['node'].endswith('.lineitem') for x in row['event_scan_metrics'])
    row['part_scan_count'] = sum(x['node'].endswith('.part') for x in row['event_scan_metrics'])
report['limits'] = ['Shared node; GPU0 has observed thermal throttling.',
                    'GPU baseline references and small CPU/GPU semantic checks; no independent full SF10K CPU reference.',
                    'Reader counters include page-cache reads; not cold physical disk traffic.',
                    'Best measured configuration is not an absolute hardware speed-of-light bound.']
report['groups'] = []
for label in args.labels:
    rows = [r for r in report['rows'] if r['label'] == label]
    for query in sorted({r['query'] for r in rows}):
        group = {'label': label, 'query': query, 'arms': {}}
        for arm in sorted({r['probe_arm'] for r in rows if r['query'] == query}):
            selected = [r for r in rows if r['query'] == query and r['probe_arm'] == arm]
            assert len({tuple(r['gpu_indices']) for r in selected}) == 1
            group['arms'][arm] = {
                'runs': [(r['arm'], r['iteration'], r['sql_seconds']) for r in selected],
                'second_pass_seconds': [r['sql_seconds'] for r in selected if r['iteration'] == 2],
                'reader_bytes': [r['reader_metrics_all_branches'].get('hybridScanNativeHostBytesRead', 0) for r in selected],
                'shuffle_bytes': [r['task_metrics'].get('shuffle_write_bytes', 0) for r in selected],
                'memory_spill_bytes': [r['task_metrics'].get('memory_spill_bytes', 0) for r in selected],
                'lineitem_scan_counts': [r['lineitem_scan_count'] for r in selected]}
        report['groups'].append(group)
        print('Q' + str(query), {a: [round(t, 3) for t in g['second_pass_seconds']]
                                for a, g in group['arms'].items()})
output = root / 'evidence' / ('sol-' + '-'.join(args.labels) + '.json')
output.write_text(json.dumps(report, indent=2) + '\n')
print(output)
