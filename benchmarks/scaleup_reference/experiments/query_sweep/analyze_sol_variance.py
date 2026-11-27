#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare recorded scan work and sampled host pressure across complete SOL repeats."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from analyze_critical_path import pressure
from finalize_sol_reuse import normalize


def windows(app):
    profile = read(app / 'profile.json')
    event_dir = Path(urlparse(profile['spark.eventLog.dir']).path)
    records = {}
    for path in sorted(event_dir.iterdir()):
        if not path.is_file() or path.name.startswith('.'):
            continue
        assert not path.name.endswith('.inprogress')
        with path.open('rb') as stream:
            for line in stream:
                if b'SparkListenerSQLExecution' not in line:
                    continue
                event = json.loads(line)
                kind = event.get('Event', '')
                key = str(event.get('executionId'))
                if kind.endswith('SparkListenerSQLExecutionStart'):
                    name = event.get('description', '')
                    if re.fullmatch(r'q\d+-iteration\d+', name):
                        records[key] = {'name': name, 'start': event['time']}
                elif kind.endswith('SparkListenerSQLExecutionEnd') and key in records:
                    records[key]['end'] = event['time']
    assert len(records) == 44 and all('end' in value for value in records.values())
    return {value['name']: value for value in records.values()}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    a = p.parse_args()
    measurements = {}
    for phase in ('S1', 'S2'):
        directory = a.work / 'critical-path' / phase
        state = read(directory / 'state.json')
        assert state['status'] == 'completed'
        group = state['groups'][0]
        app = Path(group['run']) / 'applications/suite'
        intervals = windows(app)
        rows = []
        for sample in group['samples']:
            token = f'q{sample["query"]}-iteration{sample["iteration"]}'
            plan = normalize((app / (token + '.executed.txt')).read_text())
            scans = [n for n in sample['plan_metrics'] if n['node'].startswith('GpuScan')]
            rows.append({'query': sample['query'], 'iteration': sample['iteration'],
                'sql_seconds': sample['sql_seconds'], 'sql_sha256': sample['sql_sha256'],
                'plan_sha256': hashlib.sha256(plan.encode()).hexdigest(),
                'reader_metrics': sample['reader_metrics'],
                'scan_task_seconds': {key: sum(n['metrics'].get(key, 0) for n in scans) / 1e9
                                      for key in ('bufferTime', 'gpuDecodeTime', 'opTimeNew')},
                'execution_window_ms': intervals[token], **pressure(directory, intervals[token])})
        measurements[phase] = rows
    checks = []
    for before, after in zip(measurements['S1'], measurements['S2']):
        assert (before['query'], before['iteration']) == (after['query'], after['iteration'])
        checks.append({'query': before['query'], 'iteration': before['iteration'],
            **{key + '_equal': before[key] == after[key]
               for key in ('sql_sha256', 'plan_sha256', 'reader_metrics')}})
    result = {'runs': measurements, 'matched_work_checks': checks,
              'limits': ['Host counters are sampled; boundary fragments are omitted.',
                         'Scan timings sum concurrent tasks and cannot be added as query wall time.',
                         'Physical disk reads and native requested bytes measure different work.']}
    write(a.work / 'critical-path/sol-variance.json', result)
    print('Matched SQL, plans, native work:', all(all(r[k] for k in r if k.endswith('_equal')) for r in checks))
    for phase, rows in measurements.items():
        for row in rows:
            if row['query'] in (8, 9, 12, 13) and row['iteration'] == 2:
                print(phase, row['query'], round(row['sql_seconds'], 3),
                      'disk_GB', round(row['sampled_disk_read_bytes_dm0'] / 1e9, 2),
                      'full_PSI_seconds', round(row['sampled_memory_full_seconds'], 3),
                      'allocstall', row['sampled_allocstall'])


if __name__ == '__main__':
    main()
