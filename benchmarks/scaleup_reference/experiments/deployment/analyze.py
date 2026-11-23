#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Verify reached placement controls and retain paired query timings without suite extrapolation."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import statistics
import tarfile


def read(path):
    return json.loads(Path(path).read_text())


def cpus(mask):
    result = set()
    for part in mask.split(','):
        ends = list(map(int, part.split('-')))
        result.update(range(ends[0], ends[-1] + 1))
    return result


def normalized_plan(path):
    text = Path(path).read_text()
    text = re.sub(r'#\d+[L]?', '#ID', text)
    text = re.sub(r'\[plan_id=\d+\]', '[plan_id=ID]', text)
    text = re.sub(r'executionId=\d+', 'executionId=ID', text)
    text = re.sub(r'(?<=[\w$])@[0-9a-fA-F]+\b', '@INSTANCE', text)
    return hashlib.sha256(text.encode()).hexdigest()


def analyze(root):
    plan = read(root / 'plan.json')
    output, groups, plans = [], defaultdict(list), defaultdict(set)
    for case in plan['cases']:
        run = root / 'runs' / Path(case['run']).name
        if not (run / 'case-evidence.json').exists():
            continue
        config = read(run / 'config.json')
        if config['engine_sha256'] != plan['engine_sha256'] or config['tools_sha256'] != plan['tools_sha256']:
            raise AssertionError('Artifact identity changed')
        data = read(run / 'case-evidence.json')
        suite_id = read(run / 'applications/suite/result.json')['application_id']
        records = [read(p) for p in (run / 'runtime').glob('*/*.json') if p.stem.isdigit()]
        records = [r for r in records if '--app-id ' + suite_id + ' ' in r['argv']]
        if len(records) != 32:
            raise AssertionError('Expected 32 observed executor processes')
        affinity_counts, policies, heap_policies, libraries = Counter(), Counter(), Counter(), set()
        for record in records:
            status = dict(line.split(':', 1) for line in record['status'].splitlines() if ':' in line)
            affinity_counts[len(cpus(status['Cpus_allowed_list'].strip()))] += 1
            anon = [line.split()[1] for line in record['numa_maps'].splitlines()
                    if 'anon=' in line and len(line.split()) > 1]
            policies.update(set(anon))
            heap_policies.update(line.split()[1] for line in record['numa_maps'].splitlines()
                                 if 'heap' in line.split())
            libraries.update(line.split()[-1] for line in record['maps'].splitlines()
                if any(name in line for name in ('/libucp.so', '/libucs.so', '/libuct.so',
                                                  '/libcufile.so', '/libnvrtc.so', '/libnvJitLink.so')))
        expected = 144 if case['arm'] == 'unbound' else 72
        if dict(affinity_counts) != {expected: 32}:
            raise AssertionError((case['label'], 'CPU affinity differs', affinity_counts))
        if sum(heap_policies.values()) != 32:
            raise AssertionError('Missing process heap policy evidence')
        bound = all(policy.startswith('bind:') for policy in heap_policies)
        if bound != (case['arm'] not in ('cpu-only', 'unbound')):
            raise AssertionError((case['label'], 'Memory policy differs', policies))
        second = [s for s in data['samples'] if s['iteration'] == 2]
        initialized_devices = Counter()
        for archive_path in (run / 'runtime').glob('*/executor-logs.tar.gz'):
            with tarfile.open(archive_path) as archive:
                for member in archive.getmembers():
                    if member.name.endswith('/stderr') and '/' + suite_id + '/' in member.name:
                        text = archive.extractfile(member).read().decode(errors='replace')
                        devices = set(re.findall(r'Initializing RMM ASYNC pool size = .*? on gpuId (\d+)', text))
                        if len(devices) != 1:
                            raise AssertionError('Missing or conflicting RMM device initialization')
                        initialized_devices.update(devices)
        expected_devices = {'0': 16, '1': 16} if case['arm'] == 'per-numa' else {'0': 32}
        if dict(initialized_devices) != expected_devices:
            raise AssertionError((case['label'], 'RMM devices differ', initialized_devices))
        row = {'label': case['label'], 'arm': case['arm'], 'executor_count': len(records),
            'available_cpus_per_executor': dict(affinity_counts),
            'observed_anonymous_memory_policies': dict(policies),
            'observed_process_heap_policies': dict(heap_policies),
            'rmm_initialized_logical_devices': dict(initialized_devices),
            'best_sql_sum_seconds': sum(min(s['sql_seconds'] for s in data['samples'] if str(s['query']) == q)
                                       for q in set(str(s['query']) for s in data['samples'])),
            'second_sql_sum_seconds': sum(s['sql_seconds'] for s in second),
            'mapped_native_libraries': sorted(libraries),
            'queries': {str(s['query']): {'first_sql_seconds': next(x['sql_seconds'] for x in data['samples']
                       if x['query'] == s['query'] and x['iteration'] == 1),
                       'second_sql_seconds': s['sql_seconds'],
                       'second_sql_plus_cleanup_seconds': s['sql_plus_cleanup_seconds'],
                       'validation': s['validation']['status'], 'reader_metrics': s['reader_metrics']}
                       for s in second}}
        for sample in second:
            query = str(sample['query'])
            groups[case['arm'], query].append(sample['sql_seconds'])
            for kind in ('logical', 'executed'):
                path = run / 'applications/suite' / f'q{query}-iteration2.{kind}.txt'
                plans[query, kind].add(normalized_plan(path))
        output.append(row)
    return {'status': 'completed' if len(output) == len(plan['cases']) else 'partial',
        'engine_source_revision': plan['engine_source_revision'],
        'runtime_revision': plan['runtime_revision'], 'engine_sha256': plan['engine_sha256'],
        'tools_sha256': plan['tools_sha256'], 'cases': output,
        'timings': [{'arm': arm, 'query': query, 'second_sql_seconds': values,
                    'median_second_sql_seconds': statistics.median(values)}
                    for (arm, query), values in groups.items()],
        'plan_variant_counts': {query + '-' + kind: len(values) for (query, kind), values in plans.items()},
        'reference_score_eligible': False,
        'limits': ['Focused queries only; no full-suite score or extrapolated full-suite speedup.',
                   ('One application per arm, all reference queries twice.' if plan.get('stage') == 'qualification'
                    else 'Two application repetitions per arm; order is forward then reverse.'),
                   'Interface byte counters include storage and control traffic; they do not identify GPU payload transport.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    data = analyze(args.run)
    if args.output:
        args.output.write_text(json.dumps(data, indent=2) + '\n')
    for row in data['timings']:
        print(row['arm'], 'Q' + row['query'], row['second_sql_seconds'])
    print('Status:', data['status'], 'cases:', len(data['cases']))


if __name__ == '__main__':
    main()
