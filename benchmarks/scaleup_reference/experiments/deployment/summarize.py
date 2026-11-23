#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Combine retained deployment controls and full qualification with explicit evidence boundaries."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re

from analyze import analyze, read


def assignment_audit(root, plan):
    result = []
    for case in plan['cases']:
        run = root / 'runs' / Path(case['run']).name
        if not (run / 'case-evidence.json').exists():
            continue
        application = read(run / 'applications/suite/result.json')
        inventory = set()
        for path in (run / 'nodes').glob('*.json'):
            node = read(path)
            if isinstance(node, dict):
                inventory.update(re.findall(r'GPU-[0-9a-f-]+', node.get('gpu', '')))
        assignments = Counter(row['uuid'] for row in application['gpu_assignments'])
        if set(assignments) != inventory or len(inventory) != 32 or set(assignments.values()) != {4}:
            raise AssertionError('Physical CUDA UUID assignment differs from node inventory')
        workers = []
        for path in (run / 'services/suite').glob('*.json'):
            if path.name.endswith('.stopped.json'):
                if read(path)['remaining_recorded_processes']:
                    raise AssertionError('Owned services survived teardown')
            else:
                workers.append(sum('org.apache.spark.deploy.worker.Worker' in r['command']
                                   for r in read(path)))
        expected = 2 if case['arm'] == 'per-numa' else 4
        if len(workers) != 8 or set(workers) != {expected}:
            raise AssertionError('Actual worker layout differs from the tested variant')
        result.append({'label': case['label'], 'physical_gpu_count': 32,
                       'workers_per_node': expected})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--placement', required=True, type=Path)
    parser.add_argument('--reverse', required=True, type=Path)
    parser.add_argument('--workers', required=True, type=Path)
    parser.add_argument('--full', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    experiments, focused = {}, defaultdict(lambda: defaultdict(list))
    roots = {'placement': args.placement, 'reverse': args.reverse, 'workers': args.workers}
    if args.full:
        roots['full'] = args.full
    identity = None
    samples = 0
    for name, root in roots.items():
        data = analyze(root)
        plan = read(root / 'plan.json')
        current = (data['engine_sha256'], data['tools_sha256'])
        if identity is not None and current != identity:
            raise AssertionError('Experiments used different artifacts')
        identity = current
        data['job_id'] = read(root / 'submission.json')['job_id']
        data['controller_status'] = read(root / 'status.json')
        data['assignment_audit'] = assignment_audit(root, plan)
        for case in data['cases']:
            samples += 2 * len(case['queries'])
            if name in ('placement', 'reverse'):
                for query, value in case['queries'].items():
                    focused[case['arm']][query].append(value['second_sql_seconds'])
        experiments[name] = data
    failed_run = args.placement / 'runs' / next(
        Path(c['run']).name for c in read(args.placement / 'plan.json')['cases'] if c['arm'] == 'auto-net')
    failure = read(failed_run / 'applications/suite/result.json')
    records = [read(p) for p in (failed_run / 'runtime').glob('*/*.json') if p.stem.isdigit()]
    if failure['status'] != 'failed' or failure['samples'] or len(records) != 32:
        raise AssertionError('The unqualified auto-net outcome differs from the retained evidence')
    for record in records:
        env = dict(item.split('=', 1) for item in record['environment'])
        if 'UCX_NET_DEVICES' in env or env.get('UCX_TLS') != '^posix':
            raise AssertionError('Failed arm did not reach the intended environment')
    complete = (len(experiments['placement']['cases']) == 4 and
                len(experiments['reverse']['cases']) == 4 and
                len(experiments['workers']['cases']) == 4 and
                'full' in experiments and len(experiments['full']['cases']) == 3)
    output = {'status': 'complete' if complete else 'partial',
        'engine_sha256': identity[0], 'tools_sha256': identity[1],
        'validated_timed_samples': samples, 'experiments': experiments,
        'focused_second_execution_seconds': dict(focused),
        'unqualified_configuration': {'arm': 'auto-net', 'job_id': 25472,
            'attempted_query': 9, 'completed_samples': 0, 'observed_executor_environments': 32,
            'observed_failure': 'UCX handshake/transfer failures and RapidsShuffleFetchFailedException',
            'root_cause': 'Not established; an unrestricted device choice failed in this run.'},
        'scope': {'nodes': 8, 'gpus': 32, 'workload': 'TPC-H SF30000, retained native-double GPFS dataset',
                  'worker_comparison': 'Four GPU-specific workers versus two NUMA-specific workers per node; one GPU per executor.',
                  'library_comparison': 'Identical cuFile libraries in an independent directory; no cuFile version upgrade or complete-image qualification.',
                  'timing': 'SQL planning through JVM collect; cleanup reported separately; excludes application startup and validation.'}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + '\n')
    print('Analysis:', output['status'], 'validated timed samples:', samples)


if __name__ == '__main__':
    main()
