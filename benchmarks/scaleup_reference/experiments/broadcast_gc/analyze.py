#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Analyze retained lifecycle evidence with JVM-scoped object identities and pause windows."""
import argparse
import json
from pathlib import Path
import re


def read(path):
    return json.loads(path.read_text())


def total(rows, key):
    return sum(r[key] for r in rows)


def analyze(run):
    evidence = read(run / 'case-evidence.json')
    app = run / 'applications/suite'
    initial = read(app / 'q9-iteration1.cleanup.json')
    final = read(app / 'q3-iteration1.cleanup.json')
    mapping = {}
    for row in initial:
        node = row['hostname'].split('.')[0]
        paths = list((run / 'gc').glob(node + '/agent/lifecycle-' + str(row['jvm_pid']) + '.jsonl'))
        assert len(paths) == 1, (row, paths)
        mapping[paths[0]] = row['executor']
    assert len(mapping) == 32
    jvms = []
    for path in sorted(run.glob('gc/*/agent/*.jsonl')):
        records = [json.loads(line) for line in path.read_text().splitlines()]
        pid = path.stem.split('-')[-1]
        log = path.parent.parent / ('gc-' + pid + '.log')
        lines = log.read_text().splitlines()
        pauses = []
        for line in lines:
            duration = re.search(r'GC\(\d+\) Pause .*? (\d+(?:\.\d+)?)ms\s*$', line)
            uptime = re.search(r'\[(\d+)ms\]', line)
            if duration and uptime:
                pauses.append((int(uptime[1]), float(duration[1])))
        requests = [r for r in records if r['event'] == 'gc_requested']
        returns = [r for r in records if r['event'] == 'gc_request_returned']
        assert len(requests) == len(returns)
        windows = []
        for start, end in zip(requests, returns):
            assert start['detail'] == end['detail'] and end['uptime_ms'] >= start['uptime_ms']
            durations = [duration for uptime, duration in pauses
                         if start['uptime_ms'] <= uptime <= end['uptime_ms']]
            windows.append({'phase': start['detail'],
                            'request_call_ms': end['uptime_ms'] - start['uptime_ms'],
                            'max_pause_ms': max(durations, default=0),
                            'sum_pauses_ms': sum(durations), 'pause_events': len(durations)})
        executor = mapping.get(path, 'driver')
        if executor == 'driver':
            assert any(r['event'] == 'phase' and r['detail'].startswith('sql-start-') for r in records)
        jvms.append({'executor': executor, 'node': path.parents[1].name, 'pid': pid,
                     'requests': windows,
                     'transformed_definitions': sum(r['event'] == 'class_transformed' for r in records),
                     'ignored_duplicate_agents': sum(r['event'] == 'duplicate_agent_skipped' for r in records)})
    assert len(jvms) == 33 and sum(j['executor'] == 'driver' for j in jvms) == 1
    evidence['after_q9'] = {'ordinary_objects': sum(len(r['objects']) for r in initial),
                            'ordinary_objects_before_action': sum(len(r['objects_before_action']) for r in initial),
                            'host_blocks': sum(o['host_block_present'] for r in initial for o in r['objects']),
                            'rmm_before_action_bytes': total(initial, 'rmm_before_action'),
                            'rmm_bytes': total(initial, 'rmm_bytes'),
                            'host_allocated_bytes': total(initial, 'host_allocated_bytes')}
    evidence['after_q3'] = {'ordinary_objects': sum(len(r['objects']) for r in final),
                            'rmm_bytes': total(final, 'rmm_bytes'),
                            'host_allocated_bytes': total(final, 'host_allocated_bytes')}
    windows = [r for j in jvms for r in j['requests']]
    evidence['deliberate_gc_call_windows'] = {
        'count': len(windows), 'max_single_jvm_pause_ms': max((r['max_pause_ms'] for r in windows), default=None),
        'sum_jvm_pauses_ms': sum(r['sum_pauses_ms'] for r in windows),
        'scope': 'GC pause end events within each instrumented System.gc entry/return; excludes startup and SQL; sum across JVMs is not wall time'}
    if evidence['scenario'] == 'mechanism':
        driver_phases = sorted(app.glob('driver-blocks-*.observation.json'),
                               key=lambda p: int(p.name.split('.')[0].split('-')[-1]))
        after_phases = sorted(app.glob('executor-finalized-*.observation.json'),
                              key=lambda p: int(p.name.split('.')[0].split('-')[-1]))
        if driver_phases and after_phases:
            driver, after = read(driver_phases[-1]), read(after_phases[-1])
            targets = {r['executor']: {o['object_id'] for o in r['objects']} for r in initial}
            evidence['after_driver_gc'] = {'ordinary_objects': sum(len(r['objects']) for r in driver),
                                           'host_blocks': sum(o['host_block_present'] for r in driver for o in r['objects']),
                                           'rmm_bytes': total(driver, 'rmm_bytes'),
                                           'host_allocated_bytes': total(driver, 'host_allocated_bytes')}
            evidence['after_executor_gc'] = {'ordinary_objects': sum(len(r['objects']) for r in after),
                                             'rmm_bytes': total(after, 'rmm_bytes'),
                                             'host_allocated_bytes': total(after, 'host_allocated_bytes'),
                                             'unfinalized_target_objects': sum(len(targets[r['executor']] -
                                                 set(r['finalized_object_ids'])) for r in after)}
    evidence['jvm_gc_call_evidence'] = jvms
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert read(args.run / 'status.json')['status'] == 'completed'
    rows = [analyze(case) for case in sorted((args.run / 'runs').iterdir())
            if (case / 'case-evidence.json').exists()]
    args.output.write_text(json.dumps({'source_plan': read(args.run / 'plan.json'), 'cases': rows}, indent=2) + '\n')
    print(json.dumps([{k: v for k, v in row.items() if k not in ['jvm_gc_call_evidence', 'mechanism']}
                      for row in rows], indent=2))


if __name__ == '__main__':
    main()
