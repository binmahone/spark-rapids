#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Audit matched experiment inputs, executed read branches, and query-window costs."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from analyze_q12_diagnosis import stage_windows, summarize

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
a = p.parse_args()
reports, invariants, plans, sql, reader_bytes = {}, [], {}, {}, {}
for directory in sorted((a.work / 'pressure-host-read').glob('*')):
    if not (directory / 'state.json').exists():
        continue
    state = json.loads((directory / 'state.json').read_text())
    if state['status'] != 'completed':
        continue
    assert state['native_pool_verification']['passed']
    assert all(x['exit_code'] == 0 for x in state['profilers'])
    for record in [json.loads((directory / 'controller.json').read_text()), *state['profilers']]:
        stat = Path('/proc') / str(record['pid']) / 'stat'
        assert not stat.exists() or stat.read_text().split(') ', 1)[1].split()[19] != str(record['start_time'])
    group = state['groups'][0]
    assert group['status'] == 'completed' and group['driver_exit_code'] == 0
    assert len(group['samples']) == 26 and all(x['validation']['status'] == 'passed' for x in group['samples'])
    run = Path(group['run'])
    config = json.loads((run / 'config.json').read_text())
    conf = {k: v for k, v in config['spark_conf'].items()
            if k != 'spark.executorEnv.LD_PRELOAD' and not k.startswith('spark.executorEnv.SCALEUP_COLD_READ_')}
    invariants.append({**{k: config[k] for k in ('engine_sha256', 'dataset', 'queries', 'case_variants',
                          'case_overrides', 'view_overrides', 'experiment_source_sha256')}, 'spark_conf': conf,
                       'thread_id': state['thread_id'], 'instance_id': state['instance_id'],
                       'adapter_sha256': state['adapter_sha256'], 'source_sha256': state['source_sha256']})
    data = summarize(a.work, run)
    stages = stage_windows(run)
    def rows(name):
        return [json.loads(x) for x in (directory / name).read_text().splitlines()]
    host, vm, native, pressure = [rows(x) for x in ('host.jsonl', 'vm.jsonl', 'native.jsonl', 'pressure-policy.jsonl')]
    assert len(pressure) > 2
    pressure_gaps = [(y['monotonic_ns'] - x['monotonic_ns']) / 1e9 for x, y in zip(pressure, pressure[1:])]
    samples = []
    for sample in data['samples']:
        query, iteration = sample['query'], sample['iteration']
        key = (query, iteration)
        plan = hashlib.sha256(sample.pop('normalized_plan').encode()).hexdigest()
        assert plans.setdefault(key, plan) == plan, key
        raw = next(s for s in group['samples'] if (s['query'], s['iteration']) == key)
        assert sql.setdefault(key, raw['sql_sha256']) == raw['sql_sha256']
        native_bytes = raw['reader_metrics']['hybridScanNativeHostBytesRead']
        assert reader_bytes.setdefault(key, native_bytes) == native_bytes, key
        begin = sample['execution_window_ms']['start'] * 1000000
        end = sample['execution_window_ms']['end'] * 1000000
        h, v, n = [[r for r in stream if begin <= r['time_unix_ns'] <= end] for stream in (host, vm, native)]
        assert len(h) >= 2 and len(v) >= 2 and len(n) >= 2
        vm_delta = {k: v[-1]['vmstat'][k] - v[0]['vmstat'][k] for k in v[0]['vmstat']}
        native_delta = {k: n[-1]['totals'][k] - n[0]['totals'][k] for k in n[0]['totals']}
        assert all(x >= 0 for x in native_delta.values()), key
        active_ns = sum(max(0, min(end, y['time_unix_ns']) - max(begin, x['time_unix_ns']))
                        for x, y in zip(pressure, pressure[1:]) if x['active'])
        stage = stages[f'q{query}-iteration{iteration}']
        scans = sorted([s for s in stage if s['input_bytes']], key=lambda s: s['start_ms'])
        samples.append({'query': query, 'iteration': iteration, 'sql_seconds': sample['sql_seconds'],
                        'cleanup_seconds': raw['cleanup_seconds'], 'normalized_plan_sha256': plan,
                        'native_host_read_bytes': native_bytes, 'execution_window_ms': sample['execution_window_ms'],
                        'sampled_disk_read_bytes_dm0': (h[-1]['diskstats']['dm-0'][2] - h[0]['diskstats']['dm-0'][2]) * 512,
                        'sampled_memory_full_seconds': (int(re.search(r'^full.*total=(\d+)', h[-1]['pressure_memory'], re.M)[1]) -
                                                       int(re.search(r'^full.*total=(\d+)', h[0]['pressure_memory'], re.M)[1])) / 1e6,
                        'sampled_allocstall': sum(v for k, v in vm_delta.items() if k.startswith('allocstall_')),
                        'vm_deltas': vm_delta, 'native_counter_deltas': native_delta,
                        'pressure_active_seconds_estimate': active_ns / 1e9,
                        'host_max_gap_seconds': max((y['time_unix_ns'] - x['time_unix_ns']) / 1e9 for x, y in zip(h, h[1:])),
                        'sampled_seconds': (h[-1]['time_unix_ns'] - h[0]['time_unix_ns']) / 1e9,
                        'scan_stages': scans})
    reports[directory.name] = {'read_mode': state['read_mode'], 'samples': samples,
                               'all_action_sql_seconds': sum(s['sql_seconds'] for s in samples),
                               'all_action_sql_plus_cleanup_seconds': sum(s['sql_seconds'] + s['cleanup_seconds'] for s in samples),
                               'native_totals': state.get('native_totals'),
                               'pressure_max_sample_gap_seconds': max(pressure_gaps),
                               'pressure_transitions': [r for r in pressure if r['reason'].startswith('sustained_')],
                               'runtime_source_revision': state['source_revision']}
assert reports
assert all(x == invariants[0] for x in invariants[1:]), 'Matched inputs changed'
result = {'classification': 'Fixed-prefix screening; no full-suite score or isolated cache-state equivalence',
          'invariants_verified': invariants[0], 'phases': reports,
          'limits': ['Host, VM, and native counters cover sampled intervals, excluding boundary fragments.',
                     'Pressure-active seconds are estimated by holding each sampled decision until the next sample.',
                     'The same query order does not guarantee the same page residency, disk reads, or scan scheduling.',
                     'Native direct_bytes counts requested absent spans; aligned physical reads and retries can differ.',
                     'The adapter is experimental, for immutable local files, and is not an integrated JNI implementation.']}
(a.work / 'pressure-host-read/comparison.json').write_text(json.dumps(result, indent=2) + '\n')
for phase, report in reports.items():
    print(phase, 'prefix_sql', round(report['all_action_sql_seconds'], 3),
          'transitions', len(report['pressure_transitions']))
    for s in report['samples']:
        if s['query'] in (10, 12, 13):
            print(s['query'], s['iteration'], 'sql', round(s['sql_seconds'], 3),
                  'diskGiB', round(s['sampled_disk_read_bytes_dm0'] / 2**30, 2),
                  'directGiB', round(s['native_counter_deltas']['direct_bytes'] / 2**30, 2),
                  'psi', round(s['sampled_memory_full_seconds'], 3),
                  'allocstall', s['sampled_allocstall'])
