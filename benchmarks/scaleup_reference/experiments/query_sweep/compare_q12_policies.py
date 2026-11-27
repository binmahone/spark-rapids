#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare completed lightweight controls with explicit cache and scheduling limits."""
import argparse
import json
import os
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
p.add_argument('--phases', nargs='+', required=True)
a = p.parse_args()
rows, invariants, host_settings, owners, plans = [], [], [], set(), set()
for phase in a.phases:
    directory = a.work / 'q12-diagnosis' / phase
    state = json.loads((directory / 'state.json').read_text())
    data = json.loads((directory / 'lock-analysis.json').read_text())
    assert state['status'] == 'completed' and state['observer_mode'] == 'light'
    assert state['native_pool_verification']['passed']
    owners.add((state['thread_id'], state['instance_id']))
    group = state['groups'][0]
    assert group['status'] == 'completed' and group['driver_exit_code'] == 0
    assert len(group['samples']) == 24
    assert all(s['validation']['status'] == 'passed' for s in group['samples'])
    config = json.loads((Path(group['run']) / 'config.json').read_text())
    invariant_keys = ['engine_sha256', 'dataset', 'queries', 'spark_conf', 'case_variants',
                      'case_overrides', 'view_overrides', 'experiment_source_sha256']
    invariants.append({k: config[k] for k in invariant_keys})
    host_settings.append({k: v for k, v in state['host_state_before'].items()
                          if k not in ('/proc/meminfo', '/sys/kernel/mm/lru_gen/enabled')})
    setting = int(state['host_state_before']['/sys/kernel/mm/lru_gen/enabled'].strip(), 0)
    for s in data['samples']:
        plans.add(s['normalized_plan_sha256'])
        leaves = sorted((x for x in s['stages'] if x['input_bytes']), key=lambda x: x['start_ms'])
        assert len(leaves) == 2
        scan = max(leaves, key=lambda x: x['input_bytes'])
        rows.append({'phase': phase, 'mglru_enabled_mask': setting, 'iteration': s['iteration'],
                     'sql_seconds': s['sql_seconds'], 'large_input_scan_seconds': scan['span_seconds'],
                     'largest_scan_task_seconds': scan['task_seconds']['max'],
                     'scan_order_input_bytes': [x['input_bytes'] for x in leaves],
                     'scan_overlap_seconds': max(0, leaves[0]['end_ms'] - leaves[1]['start_ms']) / 1000,
                     'sampled_interval_seconds': s['host_sampled_seconds'],
                     'physical_read_bytes_dm0': s['disk_read_bytes']['dm-0'],
                     'memory_full_seconds': s['memory_full_seconds'],
                     'vm_deltas': s['vm_deltas'], 'memory_kib_range': s['memory_kib_range'],
                     'host_max_gap_seconds': s['host_max_gap_seconds']})
assert len(owners) == len(plans) == 1
assert all(value == invariants[0] for value in invariants[1:]), 'Experiment inputs changed'
assert all(value == host_settings[0] for value in host_settings[1:]), 'Other host settings changed'
prefix = {}
for phase in a.phases:
    state = json.loads((a.work / 'q12-diagnosis' / phase / 'state.json').read_text())
    prefix[phase] = [{k: s[k] for k in ('query', 'iteration', 'sql_seconds')}
                     for s in state['groups'][0]['samples']]
report = {'classification': 'Diagnostic policy comparison; not a full-suite benchmark score',
          'owner': list(owners)[0], 'q12_plan_sha256': next(iter(plans)),
          'invariants_verified': invariants[0], 'page_size_bytes': os.sysconf('SC_PAGE_SIZE'),
          'other_host_settings_verified': host_settings[0],
          'q12_samples': rows, 'prefix_samples': prefix,
          'limits': ['Identical code, configuration and query sequence do not imply identical cache history.',
                     'No cache flush; scan order and overlap are recorded for each sample.',
                     'Disk and VM deltas cover sampled intervals rather than exact SQL boundaries.',
                     'MGLRU pgscan counts isolated pages; classic LRU pgscan counts scanned pages in the inspected kernel.',
                     'The first no-probe control showed a large tail that later controls may not reproduce.']}
target = a.work / 'evidence/q12-policy-comparison.json'
target.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps([{k: row[k] for k in ('phase', 'iteration', 'sql_seconds',
                  'large_input_scan_seconds', 'largest_scan_task_seconds')} for row in rows], indent=2))
