# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Apply the declared end-to-end gate to completed original/candidate repeats."""
import argparse
import hashlib
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
a = p.parse_args()
root = a.work / 'critical-path'
path = root / 'comparison.json'
comparison = json.loads(path.read_text())
phases = comparison['phases']
assert {'A0', 'B1', 'C1', 'C2', 'A3'} <= phases.keys()
groups, rows = {}, []
for phase in ['A0', 'B1', 'C1', 'C2', 'A3']:
    samples = phases[phase]['samples']
    assert len(samples) == 6 and all(s['validation']['status'] == 'passed' for s in samples)
    groups[phase] = {(s['query'], s['iteration']): s for s in samples}
    for q in [9, 7, 8]:
        values = [groups[phase][q, i] for i in [1, 2]]
        rows.append({'phase': phase, 'query': q,
            'sql_seconds': [s['sql_seconds'] for s in values],
            'sql_plus_cleanup_seconds': [s['sql_plus_cleanup_seconds'] for s in values],
            'best_of_two_sql_seconds': min(s['sql_seconds'] for s in values),
            'second_shuffle_bytes': values[1]['plan_shuffle_write_bytes'],
            'second_memory_full_seconds': values[1]['sampled_memory_full_seconds'],
            'second_sampled_disk_read_bytes': values[1]['sampled_disk_read_bytes_dm0']})
decisions = []
for q in [9, 7]:
    controls = [groups[p][q, 2]['sql_plus_cleanup_seconds'] for p in ['A0', 'A3']]
    candidates = [groups[p][q, 2]['sql_plus_cleanup_seconds'] for p in ['C1', 'C2']]
    accepted = max(candidates) <= .97 * min(controls)
    decisions.append({'query': q, 'accepted_for_focused_experiment': accepted,
        'control_second_seconds_including_cleanup': controls,
        'candidate_second_seconds_including_cleanup': candidates,
        'conservative_measured_reduction_fraction': 1 - max(candidates) / min(controls)})
    if q == 9:
        reference = groups['A0'][q, 2]
        for phase in ['C1', 'C2', 'A3']:
            for i in [1, 2]:
                sample = groups[phase][q, i]
                assert sample['normalized_plan_sha256'] == reference['normalized_plan_sha256']
                assert sample['sql_sha256'] == reference['sql_sha256']
                assert sample['reader_metrics'] == reference['reader_metrics']
                if phase.startswith('C'):
                    proof = sample['runtime_hash_reuse_proof']
                    assert proof['physical_plan_unchanged'] and proof['ordinary_join_reuses'] > 0
                    assert proof['restored_value'] == 'false'
    else:
        for phase in ['C1', 'C2']:
            for i in [1, 2]:
                sample = groups[phase][q, i]
                assert any('BroadcastHashJoin' in line and 'o_custkey' in line
                           and 'c_custkey' in line for line in sample['join_plan'])
                assert sample['plan_shuffle_write_bytes'] < .5 * groups['A0'][q, i]['plan_shuffle_write_bytes']
result = {'comparison_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    'confirmation_order': ['A0', 'C1', 'C2', 'A3'], 'measurements': rows, 'decisions': decisions,
    'validation_actions': sum(len(r['samples']) for r in phases.values()),
    'scope': 'Focused Q7/Q9 mechanism validation; Q8 is an unchanged temporal control',
    'q9_global_switch_probe': 'B1 also changed driverless transports; excluded from isolated reuse confirmation',
    'q9_implementation': 'Diagnostic execution-only reuse with an unchanged physical plan; production integration remains separate',
    'q7_implementation': 'Explicit SQL join-shape and broadcast-placement prototype, not a production optimizer rule',
    'full_suite_score': None, 'full_suite_status': 'Not measured for this candidate',
    'cache_limit': 'No cache flush; pressure and disk counters retained; samples are not cache-identical',
    'instance_policy': 'Retain instance and dataset'}
(root / 'decision.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(decisions, indent=2))
