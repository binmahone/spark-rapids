#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Accept repeatable execution-only reuse gains with identical plans and scan work."""
import argparse
import hashlib
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write


def normalize(plan):
    plan = re.sub(r'#\d+L?', '#', plan)
    plan = re.sub(r'\[(?:plan_id|loreId)=[^\]]*\]', '', plan)
    return re.sub(r'@[0-9a-f]+', '@', plan)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--screen-only', action='store_true',
                   help='Reject candidates that cannot meet the declared gate even with further repeats')
    a = p.parse_args()
    groups, rows = {}, []
    phases = ('R0', 'R1') if a.screen_only else ('R0', 'R1', 'R2', 'R3')
    for phase in phases:
        state = read(a.work / 'critical-path' / phase / 'state.json')
        assert state['status'] == 'completed' and state['pool_verified']
        assert all(p['exit_code'] == 0 for p in state['profilers'])
        group = state['groups'][0]
        assert group['driver_exit_code'] == 0 and group['status'] == 'completed'
        assert len(group['samples']) == 6
        app = Path(group['run']) / 'applications/suite'
        groups[phase] = {}
        for sample in group['samples']:
            assert sample['validation']['status'] == 'passed'
            q, i = sample['query'], sample['iteration']
            plan = normalize((app / f'q{q}-iteration{i}.executed.txt').read_text())
            reuse = sum(n['metrics'].get('hashTableReuses', 0) for n in sample['plan_metrics'])
            if phase in ('R1', 'R2'):
                proof = sample['runtime_hash_reuse_proof']
                assert proof['physical_plan_unchanged'] and proof['restored_value'] == 'false'
                assert reuse > 0
            else:
                assert reuse == 0
            groups[phase][q, i] = {'phase': phase, 'query': q, 'iteration': i,
                'sql_sha256': sample['sql_sha256'],
                'plan_sha256': hashlib.sha256(plan.encode()).hexdigest(),
                'reader_metrics': sample['reader_metrics'],
                'sql_seconds': sample['sql_seconds'],
                'total_seconds': sample['sql_plus_cleanup_seconds'],
                'hash_table_reuses': reuse,
                'hash_table_builds': sum(n['metrics'].get('hashTableBuilds', 0) for n in sample['plan_metrics'])}
            rows.append(groups[phase][q, i])
        assert set(groups[phase]) == {(q, i) for q in (5, 19, 20) for i in (1, 2)}
    decisions = []
    for q in (5, 19, 20):
        reference = groups['R0'][q, 2]
        for phase in groups:
            for i in (1, 2):
                for key in ('sql_sha256', 'plan_sha256', 'reader_metrics'):
                    assert groups[phase][q, i][key] == reference[key], (phase, q, i, key)
        control = [groups[p][q, 2]['total_seconds'] for p in phases if p in ('R0', 'R3')]
        candidate = [groups[p][q, 2]['total_seconds'] for p in phases if p in ('R1', 'R2')]
        meets_gate = max(candidate) <= .97 * min(control)
        decisions.append({'query': q, 'accepted': meets_gate and not a.screen_only,
                          'eligible_for_confirmation': meets_gate,
                          'control_second_total_seconds': control,
                          'candidate_second_total_seconds': candidate,
                          'conservative_reduction_fraction': 1 - max(candidate) / min(control)})
    result = {'gate': 'Slower candidate second action including cleanup must beat faster control by at least 3 percent',
              'order': list(phases), 'measurements': rows, 'decisions': decisions,
              'screen_only': a.screen_only,
              'early_rejection_reason': ('The slower-of-candidates and faster-of-controls gate cannot '
                  'recover when the first candidate already misses 3 percent against the first control.'
                  if a.screen_only else None),
              'full_suite_reuse_queries': [9] + [d['query'] for d in decisions if d['accepted']],
              'limits': ['Diagnostic execution-only switch; production integration is separate',
                         'No forced page-cache residency; preserve native work and inspect pressure']}
    write(a.work / 'critical-path' / ('sol-reuse-screen.json' if a.screen_only else 'sol-reuse-decision.json'), result)
    for d in decisions:
        print(d)
    print('Full-suite reuse queries:', result['full_suite_reuse_queries'])


if __name__ == '__main__':
    main()
