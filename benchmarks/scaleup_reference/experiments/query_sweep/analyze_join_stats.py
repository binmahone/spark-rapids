#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Evaluate join-stats execution against paired controls with matched input work."""
import argparse
import hashlib
import json
from pathlib import Path

from finalize_sol_reuse import normalize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--phases', nargs='+', default=['A0', 'B1', 'B2', 'A3'])
    args = parser.parse_args()
    rows = []
    for phase in args.phases:
        state = json.loads((args.work / f'join-stats/{phase}/state.json').read_text())
        assert state['status'] == 'completed'
        assert all(r['exit_code'] == 0 for r in state['observers'])
        app = Path(state['run']) / 'applications/suite'
        for sample in state['result']['samples']:
            assert sample['validation']['status'] == 'passed'
            if sample['iteration'] != 2:
                continue
            q = sample['query']
            rows.append({'phase': phase, 'query': q, 'candidate': state['candidate'],
                'sql_seconds': sample['sql_seconds'], 'cleanup_seconds': sample['cleanup_seconds'],
                'sql_plus_cleanup_seconds': sample['sql_plus_cleanup_seconds'],
                'sql_sha256': sample['sql_sha256'],
                'plan_sha256': hashlib.sha256(normalize((app / f'q{q}-iteration2.executed.txt').read_text()).encode()).hexdigest(),
                'reader_metrics': sample['reader_metrics']})
    decisions = []
    for query in sorted({r['query'] for r in rows}):
        group = [r for r in rows if r['query'] == query]
        for key in ('sql_sha256', 'plan_sha256', 'reader_metrics'):
            assert all(r[key] == group[0][key] for r in group), (query, key)
        candidates = [r['sql_plus_cleanup_seconds'] for r in group if r['candidate']]
        controls = [r['sql_plus_cleanup_seconds'] for r in group if not r['candidate']]
        complete = len(candidates) >= 2 and len(controls) >= 2
        decisions.append({'query': query, 'complete_repeats': complete,
                          'accepted': complete and max(candidates) <= .97 * min(controls),
                          'control_seconds': controls, 'candidate_seconds': candidates})
    output = {'rows': rows, 'decisions': decisions,
              'limits': ['Focused query times are not a full-suite total.',
                         'Cache residency is observed, not controlled.']}
    path = args.work / 'join-stats' / ('comparison-' + '-'.join(args.phases) + '.json')
    path.write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
