#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare packed-view code trials with plans, scheduling and pressure controls."""
import argparse
import hashlib
import json
from pathlib import Path

from analyze_critical_path import pressure, schedule
from finalize_sol_reuse import normalize

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--labels', nargs='+', required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
runs, reference = [], None
for label in a.labels:
    state = json.loads((a.work / 'code-profile' / label / 'state.json').read_text())
    assert state['status'] == 'completed' and state['mode'] == 'control'
    assert not state['concat_probe'] and not state['scan_copy_probe']
    candidate = state.get('packed_view', False)
    run = Path(state['run'])
    config = json.loads((run / 'config.json').read_text())
    expected = ('39bd6c1d6826000d4b646055bda0a5ac1977c4c24004d949e3e2191eccc00f8d' if candidate
                else 'ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc')
    assert config['engine_sha256'] == expected
    profiles = json.loads((run / 'profiles.json').read_text())
    for profile in profiles.values():
        for key, value in profile.items():
            if isinstance(value, str):
                profile[key] = value.replace(run.name, '<run>').replace('/artifacts/packed-view/', '/artifacts/decimal-null-copy/')
    timeline = schedule(run)
    rows, parity = [], []
    for sample in state['result']['samples']:
        assert sample['validation']['status'] == 'passed'
        token = f"q{sample['query']}-iteration{sample['iteration']}"
        plan = normalize((run / 'applications/suite' / (token + '.executed.txt')).read_text())
        fixed = {key: sample[key] for key in ('query', 'iteration', 'sql_sha256', 'case_conf', 'reader_metrics')}
        fixed['plan_sha256'] = hashlib.sha256(plan.encode()).hexdigest()
        parity.append(fixed)
        row = {**fixed, **{key: sample[key] for key in ('sql_seconds', 'cleanup_seconds', 'sql_plus_cleanup_seconds')}}
        row['pressure'] = pressure(a.work / 'code-profile' / label, timeline[token]['window'])
        row['schedule'] = timeline[token]
        row['plan_metrics'] = sample['plan_metrics']
        rows.append(row)
        print(label, token, round(row['sql_seconds'], 3), 'disk_GB',
              round(row['pressure']['sampled_disk_read_bytes_dm0'] / 1e9, 3),
              'memory_full_s', round(row['pressure']['sampled_memory_full_seconds'], 3))
    fixed = {'profiles': profiles, 'rows': parity}
    if reference is None:
        reference = fixed
    else:
        for key in fixed:
            assert fixed[key] == reference[key], (label, key)
    selected = [min((r for r in rows if r['query'] == q), key=lambda r: r['sql_seconds'])
                for q in state['queries']]
    runs.append({'label': label, 'candidate': candidate, 'engine_sha256': expected,
                 'application_id': state['result']['application_id'], 'samples': rows,
                 'selected_sql_seconds': sum(r['sql_seconds'] for r in selected),
                 'paired_cleanup_seconds': sum(r['cleanup_seconds'] for r in selected),
                 'second_action_sql_seconds': sum(r['sql_seconds'] for r in rows if r['iteration'] == 2)})
report = {'runs': runs, 'parity': 'Same SQL, normalized plans, native read work and configuration; only audited Java artifact differs',
          'limits': ['Page-cache residency is not controlled; inspect measured disk reads and pressure.',
                     'Stage spans overlap and summed task metrics are not query elapsed time.',
                     'A compiled Java class overlay is used; this is not a full distribution build.']}
a.output.write_text(json.dumps(report, indent=2) + '\n')
