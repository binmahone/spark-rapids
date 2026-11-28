#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare copy-admission trials using query time and matching work and plans."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from analyze_critical_path import pressure, schedule
from finalize_sol_reuse import normalize

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--labels', nargs='+', required=True)
p.add_argument('--output', type=Path)
a = p.parse_args()
runs, reference = [], None
for label in a.labels:
    state = json.loads((a.work / 'code-profile' / label / 'state.json').read_text())
    assert state['status'] == 'completed' and state['mode'] == 'control'
    assert not state['concat_probe']
    probe = state['scan_copy_probe']
    assert len(probe['runtime_proof']) == 8
    assert all('NATIVE_PROBE_RMM_SHARED registry=' in Path(item['stderr']).read_text()
               for item in probe['runtime_proof']), 'Native adapter did not verify shared RMM state'
    run = Path(state['run'])
    profiles = json.loads((run / 'profiles.json').read_text())
    for profile in profiles.values():
        for key, value in profile.items():
            if isinstance(value, str):
                profile[key] = re.sub(r'scan-copy-probe.jar=(original|sync|pair|paced-control|paced-queued|paced|serial-original|serial-unlocked|serial-locked),',
                    'scan-copy-probe.jar=<implementation>,', value.replace(run.name, '<run>'))
    rows, parity = [], []
    timeline = schedule(run)
    for sample in state['result']['samples']:
        assert sample['validation']['status'] == 'passed'
        token = f"q{sample['query']}-iteration{sample['iteration']}"
        plan = normalize((run / 'applications/suite' / (token + '.executed.txt')).read_text())
        fixed = {key: sample[key] for key in ('query', 'iteration', 'sql_sha256',
                    'case_conf', 'reader_metrics')}
        fixed['plan_sha256'] = hashlib.sha256(plan.encode()).hexdigest()
        parity.append(fixed)
        row = {**fixed, **{key: sample[key] for key in ('sql_seconds', 'cleanup_seconds',
                'sql_plus_cleanup_seconds')}}
        row['pressure'] = pressure(a.work / 'code-profile' / label, timeline[token]['window'])
        row['schedule'] = timeline[token]
        row['plan_metrics'] = sample['plan_metrics']
        rows.append(row)
        print(label, token, round(row['sql_seconds'], 3),
              'disk_GB', round(row['pressure']['sampled_disk_read_bytes_dm0'] / 1e9, 3),
              'memory_full_s', round(row['pressure']['sampled_memory_full_seconds'], 3))
    fixed = {'profiles': profiles, 'rows': parity,
             'agent_sha256': probe['agent_sha256'], 'library_sha256': probe['library_sha256'],
             'pair_mapping': probe['pair_mapping']}
    if reference is None:
        reference = fixed
    else:
        for key in fixed:
            assert fixed[key] == reference[key], (label, key)
    runs.append({'label': label, 'mode': probe['implementation'], 'samples': rows,
                 'runtime_proof': probe['runtime_proof']})
report = {'runs': runs, 'parity': 'Same SQL, normalized plans, read work, configuration and artifacts',
          'limits': ['Cache residency is not controlled; inspect measured disk reads and memory pressure.',
                     'Synchronization moves wait time between operators; query elapsed time is the objective.',
                     'Stage spans overlap and task metric sums are not query elapsed time.',
                     'This native adapter is a diagnostic experiment, not a full JNI product build.']}
(a.output or a.work / 'code-profile/scan-copy-comparison.json').write_text(json.dumps(report, indent=2) + '\n')
