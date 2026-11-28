#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare small pinned-copy implementations with fixed engine, SQL, plans and scan work."""
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
p.add_argument('--shuffle-copy', action='store_true', help='Compare the isolated bounce-copy probe')
a = p.parse_args()
reference = None
runs = []
for label in a.labels:
    directory = a.work / 'code-profile' / label
    state = json.loads((directory / 'state.json').read_text())
    assert state['status'] == 'completed' and state['mode'] == 'control'
    assert not state['concat_probe'] and not state['scan_copy_probe'] and not state['packed_view']
    assert len(state['pinned_copy_environment_proof']) == 8
    if a.shuffle_copy:
        assert state['pinned_copy_threshold'] == 0
        assert len(state['shuffle_copy_probe']['runtime_proof']) == 8
    else:
        assert not state.get('shuffle_copy_probe')
    run = Path(state['run'])
    config = json.loads((run / 'config.json').read_text())
    profiles = json.loads((run / 'profiles.json').read_text())
    for profile in profiles.values():
        for key, value in profile.items():
            if 'LIBCUDF_KERNEL_PINNED_COPY_THRESHOLD' in key:
                assert value == str(state['pinned_copy_threshold'])
                profile[key] = '<small-pinned-copy-path>'
            elif isinstance(value, str):
                profile[key] = value.replace(run.name, '<run>')
                if a.shuffle_copy:
                    profile[key] = profile[key].replace('shuffle-copy-probe.jar=control,',
                        'shuffle-copy-probe.jar=<mode>,').replace('shuffle-copy-probe.jar=kernel,',
                        'shuffle-copy-probe.jar=<mode>,')
    timeline = schedule(run)
    parity, rows = [], []
    for sample in state['result']['samples']:
        assert sample['validation']['status'] == 'passed'
        token = f"q{sample['query']}-iteration{sample['iteration']}"
        plan = normalize((run / 'applications/suite' / (token + '.executed.txt')).read_text())
        fixed = {k: sample[k] for k in ('query', 'iteration', 'sql_sha256', 'case_conf', 'reader_metrics')}
        fixed['plan_sha256'] = hashlib.sha256(plan.encode()).hexdigest()
        parity.append(fixed)
        row = {**fixed, **{k: sample[k] for k in ('sql_seconds', 'cleanup_seconds', 'sql_plus_cleanup_seconds')}}
        row.update(pressure=pressure(directory, timeline[token]['window']),
                   schedule=timeline[token], plan_metrics=sample['plan_metrics'])
        rows.append(row)
        print(label, token, round(row['sql_seconds'], 3), 'cleanup', round(row['cleanup_seconds'], 3),
              'disk_GB', round(row['pressure']['sampled_disk_read_bytes_dm0'] / 1e9, 3),
              'memory_full_s', round(row['pressure']['sampled_memory_full_seconds'], 3), flush=True)
    fixed = {'engine_sha256': config['engine_sha256'], 'profiles': profiles, 'rows': parity}
    if a.shuffle_copy:
        fixed['probe_manifest'] = state['shuffle_copy_probe']['manifest']
    if reference is None:
        reference = fixed
    else:
        for key in fixed:
            assert fixed[key] == reference[key], (label, key)
    runs.append({'label': label, 'policy': state['shuffle_copy_probe']['implementation'] if a.shuffle_copy else state['pinned_copy_threshold'], 'samples': rows,
                 'runtime_proof': state['shuffle_copy_probe']['runtime_proof'] if a.shuffle_copy else state['pinned_copy_environment_proof']})
a.output.write_text(json.dumps({'runs': runs,
    'parity': 'Same engine, SQL, normalized plans, scan work and configuration except the selected copy path.',
    'limits': ['Cache residency is observed rather than controlled; inspect disk reads and pressure.',
               'Stage intervals overlap; CPU and operator time sums are not query wall time.',
               'Environment values alone are not executed-branch evidence; consult the paired CUDA trace.',
               'Focused trials are not a full-suite score.']}, indent=2) + '\n')
