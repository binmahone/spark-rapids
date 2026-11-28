#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Evaluate data path granularity with matched plans, input work and runtime profiles."""
import argparse
import hashlib
import json
from pathlib import Path

from finalize_sol_reuse import normalize
from verify_slot_overlap import scheduler_overlap


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--family', choices=('scan-pack', 'shuffle-buffer', 'shuffle-wait'), required=True)
    parser.add_argument('--phases', nargs='+', default=['A0', 'B1', 'B2', 'A3'])
    args = parser.parse_args()
    rows = []
    configs = []
    profiles = []
    identities = {}
    for phase in args.phases:
        state = json.loads((args.work / f'data-path/{args.family}-{phase}/state.json').read_text())
        assert state['status'] == 'completed'
        assert all(r['exit_code'] == 0 for r in state['observers'])
        app = Path(state['run']) / 'applications/suite'
        run = Path(state['run'])
        config = json.loads((run / 'config.json').read_text())
        assert config['spark_conf']['spark.rapids.shuffle.compression.codec'] == 'none'
        kind = 'candidate' if state['candidate'] else 'control'
        identities.setdefault(kind, config['engine_sha256'])
        assert identities[kind] == config['engine_sha256']
        if args.family != 'shuffle-wait' or not state['candidate']:
            assert config['engine_sha256'] == 'ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
        slots = state['slots_per_executor']
        assert slots == 4
        assert config['spark_conf']['spark.task.resource.gpu.amount'] == '0.25'
        changed_key = state['changed_key']
        if changed_key:
            expected = state['overrides'][changed_key]
            assert config['spark_conf'][changed_key] == expected
            config['spark_conf'][changed_key] = '<experimental-setting>'
        excluded = {'engine', 'engine_sha256', 'experimental_engine_manifest'} if args.family == 'shuffle-wait' else set()
        configs.append({k: v for k, v in config.items() if k not in excluded})
        profile = json.loads((run / 'profiles.json').read_text())
        for entries in profile.values():
            if changed_key:
                assert entries[changed_key] == expected
                entries[changed_key] = '<experimental-setting>'
            for key, value in entries.items():
                if isinstance(value, str):
                    entries[key] = value.replace(config['engine'], '<engine>').replace(run.name, '<run>')
        profiles.append(profile)
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
                'reader_metrics': sample['reader_metrics'],
                'scan_summed_task_seconds': {key: sum(n['metrics'].get(key, 0)
                    for n in sample['plan_metrics'] if n['node'].startswith('GpuScan')) / 1e9
                    for key in ('bufferTime', 'gpuDecodeTime', 'opTimeNew', 'opTimeNew_exSemWait')},
                'scheduler_overlap': scheduler_overlap(app, q, slots)})
    assert all(c == configs[0] for c in configs), 'Configuration differences exceed the selected data-path setting'
    assert all(p == profiles[0] for p in profiles), 'Effective query profiles differ'
    if args.family == 'shuffle-wait':
        assert identities['candidate'] != identities['control']
    decisions = []
    for query in sorted({r['query'] for r in rows}):
        group = [r for r in rows if r['query'] == query]
        for key in ('sql_sha256', 'plan_sha256', 'reader_metrics'):
            assert all(r[key] == group[0][key] for r in group), (query, key)
        candidates = [r['sql_seconds'] for r in group if r['candidate']]
        controls = [r['sql_seconds'] for r in group if not r['candidate']]
        complete = len(candidates) >= 2 and len(controls) >= 2
        decisions.append({'query': query, 'complete_repeats': complete,
                          'sql_gate_passed': complete and max(candidates) <= .97 * min(controls),
                          'accepted': complete and max(candidates) <= .97 * min(controls) and
                              max(r['sql_plus_cleanup_seconds'] for r in group if r['candidate']) <=
                              min(r['sql_plus_cleanup_seconds'] for r in group if not r['candidate']),
                          'control_seconds': controls, 'candidate_seconds': candidates})
    output = {'rows': rows, 'decisions': decisions, 'engine_sha256': identities,
              'limits': ['Focused query times are not a full-suite total.',
                         'Operator timer sums are not query wall time or GPU utilization.',
                         'Cache residency is observed, not controlled.']}
    path = args.work / 'data-path' / (args.family + '-comparison-' + '-'.join(args.phases) + '.json')
    path.write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
