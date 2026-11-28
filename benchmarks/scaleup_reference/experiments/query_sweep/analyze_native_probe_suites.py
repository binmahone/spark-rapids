#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Verify full-suite parity and report complete-application native probe totals."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from finalize_sol_reuse import normalize
from variants import ORDER

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--labels', nargs='+', required=True)
p.add_argument('--focused', action='store_true')
p.add_argument('--output', type=Path)
a = p.parse_args()
runs = []
reference = None
for label in a.labels:
    state = json.loads((a.work / 'code-profile' / label / 'state.json').read_text())
    assert state['status'] == 'completed' and state['mode'] == 'control'
    run = Path(state['run'])
    config = json.loads((run / 'config.json').read_text())
    profiles = json.loads((run / 'profiles.json').read_text())
    queries = config['queries']
    assert queries == [q for q in ORDER if q in queries]
    if not a.focused:
        assert queries == list(ORDER)
    assert config['engine_sha256'] == 'ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
    assert len(state['concat_probe']['runtime_proof']) == 8
    assert all('NATIVE_PROBE_RMM_SHARED registry=' in Path(item['stderr']).read_text()
               for item in state['concat_probe']['runtime_proof']), 'Native adapter did not verify shared RMM state'
    for profile in profiles.values():
        for key, value in profile.items():
            if isinstance(value, str):
                profile[key] = re.sub(r'concat-offset-probe.jar=(control|candidate),',
                                     'concat-offset-probe.jar=<implementation>,',
                                     value.replace(run.name, '<run>'))
    rows = []
    for sample in state['result']['samples']:
        assert sample['validation']['status'] == 'passed'
        q, iteration = sample['query'], sample['iteration']
        plan = normalize((run / f'applications/suite/q{q}-iteration{iteration}.executed.txt').read_text())
        row = {k: sample[k] for k in ('query', 'iteration', 'sql_sha256', 'case_conf',
               'reader_metrics', 'sql_seconds', 'cleanup_seconds', 'sql_plus_cleanup_seconds')}
        row['plan_sha256'] = hashlib.sha256(plan.encode()).hexdigest()
        rows.append(row)
    assert [(r['query'], r['iteration']) for r in rows] == [(q, i) for q in queries for i in (1, 2)]
    parity = {'profiles': profiles, 'engine_sha256': config['engine_sha256'],
              'agent_sha256': state['concat_probe']['agent_sha256'],
              'library_sha256': state['concat_probe']['library_sha256'],
              'rows': [{k: r[k] for k in ('query', 'iteration', 'sql_sha256', 'case_conf',
                        'reader_metrics', 'plan_sha256')} for r in rows]}
    if reference is None:
        reference = parity
    else:
        for key in parity:
            assert parity[key] == reference[key], (label, key, 'Parity differs')
    selected = [min((r for r in rows if r['query'] == q), key=lambda r: r['sql_seconds']) for q in queries]
    summary = {'label': label, 'mode': state['concat_probe']['implementation'],
               'application_id': state['result']['application_id'],
               'sql_seconds': sum(r['sql_seconds'] for r in selected),
               'paired_cleanup_seconds': sum(r['cleanup_seconds'] for r in selected),
               'sql_plus_paired_cleanup_seconds': sum(r['sql_plus_cleanup_seconds'] for r in selected),
               'second_action_sql_seconds': sum(r['sql_seconds'] for r in rows if r['iteration'] == 2),
               'selected': selected, 'all_actions': rows}
    print({k: v for k, v in summary.items() if k not in ('selected', 'all_actions')})
    runs.append(summary)
output = {'classification': 'Experimental native component; not a stock-main score or a theoretical bound',
          'scope': 'Focused query subset' if a.focused else 'Full 22-query suite',
          'method': 'Within each complete application, choose the faster SQL action per query with its matching cleanup.',
          'parity': 'Same SQL, normalized physical plan, native read work, effective configuration, and probe artifacts.',
          'limits': ['Page-cache residency is not controlled.', 'Native binding is a diagnostic adapter, not a full JNI rebuild.'],
          'validated_actions': sum(len(r['all_actions']) for r in runs), 'runs': runs}
assert not a.focused or a.output is not None, 'Focused comparisons need a separate output path'
(a.output or a.work / 'code-profile/native-suite-comparison.json').write_text(json.dumps(output, indent=2) + '\n')
