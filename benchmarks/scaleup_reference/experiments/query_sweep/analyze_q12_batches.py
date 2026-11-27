#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Audit reader reclaim accounting and summarize complete one-second Q12 buckets."""
import argparse
import json
from pathlib import Path
import re

from analyze_q12_diagnosis import summarize

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
p.add_argument('--phase', required=True)
a = p.parse_args()
directory = a.work / 'q12-diagnosis' / a.phase
state = json.loads((directory / 'state.json').read_text())
assert state['status'] == 'completed' and state['observer_mode'] == 'batches'
assert all(x['exit_code'] == 0 for x in state['profilers'])
data = json.loads((directory / 'lock-analysis.json').read_text())
contents = (directory / 'reclaim.log').read_text()
assert 'READY monotonic_ns=' in contents
assert not re.search(r'\blost\b|\bdropped\b|\bERROR\b', contents, re.I)
scalars = {k: int(v) for k, v in re.findall(r'^@(\w+): (\d+)$', contents, re.M)}
assert scalars.get('batch_begins', 0) == scalars.get('batch_ends', 0)
assert scalars.get('batch_reentry', 0) == 0
assert not re.search(r'^@batch_(begin|order)\[', contents, re.M)
maps = {}
for name, second, value in re.findall(r'^@(\w+)\[(\d+)\]: (\d+)$', contents, re.M):
    bucket = maps.setdefault(name, {})
    assert int(second) not in bucket
    bucket[int(second)] = int(value)
for name, total in [('calls', 'batch_ends'), ('pages', 'total_pages'),
                    ('duration_ns', 'total_duration_ns')]:
    assert sum(maps.get(name, {}).values()) == scalars.get(total, 0)
events = []
for line in contents.splitlines():
    if line.startswith('BATCH_RECLAIM '):
        event = dict(re.findall(r'(\w+)=(\S+)', line))
        event = {k: v if k == 'comm' else int(v) for k, v in event.items()}
        assert event['end_ns'] - event['begin_ns'] == event['duration_ns'] > 50000000
        events.append(event)
offset = data['monotonic_to_unix_offset_ns']
all_samples = summarize(a.work, Path(state['groups'][0]['run']))['samples']
host = [json.loads(line) for line in (directory / 'host.jsonl').open()]
whole_run_longest = []
for event in sorted(events, key=lambda e: e['duration_ns'], reverse=True)[:10]:
    begin, end = event['begin_ns'] + offset, event['end_ns'] + offset
    queries = [{'query': s['query'], 'iteration': s['iteration']} for s in all_samples
               if s['execution_window_ms']['start'] * 1000000 <= begin
               and end <= s['execution_window_ms']['end'] * 1000000]
    nearby = [r for r in host if begin - 1000000000 <= r['time_unix_ns'] <= end + 1000000000]
    gaps = [(y['time_unix_ns'] - x['time_unix_ns']) / 1e9 for x, y in zip(nearby, nearby[1:])]
    whole_run_longest.append({**event, 'fully_containing_sql': queries,
                              'nearby_host_samples': len(nearby),
                              'nearby_max_sample_gap_seconds': max(gaps) if gaps else None})
samples = []
for sample in data['samples']:
    begin = sample['execution_window_ms']['start'] * 1000000 - offset
    end = sample['execution_window_ms']['end'] * 1000000 - offset
    first, last = (begin + 999999999) // 1000000000, end // 1000000000
    selected = range(first, last)
    totals = {name: sum(values.get(second, 0) for second in selected)
              for name, values in maps.items() if not name.startswith('max_')
              and not name.endswith('_max')}
    maxima = {name: max((values.get(second, 0) for second in selected), default=0)
              for name, values in maps.items() if name.startswith('max_') or name.endswith('_max')}
    slow = [e for e in events if begin <= e['begin_ns'] and e['end_ns'] <= end]
    calls = totals.get('calls', 0)
    samples.append({'iteration': sample['iteration'], 'sql_seconds': sample['sql_seconds'],
                    'full_completion_bucket_seconds': [first, last],
                    'bucket_totals': totals, 'bucket_maxima': maxima,
                    'derived_pages_per_call': totals.get('pages', 0) / calls if calls else None,
                    'derived_duration_ms_per_call': totals.get('duration_ns', 0) / calls / 1e6 if calls else None,
                    'slow_calls_fully_inside_sql': len(slow),
                    'longest_complete_calls': sorted(slow, key=lambda e: e['duration_ns'], reverse=True)[:10]})
output = {'phase': a.phase, 'mglru_enabled': state['host_state_before']['/sys/kernel/mm/lru_gen/enabled'],
          'audit': scalars, 'samples': samples, 'whole_run_longest_calls': whole_run_longest,
          'limits': ['Reader threads only, identified by actual kvikio thread names.',
                     'Buckets group calls by completion second; durations can begin before the selected window.',
                     'Only complete one-second buckets within SQL boundaries are included; edge buckets are excluded.',
                     'Duration sums include concurrent threads and are not query wall time.',
                     'shrink_lruvec entry alone is not proof of classic execution when MGLRU is enabled.',
                     'The proportional eligibility count describes entry state, not actual pages reclaimed by that invocation.',
                     'Means are derived from recorded totals, not individual-call distribution quantiles.']}
(directory / 'batch-analysis.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps(output, indent=2))
