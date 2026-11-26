#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Apply a bounded tuning gate while retaining first-action and cache evidence."""
import argparse
import json
from pathlib import Path
import statistics
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True)
a = p.parse_args()
directory = Path(a.work) / 'read-tuning'
phases = {name: read(directory / name / 'analysis.json') for name in ['A0', 'B1', 'B2', 'A3']
          if (directory / name / 'analysis.json').exists()}
assert {'A0', 'B1'} <= phases.keys()
queries = [9, 10, 11, 12, 13]
rows = []
for name, phase in phases.items():
    assert phase['pool_verified']
    assert len(phase['samples']) == 10
    assert all(s['validation']['status'] == 'passed' for s in phase['samples'])
    rows.append({'phase': name, 'threads': phase['kvikio_threads'],
                 'second_total_seconds': sum(s['sql_plus_cleanup_seconds'] for s in phase['samples'] if s['iteration'] == 2),
                 'all_action_total_seconds': sum(s['sql_plus_cleanup_seconds'] for s in phase['samples'])})
by_name = {r['phase']: r for r in rows}
complete = set(phases) == {'A0', 'B1', 'B2', 'A3'}
controls = [r for r in rows if r['phase'].startswith('A')]
candidates = [r for r in rows if r['phase'].startswith('B')]
per_query = []
plans_match = True
for q in queries:
    values = {name: [s for s in phase['samples'] if s['query'] == q] for name, phase in phases.items()}
    plans_match &= len({s['normalized_plan_sha256'] for samples in values.values() for s in samples}) == 1
    detail = {'query': q, 'phases': {name: [{
        'iteration': s['iteration'], 'sql_seconds': s['sql_seconds'],
        'sql_plus_cleanup_seconds': s['sql_plus_cleanup_seconds'],
        'physical_read_bytes': s['disk_read_bytes']['dm-0'],
        'memory_full_seconds': s['memory_full_seconds'],
        'allocstall': sum(v for k, v in s['vm_deltas'].items() if k.startswith('allocstall')),
        'scan_buffer_task_seconds': s['scan_task_seconds']['bufferTime'],
        'normalized_plan_sha256': s['normalized_plan_sha256']}
        for s in samples] for name, samples in values.items()}}
    before = [s['sql_plus_cleanup_seconds'] for name, samples in values.items() if name.startswith('A') for s in samples if s['iteration'] == 2]
    after = [s['sql_plus_cleanup_seconds'] for name, samples in values.items() if name.startswith('B') for s in samples if s['iteration'] == 2]
    detail['second_action_median_change_fraction'] = statistics.median(after) / statistics.median(before) - 1
    per_query.append(detail)
separated = max(r['second_total_seconds'] for r in candidates) <= .97 * min(r['second_total_seconds'] for r in controls)
regression_guard = all(row['second_action_median_change_fraction'] <= .05 for row in per_query if row['query'] in (9, 12, 13))
report = {'complete_ABBA': complete, 'groups': rows, 'queries': per_query,
          'normalized_plans_match': plans_match,
          'initial_second_action_regression_above_20_percent': by_name['B1']['second_total_seconds'] > 1.20 * by_name['A0']['second_total_seconds'],
          'worst_candidate_second_total_3_percent_below_best_control': separated,
          'critical_query_second_action_regression_below_5_percent': regression_guard,
          'eligible_for_full_suite': complete and plans_match and separated and regression_guard,
          'scope': 'Promotion gate for a full-suite trial, not proof of an all-query improvement',
          'limits': ['No OS cache flush; examine first-action physical reads and reclaim separately',
                     'Actual initialized native thread pools are verified for all eight executors',
                     'Thread counts differ only at fresh-executor startup; frozen SQL, data, JARs and per-query overrides are preserved']}
write(directory / 'decision.json', report)
print(json.dumps({k: v for k, v in report.items() if k != 'queries'}, indent=2))
