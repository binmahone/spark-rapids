#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Compare complete full-suite applications without stitching query variants across trials."""
import argparse
import json
import os
from pathlib import Path
import statistics

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('label')
args = parser.parse_args()
source = root/'evidence'/('sol-' + args.label + '.json')
data = json.loads(source.read_text())
assert all(m['status'] == 'completed' for m in data['matrices'])
assert {r['probe_arm'] for r in data['rows']} == {'baseline', 'combined'}
assert len({tuple(r['gpu_indices']) for r in data['rows']}) == 1
assert len({a['engine_sha256'] for a in data['applications']}) == 1
assert len({a['dataset'] for a in data['applications']}) == 1
assert len({json.dumps(a['dataset_contract'],sort_keys=True) for a in data['applications']}) == 1
configurations = {a['probe_arm']: a['effective_conf'] for a in data['applications']}
changed_conf = {key: [configurations['baseline'].get(key), configurations['combined'].get(key)]
                for key in set(configurations['baseline']) | set(configurations['combined'])
                if configurations['baseline'].get(key) != configurations['combined'].get(key)}
assert set(changed_conf) <= {'spark.sql.shuffle.partitions'}, changed_conf
queries = list(range(1, 23))
arms = {}
for arm in ('baseline', 'combined'):
    rows = [r for r in data['rows'] if r['probe_arm'] == arm]
    assert len({r['application_id'] for r in rows}) == 1
    assert len(rows) == 44 and {r['query'] for r in rows} == set(queries)
    assert all(r['validation']['status'] == 'passed' for r in rows)
    assert all(r['sql_shuffle_crosscheck_available'] and
               r['sql_accumulator_shuffle_write_bytes'] == r['task_metrics']['shuffle_write_bytes']
               for r in rows)
    by_query = {}
    for query in queries:
        selected = sorted((r for r in rows if r['query'] == query), key=lambda r: r['iteration'])
        assert [r['iteration'] for r in selected] == [1, 2]
        by_query[str(query)] = {
            'query_arm': selected[0].get('query_arm', arm),
            'sql_seconds': [r['sql_seconds'] for r in selected],
            'sql_plus_cleanup_seconds': [r['sql_plus_cleanup_seconds'] for r in selected],
            'reader_bytes': [r['reader_metrics_all_branches']['hybridScanNativeHostBytesRead'] for r in selected],
            'shuffle_bytes': [r['task_metrics']['shuffle_write_bytes'] for r in selected],
            'memory_spill_bytes': [r['task_metrics'].get('memory_spill_bytes', 0) for r in selected],
            'disk_spill_bytes': [r['task_metrics'].get('disk_spill_bytes', 0) for r in selected],
            'thermal': [r['thermal'] for r in selected]}
    arms[arm] = {'application_id': rows[0]['application_id'], 'queries': by_query,
        'sum_query_best_sql_seconds': sum(min(q['sql_seconds']) for q in by_query.values()),
        'sum_query_best_sql_plus_cleanup_seconds': sum(min(q['sql_plus_cleanup_seconds']) for q in by_query.values()),
        'sum_first_pass_sql_seconds': sum(q['sql_seconds'][0] for q in by_query.values()),
        'sum_second_pass_sql_seconds': sum(q['sql_seconds'][1] for q in by_query.values()),
        'sum_query_median_sql_seconds': sum(statistics.median(q['sql_seconds']) for q in by_query.values())}
base = arms['baseline']; candidate = arms['combined']
reductions = {key: 1 - candidate[key]/base[key] for key in base if key.startswith('sum_')}
report = {'classification': 'Complete measured four-GPU baseline versus combined SQL prototype',
    'absolute_hardware_speed_of_light_established': False,
    'healthy_node_performance_qualified': False,
    'limits': data['limits'], 'source_evidence': str(source), 'matrices': data['matrices'],
    'gpu_indices': data['rows'][0]['gpu_indices'], 'extra_rmm_alloc_size_limit': False,
    'engine_sha256':data['applications'][0]['engine_sha256'],
    'dataset':data['applications'][0]['dataset'],
    'dataset_contract':data['applications'][0]['dataset_contract'],
    'applications':data['applications'],
    'effective_configuration_changes': changed_conf,
    'query_execution_count': len(data['rows']), 'all_existing_reference_comparisons_passed': True,
    'arms': arms, 'observed_time_reduction_fraction': reductions}
output = root/'evidence'/('comparison-' + args.label + '.json')
output.write_text(json.dumps(report, indent=2)+'\n')
for name, arm in arms.items():
    print(name, {k: round(v, 3) for k, v in arm.items() if k.startswith('sum_')})
print('observed_reductions', reductions)
for query in queries:
    before = base['queries'][str(query)]; after = candidate['queries'][str(query)]
    print('Q'+str(query), before['query_arm'], after['query_arm'],
          [round(x, 3) for x in before['sql_seconds']], [round(x, 3) for x in after['sql_seconds']])
print(output)
