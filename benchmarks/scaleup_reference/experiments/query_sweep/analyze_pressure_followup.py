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
"""Correlate completed reclaim-policy trials with runtime pressure and execution counters."""
import argparse
from bisect import bisect_left, bisect_right
import hashlib
import json
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from analyze_sweep import summarize


def records(path):
    return [json.loads(line) for line in path.open() if line.endswith('\n')]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True)
    p.add_argument('--mode', choices=('screen', 'full'), required=True)
    args = p.parse_args()
    work = Path(args.work)
    directory = work / 'pressure-followup' / args.mode
    state = read(directory / 'state.json')
    assert state['status'] == 'completed'
    assert state['restored']['settings']['watermark_scale_factor'] == '10'
    assert state['monitor_exit_codes'] == [0, 0]
    assert (work / 'evidence' / ('pressure-' + args.mode + '.exit')).read_text().strip() == '0'
    host, vm = records(directory / 'host-monitor.jsonl'), records(directory / 'vm-monitor.jsonl')
    host_times, vm_times = [s['time_unix_ns'] for s in host], [s['time_unix_ns'] for s in vm]
    groups, samples = [], []
    target_queries = {12, 13, 18}
    for group in state['groups']:
        data = summarize(work, Path(group['run']))
        assert data['status'] == 'completed'
        rows = []
        for sample in data['samples']:
            begin, end = [sample['execution_window_ms'][key] * 1_000_000 for key in ('start', 'end')]
            h = host[bisect_left(host_times, begin):bisect_right(host_times, end)]
            v = vm[bisect_left(vm_times, begin):bisect_right(vm_times, end)]
            assert len(h) >= 2 and len(v) >= 2
            assert all(row['watermark_scale_factor'] == group['watermark_scale_factor'] for row in v)
            pressure = {}
            for kind in ('memory', 'io', 'cpu'):
                for severity in ('some', 'full'):
                    pattern = '^' + severity + r'.*total=(\d+)'
                    first = re.search(pattern, h[0]['pressure_' + kind], re.M)
                    last = re.search(pattern, h[-1]['pressure_' + kind], re.M)
                    if first and last:
                        pressure[kind + '_' + severity + '_seconds'] = (int(last[1]) - int(first[1])) / 1e6
            allocation = {key: v[-1]['vmstat'][key] - v[0]['vmstat'][key] for key in v[0]['vmstat']}
            before = {row['pid']: row for row in h[0]['executors']}
            after = {row['pid']: row for row in h[-1]['executors']}
            assert len(before.keys() & after.keys()) == 8
            row = {key: sample[key] for key in ('query', 'iteration', 'sql_seconds', 'cleanup_seconds',
                'sql_plus_cleanup_seconds', 'task_metrics', 'reader_metrics', 'validation', 'execution_window_ms')}
            row.update(arm=group['arm'], index=group['index'], run=data['run'],
                watermark_scale_factor=group['watermark_scale_factor'], pressure=pressure,
                allocation=allocation, allocstall_count=sum(value for key, value in allocation.items() if key.startswith('allocstall')),
                physical_read_bytes=sum(after[pid]['io'].get('read_bytes', 0) - before[pid]['io'].get('read_bytes', 0)
                                        for pid in before.keys() & after.keys()),
                dm0_read_bytes=(h[-1]['diskstats']['dm-0'][2] - h[0]['diskstats']['dm-0'][2]) * 512,
                normalized_plan_sha256=hashlib.sha256(sample['normalized_plan'].encode()).hexdigest(),
                min_mem_free_kib=min(x['memory_kib']['MemFree'] for x in h),
                min_mem_available_kib=min(x['memory_kib']['MemAvailable'] for x in h),
                min_node_free_kib={node: min(x['node_memory_kib'][node]['MemFree'] for x in v)
                                   for node in v[0]['node_memory_kib']},
                sampling_boundary_gap_seconds=((h[0]['time_unix_ns'] - begin) + (end - h[-1]['time_unix_ns'])) / 1e9)
            rows.append(row)
            samples.append(row)
        groups.append({'arm': group['arm'], 'index': group['index'], 'run': data['run'],
            'sum_all_actions_total_seconds': sum(s['sql_plus_cleanup_seconds'] for s in rows),
            'sum_target_actions_total_seconds': sum(s['sql_plus_cleanup_seconds'] for s in rows if s['query'] in target_queries),
            'sum_second_actions_total_seconds': sum(s['sql_plus_cleanup_seconds'] for s in rows if s['iteration'] == 2),
            'sum_best_sql_seconds': sum(min(s['sql_seconds'] for s in rows if s['query'] == q) for q in state['queries']),
            'sum_best_total_seconds': sum(min(s['sql_plus_cleanup_seconds'] for s in rows if s['query'] == q) for q in state['queries']),
            'memory_full_seconds': sum(s['pressure']['memory_full_seconds'] for s in rows),
            'allocstall_count': sum(s['allocstall_count'] for s in rows),
            'physical_read_bytes': sum(s['physical_read_bytes'] for s in rows)})
    controls, candidates = ([g for g in groups if g['arm'] == arm] for arm in ('A', 'B'))
    assert len(controls) == len(candidates) == 2
    comparisons = {key: {'controls': [g[key] for g in controls], 'candidates': [g[key] for g in candidates]}
                   for key in groups[0] if key not in ('arm', 'index', 'run')}
    plan_checks = [{'query': q, 'same_normalized_plan': len({s['normalized_plan_sha256'] for s in samples if s['query'] == q}) == 1,
                   'native_read_byte_values': sorted({s['task_metrics']['native_host_read_bytes'] for s in samples if s['query'] == q})}
                  for q in state['queries']]
    pressure_reproduced = all(g['memory_full_seconds'] >= 1 or g['allocstall_count'] >= 1000 for g in controls)
    timing_gate = all(max(g[key] for g in candidates) <= .97 * min(g[key] for g in controls)
                      for key in ('sum_all_actions_total_seconds', 'sum_second_actions_total_seconds'))
    stability_gate = (
        max(g['sum_target_actions_total_seconds'] for g in candidates) <=
        .90 * min(g['sum_target_actions_total_seconds'] for g in controls) and
        max(g['sum_all_actions_total_seconds'] for g in candidates) <=
        1.03 * min(g['sum_all_actions_total_seconds'] for g in controls) and
        max(g['memory_full_seconds'] for g in candidates) <=
        .50 * min(g['memory_full_seconds'] for g in controls))
    report = {'classification': 'Controlled single-host measurements; no general kernel tuning recommendation',
              'groups': groups, 'samples': samples, 'comparisons': comparisons,
              'plan_checks': plan_checks, 'pressure_reproduced_in_both_controls': pressure_reproduced,
              'conservative_timing_gate': timing_gate,
              'conservative_stability_gate': stability_gate,
              'merits_full_suite_confirmation': pressure_reproduced and (timing_gate or stability_gate) and all(x['same_normalized_plan'] for x in plan_checks),
              'restored_watermark_scale_factor': 10,
              'limits': ['One-second host sampling omits incomplete boundary intervals',
                         'No physical cache flush; identical native read bytes do not imply identical physical reads',
                         'First and second actions are both included in the all-actions timing gate',
                         'Summed task-duration metrics are not wall time']}
    write(directory / 'analysis.json', report)
    print(json.dumps({key: value for key, value in report.items() if key != 'samples'}, indent=2))


if __name__ == '__main__':
    main()
