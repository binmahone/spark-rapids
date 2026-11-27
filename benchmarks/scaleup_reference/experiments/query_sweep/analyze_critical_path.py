# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Attribute measured task work to stage intervals without summing overlapping wall times."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import statistics
from urllib.parse import urlparse

from analyze_sweep import summarize


def schedule(run):
    profile = json.loads((run / 'applications/suite/profile.json').read_text())
    executions, windows, roots, parents = {}, {}, defaultdict(set), {}
    metrics, task_rows, counters = {}, defaultdict(list), defaultdict(Counter)
    def walk(node):
        for m in node.get('metrics', []):
            metrics[str(m['accumulatorId'])] = {'node': node['nodeName'],
                'description': node['simpleString'], 'name': m['name'], 'type': m['metricType']}
        for child in node.get('children', []):
            walk(child)
    for path in sorted(Path(urlparse(profile['spark.eventLog.dir']).path).iterdir()):
        if not path.is_file() or path.name.startswith('.'):
            continue
        assert not path.name.endswith('.inprogress')
        for line in path.open():
            e = json.loads(line)
            kind = e.get('Event', '')
            if kind.endswith('SparkListenerSQLExecutionStart'):
                key = str(e['executionId'])
                if re.fullmatch(r'q\d+-iteration\d+', e.get('description', '')):
                    executions[key] = e['description']
                    windows[key] = {'start': e['time']}
                walk(e['sparkPlanInfo'])
            elif kind.endswith('SparkListenerSQLExecutionEnd'):
                key = str(e['executionId'])
                if key in windows:
                    windows[key]['end'] = e['time']
            elif kind == 'SparkListenerJobStart':
                root = (e.get('Properties') or {}).get('spark.sql.execution.root.id')
                if root is not None:
                    for stage in e['Stage IDs']:
                        roots[stage].add(str(root))
            elif kind == 'SparkListenerStageSubmitted':
                info = e['Stage Info']
                parents[info['Stage ID']] = info.get('Parent IDs', [])
            elif kind == 'SparkListenerTaskEnd':
                info, m, stage = e['Task Info'], e.get('Task Metrics') or {}, e['Stage ID']
                assert not info.get('Failed') and not info.get('Killed'), 'Failed tasks need separate attribution'
                task_rows[stage].append({'start': info['Launch Time'], 'end': info['Finish Time'],
                    'executor': info['Executor ID'], 'run_ms': m.get('Executor Run Time', 0),
                    'cpu_ns': m.get('Executor CPU Time', 0),
                    'input': (m.get('Input Metrics') or {}).get('Bytes Read', 0),
                    'shuffle_write': (m.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written', 0)})
                for item in info.get('Accumulables', []):
                    value = str(item.get('Update', ''))
                    if re.fullmatch(r'-?\d+', value):
                        counters[stage][str(item['ID'])] += int(value)
    result = {}
    for root, token in executions.items():
        window = windows[root]
        assert 'end' in window
        stages = []
        bins = defaultdict(Counter)
        for stage, rows in task_rows.items():
            if root not in roots[stage]:
                continue
            assert len({executions[r] for r in roots[stage] if r in executions}) == 1
            begin, end = min(r['start'] for r in rows), max(r['end'] for r in rows)
            duration = [(r['end'] - r['start']) / 1000 for r in rows]
            for r in rows:
                start_ms, end_ms = r['start'] - window['start'], r['end'] - window['start']
                for bin_id in range(start_ms // 500, (end_ms + 499) // 500):
                    overlap = max(0, min(end_ms, (bin_id + 1) * 500) - max(start_ms, bin_id * 500))
                    bins[bin_id][str(stage)] += overlap / 500
            metric_values = [{**metrics[k], 'accumulator_id': k, 'task_update_sum': value}
                             for k, value in counters[stage].items() if k in metrics and value]
            stages.append({'stage': stage, 'parents': parents.get(stage, []),
                'start_seconds': (begin - window['start']) / 1000,
                'end_seconds': (end - window['start']) / 1000,
                'span_seconds': (end - begin) / 1000, 'tasks': len(rows),
                'task_wall_sum_seconds': sum(duration), 'task_wall_median_seconds': statistics.median(duration),
                'task_wall_max_seconds': max(duration),
                'task_run_sum_seconds': sum(r['run_ms'] for r in rows) / 1000,
                'task_cpu_sum_seconds': sum(r['cpu_ns'] for r in rows) / 1e9,
                'input_bytes': sum(r['input'] for r in rows),
                'shuffle_write_bytes': sum(r['shuffle_write'] for r in rows),
                'operator_metrics': metric_values})
        result[token] = {'window': window, 'stages': sorted(stages, key=lambda r: r['start_seconds']),
                         'task_occupancy_half_second': [{'start_seconds': k / 2, 'stage_mean_running_tasks': dict(v)}
                                                        for k, v in sorted(bins.items())]}
    return result


def pressure(directory, window):
    host = [json.loads(line) for line in (directory / 'host.jsonl').open()]
    vm = [json.loads(line) for line in (directory / 'vm.jsonl').open()]
    begin, end = window['start'] * 1000000, window['end'] * 1000000
    h, v = [[r for r in stream if begin <= r['time_unix_ns'] <= end] for stream in [host, vm]]
    assert len(h) >= 2 and len(v) >= 2
    def full(r):
        return int(re.search(r'^full.*total=(\d+)', r['pressure_memory'], re.M)[1])
    return {'sampled_disk_read_bytes_dm0': (h[-1]['diskstats']['dm-0'][2] - h[0]['diskstats']['dm-0'][2]) * 512,
            'sampled_memory_full_seconds': (full(h[-1]) - full(h[0])) / 1e6,
            'sampled_allocstall': sum(v[-1]['vmstat'][k] - v[0]['vmstat'][k]
                                     for k in v[0]['vmstat'] if k.startswith('allocstall_')),
            'host_sampled_seconds': (h[-1]['time_unix_ns'] - h[0]['time_unix_ns']) / 1e9}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True, type=Path)
    p.add_argument('--run', type=Path, help='Inspect one retained run without comparing experiment phases')
    p.add_argument('--output', type=Path)
    a = p.parse_args()
    if a.run:
        assert a.output
        a.output.parent.mkdir(parents=True, exist_ok=True)
        a.output.write_text(json.dumps(schedule(a.run), indent=2) + '\n')
        return
    reports = {}
    for directory in sorted((a.work / 'critical-path').iterdir()):
        if not directory.is_dir() or directory.name == 'oracle':
            continue
        state = json.loads((directory / 'state.json').read_text())
        if state['status'] != 'completed':
            continue
        assert state['pool_verified'] and all(r['exit_code'] == 0 for r in state['profilers'])
        group = state['groups'][0]
        run = Path(group['run'])
        assert len(group['samples']) == 6 and all(s['validation']['status'] == 'passed' for s in group['samples'])
        data, timeline = summarize(a.work, run), schedule(run)
        for s, raw in zip(data['samples'], group['samples']):
            assert (s['query'], s['iteration']) == (raw['query'], raw['iteration'])
            token = f"q{s['query']}-iteration{s['iteration']}"
            s['normalized_plan_sha256'] = hashlib.sha256(s.pop('normalized_plan').encode()).hexdigest()
            s['sql_sha256'] = raw['sql_sha256']
            s['case_conf'] = raw['case_conf']
            s['plan_metrics'] = raw['plan_metrics']
            if 'runtime_hash_reuse_proof' in raw:
                s['runtime_hash_reuse_proof'] = raw['runtime_hash_reuse_proof']
            s.update(pressure(directory, s['execution_window_ms']))
            s['schedule'] = timeline[token]
        reports[directory.name] = data
    result = {'phases': reports,
        'scope': 'Focused mechanism probes; no full-suite qualification or strict cache-state matching',
        'limits': ['Stage spans can overlap; do not add them as SQL wall time.',
                   'Operator task updates sum across concurrent tasks and are not elapsed query time.',
                   'Task occupancy counts scheduled tasks, not active CUDA work.',
                   'Pressure and disk counters omit sampling boundary fragments.',
                   'Q7 changes explicit join shape and broadcast placement; no isolated component speedup is claimed.']}
    (a.work / 'critical-path/comparison.json').write_text(json.dumps(result, indent=2) + '\n')
    for phase, report in reports.items():
        for s in report['samples']:
            reuses = sum(n['metrics'].get('hashTableReuses', 0) for n in s['plan_metrics'])
            print(phase, s['query'], s['iteration'], round(s['sql_seconds'], 3),
                  'shuffleGB', round(s['plan_shuffle_write_bytes'] / 1e9, 2), 'hash_reuses', reuses,
                  'psi', round(s['sampled_memory_full_seconds'], 3))


if __name__ == '__main__':
    main()
