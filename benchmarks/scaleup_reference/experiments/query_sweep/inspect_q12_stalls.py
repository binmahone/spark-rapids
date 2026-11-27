#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Correlate measured reclaim durations and thread stacks with closed Q12 windows."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import statistics
from urllib.parse import urlparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory
    analysis = json.loads((directory / 'analysis.json').read_text())
    offset = analysis['perf_monotonic_to_unix_offset_ns']
    events, current = [], None
    for line in (directory / 'reclaim.log').read_text().splitlines():
        if line.startswith('SLOW '):
            current = dict(re.findall(r'(\w+)=(\S+)', line))
            for key in current.keys() - {'comm'}:
                current[key] = int(current[key])
            current['stack'] = []
            events.append(current)
        elif line.startswith('        ') and current is not None:
            current['stack'].append(line.strip())
        elif not line.strip() and current is not None:
            continue
        else:
            current = None
    results = []
    rows = [json.loads(line) for line in (directory / 'threads.jsonl').open()]
    state = json.loads((directory / 'state.json').read_text())
    run = Path(state['groups'][0]['run'])
    profile = json.loads((run / 'applications/suite/profile.json').read_text())
    event_root = Path(urlparse(profile['spark.eventLog.dir']).path)
    workers = {p.parent.name: int(p.parents[3].name.split('-')[-1])
               for p in event_root.parent.glob('suite/worker-*/work/app-*/*/stderr')}
    assert len(workers) == 8, workers
    scan_stages = {max(s['stages'], key=lambda stage: stage['input_bytes'])['stage']
                   for s in analysis['samples']}
    tasks = defaultdict(list)
    for path in event_root.iterdir():
        if not path.is_file() or path.name.startswith('.'):
            continue
        assert not path.name.endswith('.inprogress')
        for line in path.open():
            if 'SparkListenerTaskEnd' not in line:
                continue
            event = json.loads(line)
            if event['Stage ID'] not in scan_stages:
                continue
            info, metrics = event['Task Info'], event['Task Metrics']
            tasks[event['Stage ID']].append({
                'id': info['Task ID'], 'executor': info['Executor ID'],
                'worker': workers[info['Executor ID']], 'launch_ms': info['Launch Time'],
                'finish_ms': info['Finish Time'],
                'seconds': (info['Finish Time'] - info['Launch Time']) / 1000,
                'cpu_seconds': metrics.get('Executor CPU Time', 0) / 1e9,
                'gc_ms': metrics.get('JVM GC Time'),
                'accumulators': [a for a in info.get('Accumulables', []) if any(
                    k in a.get('Name', '').lower() for k in ('buffer', 'decode', 'spill', 'semaphore'))]})
    for sample in analysis['samples']:
        begin = sample['execution_window_ms']['start'] * 10**6
        end = sample['execution_window_ms']['end'] * 10**6
        selected = [r for r in rows if begin <= r['time_unix_ns'] <= end]
        assert selected
        stalls = []
        for event in events:
            overlap = min(end, event['end_ns'] + offset) - max(begin, event['begin_ns'] + offset)
            if overlap > 0:
                stalls.append({**event, 'overlap_seconds': overlap / 1e9,
                               'begin_from_sql_seconds': (event['begin_ns'] + offset - begin) / 1e9})
        by_thread = defaultdict(lambda: {'samples': 0, 'states': Counter(), 'cpus': Counter(),
                                         'stacks': Counter(), 'first': None, 'last': None})
        kernel = Counter()
        zones = defaultdict(list)
        for row in selected:
            for entry in row['executors']:
                for item in entry['threads']:
                    key = (entry['pid'], item['tid'], item['name'])
                    data = by_thread[key]
                    data['samples'] += 1
                    data['states'][item['state']] += 1
                    data['cpus'][item['processor']] += 1
                    if item.get('stack'):
                        data['stacks'][item['stack']] += 1
                    ticks = {'utime': item['utime'], 'stime': item['stime'],
                             'time_unix_ns': row['time_unix_ns']}
                    if data['first'] is None:
                        data['first'] = ticks
                    data['last'] = ticks
            for item in row['kernel_threads']:
                kernel[item['stack']] += 1
            for zone in row['zones']:
                values = {}
                for value in zone['values']:
                    match = re.fullmatch(r'(pages free|min|low|high)\s+(\d+)', value)
                    if match:
                        values[match[1]] = int(match[2])
                zones[zone['name']].append(values)
        threads = []
        for (pid, tid, name), data in by_thread.items():
            ticks = sum(data['last'][k] - data['first'][k] for k in ('utime', 'stime'))
            if name.startswith('kvikio-') or ticks > 100 or data['states'].get('D'):
                threads.append({'pid': pid, 'tid': tid, 'name': name, 'cpu_ticks_delta': ticks,
                                **data, 'stacks': data['stacks'].most_common(12)})
        threads.sort(key=lambda t: t['cpu_ticks_delta'], reverse=True)
        result = {'iteration': sample['iteration'], 'sql_seconds': sample['sql_seconds'],
                  'execution_window_ms': sample['execution_window_ms'],
                  'slow_reclaim_events': sorted(stalls, key=lambda e: e['overlap_seconds'], reverse=True),
                  'summed_slow_reclaim_thread_seconds': sum(e['overlap_seconds'] for e in stalls),
                  'thread_samples': len(selected),
                  'sampler_mean_seconds': statistics.mean(r['sampler_seconds'] for r in selected),
                  'sampler_max_seconds': max(r['sampler_seconds'] for r in selected),
                  'threads': threads, 'kernel_stacks': kernel.most_common(20),
                  'zones': {k: {'minimum_free_pages': min(r['pages free'] for r in v),
                                'minimum_low_margin_pages': min(r['pages free'] - r['low'] for r in v),
                                'first': v[0], 'last': v[-1]} for k, v in zones.items()}}
        scan = max(sample['stages'], key=lambda stage: stage['input_bytes'])
        values = tasks[scan['stage']]
        assert len(values) == scan['tasks']
        result['scan_stage'] = scan['stage']
        result['longest_scan_tasks'] = sorted(values, key=lambda t: t['seconds'], reverse=True)[:32]
        result['scan_workers'] = [{
            'worker': w, 'tasks': sum(t['worker'] == w for t in values),
            'over_5_seconds': sum(t['worker'] == w and t['seconds'] > 5 for t in values),
            'max_seconds': max(t['seconds'] for t in values if t['worker'] == w)} for w in range(8)]
        results.append(result)
    output = {'samples': results, 'limits': [
        'Only direct reclaim events longer than 50 ms are individually logged.',
        'Overlapping thread durations are summed work, not query wall time.',
        'Sampled kernel stacks and CPU tick deltas do not identify a lock owner.',
        'NUMA zone samples are observations, not each allocation decision.']}
    (directory / 'stall-analysis.json').write_text(json.dumps(output, indent=2) + '\n')
    for result in results:
        print(json.dumps({k: result[k] for k in ['iteration', 'sql_seconds', 'thread_samples',
              'sampler_mean_seconds', 'sampler_max_seconds', 'summed_slow_reclaim_thread_seconds', 'zones']}))
        print('Longest reclaim events', json.dumps(result['slow_reclaim_events'][:5]))


if __name__ == '__main__':
    main()
