#!/usr/bin/env python3
"""Extract timings and work counters without treating screening as qualification."""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write


def task_metrics(events):
    stages = defaultdict(set)
    groups = {}
    counters = defaultdict(Counter)
    finished = set()
    windows = {}
    for path in sorted(events.glob('*')):
        if not path.is_file() or path.name.startswith('.'):
            continue
        with path.open() as stream:
            for raw in stream:
                if not raw.endswith('\n') and path.name.endswith('.inprogress'):
                    break
                event = json.loads(raw)
                kind = event.get('Event', '')
                if kind == 'SparkListenerJobStart':
                    props = event.get('Properties') or {}
                    execution = props.get('spark.sql.execution.root.id')
                    if execution is not None:
                        for stage in event['Stage IDs']:
                            stages[stage].add(str(execution))
                elif kind.endswith('SparkListenerSQLExecutionStart'):
                    description = event.get('description', '')
                    if re.fullmatch(r'q\d+-iteration\d+', description):
                        groups[str(event['executionId'])] = description
                        windows[str(event['executionId'])] = {'start': event['time']}
                elif kind.endswith('SparkListenerSQLExecutionEnd'):
                    finished.add(str(event['executionId']))
                    if str(event['executionId']) in windows:
                        windows[str(event['executionId'])]['end'] = event['time']
                elif kind == 'SparkListenerTaskEnd':
                    metric = event.get('Task Metrics') or {}
                    target = counters[event['Stage ID']]
                    for key in ['Memory Bytes Spilled', 'Disk Bytes Spilled', 'Executor Run Time',
                                'Executor CPU Time', 'JVM GC Time']:
                        target[key] += metric.get(key, 0)
                    target['shuffle_write_bytes'] += (metric.get('Shuffle Write Metrics') or {}).get(
                        'Shuffle Bytes Written', 0)
                    target['input_bytes'] += (metric.get('Input Metrics') or {}).get('Bytes Read', 0)
                    labels = {'hybrid scan bytes read to host with KvikIO': 'native_host_read_bytes',
                              'hybrid scan bytes read to device with GDS, including alignment padding':
                                  'native_device_read_bytes',
                              'hybrid scan splits read': 'native_scan_splits'}
                    for accumulator in (event.get('Task Info') or {}).get('Accumulables', []):
                        key = labels.get(accumulator.get('Name'))
                        if key and 'Update' in accumulator:
                            target[key] += int(accumulator['Update'])
                    target['task_attempts'] += 1
    result = defaultdict(Counter)
    for stage, metric in counters.items():
        tokens = {groups[execution] for execution in stages[stage] if execution in groups}
        if len(tokens) > 1:
            raise AssertionError('Stage reuse spans independent query actions; attribute explicitly')
        for token in tokens:
            result[token].update(metric)
    return result, {groups[key] for key in finished if key in groups}, {
        groups[key]: window for key, window in windows.items()}


def summarize(work, run):
    result_path = run / 'applications/suite/result.json'
    if not result_path.exists():
        return None
    data = read(result_path)
    if data['status'] not in ('completed', 'failed'):
        if not (run / 'applications/suite/driver-service.json.stopped.json').exists():
            return None
        data['status'] = 'interrupted; validated prefix retained'
    profile = read(run / 'applications/suite/profile.json')
    location = urlparse(profile['spark.eventLog.dir'])
    if location.scheme != 'file':
        raise ValueError('Expected host-local event logs')
    task, finished, windows = task_metrics(Path(location.path))
    rows = []
    for sample in data['samples']:
        token = f"q{sample['query']}-iteration{sample['iteration']}"
        if token not in finished:
            raise AssertionError('Validated sample lacks a completed SQL execution: ' + token)
        row = {key: sample[key] for key in ['query', 'iteration', 'sql_seconds',
            'cleanup_seconds', 'sql_plus_cleanup_seconds', 'validation', 'reader_metrics']}
        row['variant'] = sample.get('variant', 'baseline')
        row['task_metrics'] = dict(task[token])
        row['execution_window_ms'] = windows[token]
        if not row['task_metrics'].get('task_attempts'):
            raise AssertionError('Missing SQL-root task accounting: ' + str(run) + '/' + token)
        row['plan_shuffle_write_bytes'] = sum(op['metrics'].get('shuffleBytesWritten', 0)
            for op in sample['plan_metrics'] if 'Exchange' in op['node'])
        plan = (run / 'applications/suite' / (token + '.executed.txt')).read_text()
        row['join_plan'] = [line.strip() for line in plan.splitlines() if 'Join' in line]
        row['scan_plan'] = [line.strip() for line in plan.splitlines() if 'Scan' in line]
        normalized = re.sub(r'#\d+[L]?', '#', plan)
        normalized = re.sub(r'\[(?:plan_id|loreId)=[^\]]*\]', '', normalized)
        normalized = re.sub(r'@[0-9a-f]+', '@', normalized)
        row['normalized_plan'] = normalized
        rows.append(row)
    return {'run': str(run.relative_to(work)), 'status': data['status'],
            'error': data.get('error'), 'samples': rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--baseline-run', default='baseline',
                        help='Relative run directory used as the explicit comparison reference')
    args = parser.parse_args()
    work = Path(args.work).resolve()
    runs = [work / 'baseline'] + sorted((work / 'screen').glob('*-*'))
    runs = [run for run in runs if run.is_dir()]
    if args.workers < 1:
        raise ValueError('Worker count must be positive')
    if args.workers == 1:
        results = [summarize(work, run) for run in runs]
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            results = list(pool.map(summarize, [work] * len(runs), runs))
    groups = [result for result in results if result]
    reference = next(g for g in groups if g['run'] == args.baseline_run)
    baseline = {s['query']: s for s in reference['samples'] if s['iteration'] == 2}
    comparisons = []
    for group in groups:
        if group['run'] == args.baseline_run:
            continue
        for sample in group['samples']:
            if sample['iteration'] != 2:
                continue
            original = baseline[sample['query']]
            item = {'query': sample['query'], 'variant': sample['variant'], 'run': group['run'],
                'baseline_sql_seconds': original['sql_seconds'],
                'candidate_sql_seconds': sample['sql_seconds'],
                'baseline_total_seconds': original['sql_plus_cleanup_seconds'],
                'candidate_total_seconds': sample['sql_plus_cleanup_seconds'],
                'plan_changed': original['normalized_plan'] != sample['normalized_plan'],
                'saved_sql_seconds': original['sql_seconds'] - sample['sql_seconds'],
                'relative_sql_change': sample['sql_seconds'] / original['sql_seconds'] - 1,
                'baseline_task_metrics': original['task_metrics'],
                'candidate_task_metrics': sample['task_metrics'],
                'status': 'Unqualified screen; cache/order and repeat noise remain'}
            comparisons.append(item)
    report = {'status': 'diagnostic', 'score_eligible': False, 'groups': groups,
        'baseline_run': args.baseline_run,
        'comparisons': comparisons,
        'limits': ['Second consecutive action; no OS cache flush',
                   'Full-result validation uses retained GPU references',
                   'Task memory-spill counters do not measure spill latency',
                   'Candidate selection requires isolated repetition before a combined suite']}
    write(work / 'evidence/sweep-analysis.json', report)
    for item in comparisons:
        print(f"Q{item['query']:02} {item['variant']:36} "
              f"{item['baseline_sql_seconds']:8.3f} -> {item['candidate_sql_seconds']:8.3f} "
              f"({item['relative_sql_change']:+.1%})")


if __name__ == '__main__':
    main()
