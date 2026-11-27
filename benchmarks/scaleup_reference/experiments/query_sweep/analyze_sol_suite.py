#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Summarize complete validated applications without mixing samples across runs."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from variants import ORDER


def summarize(work, phase):
    state = read(work / 'critical-path' / phase / 'state.json')
    assert state['status'] == 'completed' and state['pool_verified']
    assert len(state['groups']) == 1
    group = state['groups'][0]
    assert group['status'] == 'completed' and group['driver_exit_code'] == 0
    app = Path(group['run']) / 'applications/suite'
    result = read(app / 'result.json')
    samples = result['samples']
    assert len(samples) == 44 and all(s['validation']['status'] == 'passed' for s in samples)
    assert [(s['query'], s['iteration']) for s in samples] == [(q, i) for q in ORDER for i in (1, 2)]
    queries = []
    for q in ORDER:
        values = [s for s in samples if s['query'] == q]
        assert len({s['sql_sha256'] for s in values}) == 1
        for s in values:
            cleanup = read(app / f'q{q}-iteration{s["iteration"]}.cleanup.json')
            assert len(cleanup) == 8
            assert abs(s['sql_plus_cleanup_seconds'] - s['sql_seconds'] - s['cleanup_seconds']) < 1e-6
        best = min(values, key=lambda s: s['sql_seconds'])
        queries.append({'query': q, 'variant': best['variant'], 'sql_sha256': best['sql_sha256'],
            'first_sql_seconds': values[0]['sql_seconds'], 'second_sql_seconds': values[1]['sql_seconds'],
            'best_sql_seconds': best['sql_seconds'], 'selected_iteration': best['iteration'],
            'paired_cleanup_seconds': best['cleanup_seconds'],
            'paired_total_seconds': best['sql_plus_cleanup_seconds'],
            'native_read_bytes': best['reader_metrics']['hybridScanNativeHostBytesRead'],
            'shuffle_write_bytes': sum(n['metrics'].get('dataSize', 0) for n in best['plan_metrics']
                                      if n['node'] == 'GpuColumnarExchange'),
            'runtime_hash_reuse': best.get('runtime_hash_reuse_proof')})
    return {'phase': phase, 'application_id': result['application_id'],
        'source_revision': state['source_revision'], 'run': group['run'],
        'queries': queries, 'validated_actions': len(samples),
        'best_of_two_sql_seconds': sum(q['best_sql_seconds'] for q in queries),
        'paired_cleanup_seconds': sum(q['paired_cleanup_seconds'] for q in queries),
        'paired_total_seconds': sum(q['paired_total_seconds'] for q in queries),
        'first_sql_seconds': sum(q['first_sql_seconds'] for q in queries),
        'second_sql_seconds': sum(q['second_sql_seconds'] for q in queries),
        'all_actions_sql_seconds': sum(s['sql_seconds'] for s in samples),
        'application_wall_seconds': (state['end_unix_ns'] - state['start_unix_ns']) / 1e9}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True, type=Path)
    p.add_argument('--phases', nargs='+', default=['S1', 'S0', 'S2'])
    a = p.parse_args()
    reports = [summarize(a.work, phase) for phase in a.phases]
    output = {'classification': 'Measured experimental SOL candidates, not a theoretical lower bound or stock main score',
        'method': 'Each application contains all 22 queries in fixed order, two independent actions per query. '
                  'Choose the faster SQL action within that application and retain its matching cleanup time. '
                  'No cross-application per-query selection. Startup and validation excluded from SQL totals.',
        'limitations': ['No controlled page-cache residency',
                       'Q9 uses diagnostic planning/execution configuration separation',
                       'Selected SQL includes explicit query rewrites and a dataset-checked Q21 integer narrowing'],
        'runs': reports}
    if len(reports) > 1:
        reference = {q['query']: q for q in reports[0]['queries']}
        for report in reports[1:]:
            for q in report['queries']:
                if q['query'] != 7:
                    assert q['sql_sha256'] == reference[q['query']]['sql_sha256']
    write(a.work / 'critical-path/sol-suite-scores.json', output)
    for report in reports:
        print(json.dumps({k: v for k, v in report.items() if k != 'queries'}))


if __name__ == '__main__':
    main()
