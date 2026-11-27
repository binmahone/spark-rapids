#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Summarize complete validated applications without mixing samples across runs."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from variants import ORDER
from finalize_sol_reuse import normalize


def summarize(work, phase):
    state = read(work / 'join-stats' / phase / 'state.json')
    assert state['status'] == 'completed' and len(state['native_threads']) == 8
    assert all(r['exit_code'] == 0 for r in state['observers'])
    group = {**state['result'], 'run': state['run']}
    assert group['status'] == 'completed' and group['driver_exit_code'] == 0
    app = Path(group['run']) / 'applications/suite'
    config = read(Path(group['run']) / 'config.json')
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
            if q == 9:
                proof = s['runtime_hash_reuse_proof']
                assert proof['physical_plan_unchanged'] and proof['restored_value'] == 'false'
                assert sum(n['metrics'].get('hashTableReuses', 0) for n in s['plan_metrics']) > 0
        plans = [normalize((app / f'q{q}-iteration{s["iteration"]}.executed.txt').read_text())
                 for s in values]
        assert plans[0] == plans[1], (phase, q, 'within-application plan changed')
        assert values[0]['reader_metrics'] == values[1]['reader_metrics'], (phase, q)
        best = min(values, key=lambda s: s['sql_seconds'])
        queries.append({'query': q, 'variant': best['variant'], 'sql_sha256': best['sql_sha256'],
            'plan_sha256': hashlib.sha256(plans[0].encode()).hexdigest(),
            'reader_metrics': best['reader_metrics'],
            'first_sql_seconds': values[0]['sql_seconds'], 'second_sql_seconds': values[1]['sql_seconds'],
            'best_sql_seconds': best['sql_seconds'], 'selected_iteration': best['iteration'],
            'paired_cleanup_seconds': best['cleanup_seconds'],
            'paired_total_seconds': best['sql_plus_cleanup_seconds'],
            'native_read_bytes': best['reader_metrics']['hybridScanNativeHostBytesRead'],
            'shuffle_write_bytes': sum(n['metrics'].get('shuffleBytesWritten', 0) for n in best['plan_metrics']
                                      if 'Exchange' in n['node']),
            'join_stats_computations': sum(n['metrics'].get('buildStatsComputations', 0)
                                           for n in best['plan_metrics']),
            'join_stats_reuses': sum(n['metrics'].get('buildStatsReuses', 0)
                                    for n in best['plan_metrics']),
            'runtime_hash_reuse': best.get('runtime_hash_reuse_proof')})
    return {'phase': phase, 'application_id': result['application_id'],
        'candidate': state['candidate'], 'engine_sha256': config['engine_sha256'],
        'source_revision': state['source_revision'], 'run': group['run'],
        'queries': queries, 'validated_actions': len(samples),
        'best_of_two_sql_seconds': sum(q['best_sql_seconds'] for q in queries),
        'paired_cleanup_seconds': sum(q['paired_cleanup_seconds'] for q in queries),
        'paired_total_seconds': sum(q['paired_total_seconds'] for q in queries),
        'first_sql_seconds': sum(q['first_sql_seconds'] for q in queries),
        'second_sql_seconds': sum(q['second_sql_seconds'] for q in queries),
        'all_actions_sql_seconds': sum(s['sql_seconds'] for s in samples),
        'experiment_wall_seconds': (state['end_unix_ns'] - state['start_unix_ns']) / 1e9}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True, type=Path)
    p.add_argument('--phases', nargs='+', default=['full-B1', 'full-A2'])
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
                for key in ('sql_sha256', 'plan_sha256', 'reader_metrics'):
                    assert q[key] == reference[q['query']][key], (report['phase'], q['query'], key)
    write(a.work / 'join-stats/sol-suite-scores.json', output)
    for report in reports:
        print(json.dumps({k: v for k, v in report.items() if k != 'queries'}))


if __name__ == '__main__':
    main()
