#!/usr/bin/env python3
"""Confirm selected probes in ABBA order, then execute their combined 22-query workload."""
import argparse
from collections import defaultdict
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original


def complete(result, cases):
    if result['status'] != 'completed' or result['driver_exit_code']:
        raise RuntimeError('Confirmation application failed: ' + str(result.get('error')))
    assert len(result['samples']) == len(cases) * 2
    assert all(s['validation']['status'] == 'passed' for s in result['samples'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--selection', required=True,
                        help='JSON containing candidates [{query, variant}] with screening evidence')
    args = parser.parse_args()
    work = Path(args.work).resolve()
    selection = read(args.selection)
    available = {(c.query, c.name): c for c in catalog()}
    cases = [available[(item['query'], item['variant'])] for item in selection['candidates']]
    assert cases and len({c.query for c in cases}) == len(cases)
    if any(c.name == 'batch_512m' for c in cases):
        raise ValueError('Batch targets require a separate startup-configuration matrix')
    output = work / 'confirmation/state.json'
    if output.exists():
        raise RuntimeError('Confirmation state exists; inspect before resuming')
    report = {'status': 'running', 'score_eligible': False, 'selection': selection,
              'arms': [], 'cache_policy': 'No flush; two actions per query per fresh application',
              'instance_policy': 'Retain until explicit user release'}
    write(output, report)
    try:
        for index, arm in enumerate(['baseline', 'candidate', 'candidate', 'baseline']):
            current = cases if arm == 'candidate' else [
                Variant(c.query, 'baseline', original(c.query), 'Unchanged SQL control') for c in cases]
            run, result = launch_group(work, 'confirm-' + arm, index, current, {})
            complete(result, current)
            report['arms'].append({'arm': arm, 'run': str(run), 'samples': result['samples']})
            write(output, report)
        warm = defaultdict(lambda: defaultdict(list))
        for arm in report['arms']:
            for sample in arm['samples']:
                if sample['iteration'] == 2:
                    warm[sample['query']][arm['arm']].append(sample)
        accepted = []
        report['decisions'] = []
        for case in cases:
            data = warm[case.query]
            base = [s['sql_plus_cleanup_seconds'] for s in data['baseline']]
            candidate = [s['sql_plus_cleanup_seconds'] for s in data['candidate']]
            improvement = 1 - sum(candidate) / sum(base)
            # Require nonoverlapping repeated totals and a useful fractional improvement.
            keep = max(candidate) < min(base) and improvement >= .03
            report['decisions'].append({'query': case.query, 'variant': case.name,
                'baseline_totals': base, 'candidate_totals': candidate,
                'relative_improvement': improvement, 'selected': keep,
                'scope': 'Observed same-host ABBA; no guarantee for other data or hosts'})
            if keep:
                accepted.append(case)
        write(output, report)
        selected = {case.query: case for case in accepted}
        combined = [selected.get(q, Variant(q, 'baseline', original(q), 'Unchanged SQL')) for q in ORDER]
        run, result = launch_group(work, 'combined', 0, combined, {})
        complete(result, combined)
        report.update(status='completed', combined_run=str(run), combined_samples=result['samples'],
            combined_best_sql_sum_seconds=sum(min(s['sql_seconds'] for s in result['samples']
                                                  if s['query'] == q) for q in ORDER),
            combined_best_total_sum_seconds=sum(min(s['sql_plus_cleanup_seconds']
                for s in result['samples'] if s['query'] == q) for q in ORDER))
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        write(output, report)


if __name__ == '__main__':
    main()
