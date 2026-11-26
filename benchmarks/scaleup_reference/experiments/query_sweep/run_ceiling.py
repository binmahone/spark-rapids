"""Measure SQL and execution-granularity improvements over the retained experimental suite."""
import argparse
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from ceiling_variants import cases as new_sql_cases
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original
import services


def prior_selection():
    manifest = read(HERE / 'selected-cases.json')
    available = {(c.query, c.name): c for c in catalog(include_followups=True)}
    selected = {c['query']: available[(c['query'], c['variant'])] for c in manifest['candidates']}
    result = {q: selected.get(q, Variant(q, 'baseline', original(q), 'Prior selected SQL')) for q in ORDER}
    for q, case in result.items():
        assert hashlib.sha256(case.sql.encode()).hexdigest() == manifest['sql_sha256_by_query'][str(q)]
    return result


def totals(samples, query):
    return [s['sql_plus_cleanup_seconds'] for s in samples if s['query'] == query and s['iteration'] == 2]


def decision(query, a, b):
    base, candidate = totals(a, query), totals(b, query)
    assert len(base) == len(candidate) == 2
    # Every observed candidate must beat every control; avoid benefiting from slow control outliers.
    conservative = 1 - max(candidate) / min(base)
    return {'query': query, 'baseline_totals': base, 'candidate_totals': candidate,
            'conservative_fraction_saved_estimate': conservative,
            'accepted': conservative >= .03}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--phase', choices=['sql', 'tune'], required=True)
    args = parser.parse_args()
    work = Path(args.work).resolve()
    output = work / 'ceiling' / (args.phase + '.json')
    if output.exists():
        raise RuntimeError('Phase evidence exists; use a separately named work directory for a rerun')
    report = {'status': 'running', 'classification': 'Experimental bounded performance search',
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'],
                                                         text=True).strip(),
              'cache_policy': 'No cache flush; two complete actions per query per fresh application',
              'groups': [], 'decisions': [], 'instance_policy': 'Explicit user release only'}
    write(output, report)
    write(work / 'ceiling/controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                           'start_time': services.start_time(os.getpid())})
    def run(label, values, conf=None):
        location, result = launch_group(work, 'ceiling-' + label, 0, values, conf or {})
        report['groups'].append({'label': label, 'run': str(location),
                                'status': result['status'], 'samples': result['samples'],
                                'error': result.get('error'), 'active_query': result.get('active_query')})
        write(output, report)
        if result['status'] != 'completed' or result['driver_exit_code']:
            raise RuntimeError('Inspect failed group: ' + str(location))
        assert len(result['samples']) == 2 * len(values)
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        return result['samples']
    selected = prior_selection()
    try:
        if args.phase == 'sql':
            run('prior-suite', [selected[q] for q in ORDER])
            candidates = new_sql_cases()
            a, b = [], []
            for index, arm in enumerate('ABBA'):
                values = [selected[c.query] for c in candidates] if arm == 'A' else candidates
                samples = run('sql-' + arm + str(index), values)
                (a if arm == 'A' else b).extend(samples)
            for case in candidates:
                d = decision(case.query, a, b)
                d.update(variant=case.name, baseline_variant=selected[case.query].name)
                report['decisions'].append(d)
                if d['accepted']:
                    selected[case.query] = case
        else:
            prior = read(work / 'ceiling/sql.json')
            assert prior['status'] == 'completed'
            available = {(c.query, c.name): c for c in catalog(include_followups=True)}
            for q, spec in prior['selected'].items():
                if spec['name'] != 'baseline':
                    selected[int(q)] = available[(int(q), spec['name'])]
            baseline_samples = run('tune-control', [selected[q] for q in ORDER])
            settings = [
                ('shuffle128', {'spark.sql.shuffle.partitions': '128'}),
                ('shuffle64', {'spark.sql.shuffle.partitions': '64'}),
                ('operator512', {'spark.rapids.sql.batchSizeBytes': '536870912'})]
            winners = {}
            for name, overrides in settings:
                samples = run('screen-' + name, [selected[q] for q in ORDER], overrides)
                for q in ORDER:
                    candidate = totals(samples, q)[0]
                    baseline = totals(baseline_samples, q)[0]
                    if candidate < baseline * .95 and candidate < winners.get(q, {}).get('time', float('inf')):
                        winners[q] = {'time': candidate, 'name': name, 'overrides': overrides}
            report['screen_winners'] = winners
            write(output, report)
            if winners:
                queries = [q for q in ORDER if q in winners]
                candidates = [replace(selected[q], overrides={**selected[q].overrides,
                              **winners[q]['overrides']}) for q in queries]
                a, b = [], []
                for index, arm in enumerate('ABBA'):
                    values = [selected[q] for q in queries] if arm == 'A' else candidates
                    samples = run('tune-' + arm + str(index), values)
                    (a if arm == 'A' else b).extend(samples)
                for case in candidates:
                    d = decision(case.query, a, b)
                    d.update(variant=case.name, overrides=case.overrides)
                    report['decisions'].append(d)
                    if d['accepted']:
                        selected[case.query] = case
            combined = run('final-suite', [selected[q] for q in ORDER])
            report['combined_best_sql_sum_seconds'] = sum(min(s['sql_seconds'] for s in combined
                if s['query'] == q) for q in ORDER)
            report['combined_best_total_sum_seconds'] = sum(min(s['sql_plus_cleanup_seconds']
                for s in combined if s['query'] == q) for q in ORDER)
        report.update(status='completed', selected={str(q): vars(c) for q, c in selected.items()})
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        write(output, report)


if __name__ == '__main__':
    main()
