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

"""Measure SQL and execution-granularity improvements over the retained experimental suite."""
import argparse
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from ceiling_variants import cases as new_sql_cases, refinements, q11_total_reuse
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
    parser.add_argument('--phase', choices=['sql', 'refine', 'tune', 'post'], required=True)
    parser.add_argument('--resume', action='store_true', help='Resume a failed phase without discarding its evidence')
    args = parser.parse_args()
    work = Path(args.work).resolve()
    output = work / 'ceiling' / (args.phase + '.json')
    if output.exists() and not args.resume:
        raise RuntimeError('Phase evidence exists; use a separately named work directory for a rerun')
    report = {'status': 'running', 'classification': 'Experimental bounded performance search',
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'],
                                                         text=True).strip(),
              'cache_policy': 'No cache flush; two complete actions per query per fresh application',
              'groups': [], 'decisions': [], 'instance_policy': 'Explicit user release only'}
    if args.resume:
        previous = read(output)
        if previous['status'] != 'failed':
            raise RuntimeError('Only a failed, stopped phase can be resumed')
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        write(output.with_name(output.stem + '-failed-' + stamp + '.json'), previous)
        report['groups'] = previous['groups']
    write(output, report)
    write(work / 'ceiling/controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                           'start_time': services.start_time(os.getpid())})
    def run(label, values, conf=None):
        conf = conf or {}
        expected_conf = read(HERE.parents[1] / 'configs/aws48x.json')['spark_conf']
        expected_conf.update(conf)
        expected_variants = {str(c.query): c.name for c in values}
        expected_overrides = {str(c.query): c.overrides for c in values}
        for group in reversed(report['groups']):
            if group['label'] != label or group['status'] != 'completed':
                continue
            config = read(Path(group['run']) / 'config.json')
            fingerprints = {c.query: hashlib.sha256(c.sql.encode()).hexdigest() for c in values}
            if (config['spark_conf'] == expected_conf and config['case_variants'] == expected_variants
                    and config['case_overrides'] == expected_overrides
                    and len(group['samples']) == 2 * len(values)
                    and all(s['sql_sha256'] == fingerprints[s['query']]
                            and s['validation']['status'] == 'passed' for s in group['samples'])):
                return group['samples']
        prefix = 'ceiling-' + label
        indices = [int(p.name.rsplit('-', 1)[1]) for p in (work / 'screen').glob(prefix + '-*')
                   if p.name.rsplit('-', 1)[1].isdigit()]
        index = max(indices, default=-1) + 1
        location, result = launch_group(work, prefix, index, values, conf)
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
            if args.phase == 'post':
                tuned = read(work / 'ceiling/tune.json')
                assert tuned['status'] == 'completed'
                for q, spec in tuned['selected'].items():
                    case = (Variant(int(q), 'baseline', original(int(q)), 'Unchanged reference SQL')
                            if spec['name'] == 'baseline' else available[(int(q), spec['name'])])
                    selected[int(q)] = replace(case, overrides=spec['overrides'])
                candidates = [replace(q11_total_reuse(), overrides=selected[11].overrides),
                    replace(selected[21], overrides={**selected[21].overrides,
                        'spark.sql.shuffle.partitions': '128',
                        'spark.rapids.sql.batchSizeBytes': '4294967296'})]
                a, b = [], []
                for index, arm in enumerate('ABBA'):
                    values = [selected[c.query] for c in candidates] if arm == 'A' else candidates
                    samples = run('post-' + arm + str(index), values)
                    (a if arm == 'A' else b).extend(samples)
                for candidate in candidates:
                    d = decision(candidate.query, a, b)
                    d.update(variant=candidate.name, baseline_variant=selected[candidate.query].name,
                             overrides=candidate.overrides)
                    if candidate.query == 21:
                        d['probe_scope'] = ('A bounded joint setting: 128 shuffle partitions and a 4 GiB '
                            'operator batch, following observed internal repartitioning at 128/2 GiB')
                        d['aggregate_tasks_repartitioned'] = {
                            arm: [sum(n['metrics'].get('numTasksRepartitioned', 0)
                                for n in s['plan_metrics'] if n['node'] == 'GpuHashAggregate')
                                for s in samples if s['query'] == 21 and s['iteration'] == 2]
                            for arm, samples in [('control', a), ('candidate', b)]}
                    report['decisions'].append(d)
                    if d['accepted']:
                        selected[candidate.query] = candidate
                if any(d['accepted'] for d in report['decisions']):
                    combined = run('post-final-suite', [selected[q] for q in ORDER])
                    report['combined_best_sql_sum_seconds'] = sum(min(s['sql_seconds'] for s in combined
                        if s['query'] == q) for q in ORDER)
                    report['combined_best_total_sum_seconds'] = sum(min(s['sql_plus_cleanup_seconds']
                        for s in combined if s['query'] == q) for q in ORDER)
                else:
                    report['combined_best_sql_sum_seconds'] = tuned['combined_best_sql_sum_seconds']
                    report['combined_best_total_sum_seconds'] = tuned['combined_best_total_sum_seconds']
                    report['combined_source_phase'] = 'tune'
                report.update(status='completed', selected={str(q): vars(c) for q, c in selected.items()})
                return
            for q, spec in prior['selected'].items():
                if spec['name'] != 'baseline':
                    selected[int(q)] = available[(int(q), spec['name'])]
            if args.phase == 'refine':
                candidates = refinements()
                a, b = [], []
                for index, arm in enumerate('ABBA'):
                    values = [selected[c.query] for c in candidates] if arm == 'A' else candidates
                    samples = run('refine-' + arm + str(index), values)
                    (a if arm == 'A' else b).extend(samples)
                for case in candidates:
                    d = decision(case.query, a, b)
                    d.update(variant=case.name, baseline_variant=selected[case.query].name)
                    report['decisions'].append(d)
                    if d['accepted']:
                        selected[case.query] = case
                report.update(status='completed', selected={str(q): vars(c) for q, c in selected.items()})
                return
            refined = read(work / 'ceiling/refine.json')
            assert refined['status'] == 'completed'
            for q, spec in refined['selected'].items():
                if spec['name'] != 'baseline':
                    selected[int(q)] = available[(int(q), spec['name'])]
            baseline_samples = run('tune-control', [selected[q] for q in ORDER])
            reader_probe = run('reader-cap-reachability', [selected[q] for q in (1, 9)],
                {'spark.rapids.sql.reader.batchSizeBytes': '536870912'})
            def scan_signature(samples, query):
                sample = next(s for s in samples if s['query'] == query and s['iteration'] == 2)
                return sorted((n['node'], *(n['metrics'].get(key, 0) for key in
                    ('numOutputBatches', 'numOutputRows', 'hybridScanSplits',
                     'hybridScanNativeHostBytesRead'))) for n in sample['plan_metrics']
                    if n['node'].startswith('GpuScan'))
            report['reader_cap_probe'] = [{'query': q,
                'control_scan_signature': scan_signature(baseline_samples, q),
                'probe_scan_signature': scan_signature(reader_probe, q),
                'same_scan_batches': scan_signature(baseline_samples, q) == scan_signature(reader_probe, q)}
                for q in (1, 9)]
            report['reader_cap_scope'] = (
                'Hybrid MULTITHREADED decodes a split eagerly; the reader byte cap is only used '
                'by the disabled file-combining branch. This reachability control is not a tuning candidate.')
            write(output, report)
            assert all(p['same_scan_batches'] for p in report['reader_cap_probe']), (
                'Reader-cap scan counters changed; investigate before excluding this tuning direction')
            settings = [
                ('shuffle128', {'spark.sql.shuffle.partitions': '128'}),
                ('shuffle64', {'spark.sql.shuffle.partitions': '64'}),
                ('operator512', {'spark.rapids.sql.batchSizeBytes': '536870912'}),
                ('scan4g', {'spark.sql.files.maxPartitionBytes': '4g'})]
            winners = {}
            for name, overrides in settings:
                samples = run('screen-' + name, [selected[q] for q in ORDER], overrides)
                for q in ORDER:
                    # The established Q13 scan override takes precedence over a global scan setting.
                    if q == 13 and name == 'scan4g':
                        continue
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
