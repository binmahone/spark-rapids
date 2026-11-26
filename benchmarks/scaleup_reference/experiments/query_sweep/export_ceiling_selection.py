#!/usr/bin/env python3
"""Export a completed, measured ceiling selection without requiring the search to be rerun."""
import argparse
import hashlib
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
from variants import ORDER, Variant, catalog, original


def score(samples, field):
    return sum(min(s[field] for s in samples if s['query'] == q) for q in ORDER)


def compact_samples(group):
    return {'label': group['label'], 'run': group['run'], 'status': group['status'],
        'samples': [{key: s[key] for key in ('query', 'variant', 'iteration', 'sql_seconds',
            'cleanup_seconds', 'sql_plus_cleanup_seconds', 'application_id', 'sql_sha256',
            'validation')} for s in group['samples']]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    work, output = Path(args.work), Path(args.output)
    if output.exists():
        raise RuntimeError('Export target exists; use a new snapshot directory')
    phases = {phase: read(work / 'ceiling' / (phase + '.json')) for phase in ('sql', 'refine', 'tune')}
    if (work / 'ceiling/post.json').exists():
        phases['post'] = read(work / 'ceiling/post.json')
    assert all(value['status'] == 'completed' for value in phases.values())
    tuned = phases['tune']
    final = next(g for g in reversed(tuned['groups']) if g['label'] == 'final-suite' and g['status'] == 'completed')
    chosen = phases.get('post', tuned)
    if 'post' in phases and (any(d['accepted'] for d in chosen['decisions']) or
                            chosen.get('invalidated_unfired_shuffle_settings')):
        final = next(g for g in reversed(chosen['groups']) if g['label'] == 'post-final-suite' and g['status'] == 'completed')
    baseline = next(g for g in phases['sql']['groups'] if g['label'] == 'prior-suite' and g['status'] == 'completed')
    control = next(g for g in tuned['groups'] if g['label'] == 'tune-control' and g['status'] == 'completed')
    assert len(final['samples']) == len(baseline['samples']) == len(control['samples']) == 44
    assert all(s['validation']['status'] == 'passed' for g in (final, baseline, control) for s in g['samples'])
    config = read(Path(final['run']) / 'config.json')
    available = {(c.query, c.name): c for c in catalog(include_followups=True)}
    selected = {}
    for q in ORDER:
        spec = chosen['selected'][str(q)]
        case = (Variant(q, 'baseline', original(q), 'Unchanged reference SQL') if spec['name'] == 'baseline'
                else available[(q, spec['name'])])
        digest = hashlib.sha256(case.sql.encode()).hexdigest()
        assert all(s['sql_sha256'] == digest for s in final['samples'] if s['query'] == q)
        assert config['case_variants'][str(q)] == spec['name']
        assert config['case_overrides'][str(q)] == spec['overrides']
        selected[q] = (case, digest, spec['overrides'])
    selection = read(HERE / 'selected-cases.json')
    selection.update(
        classification='Validated bounded experimental selection; not an absolute hardware bound or Daily Trend main score',
        candidates=[{'query': q, 'variant': case.name} for q, (case, _, _) in selected.items() if case.name != 'baseline'],
        runtime_overrides_by_query={str(q): overrides for q, (_, _, overrides) in selected.items() if overrides},
        sql_sha256_by_query={str(q): digest for q, (_, digest, _) in selected.items()},
        common_configuration_sha256=sha(HERE.parents[1] / 'configs/aws48x.json'),
        tested_source_revisions={phase: data['source_revision'] for phase, data in phases.items()},
        result_group=final['run'], result_application_ids=sorted({s['application_id'] for s in final['samples']}))
    evidence = {
        'classification': selection['classification'],
        'score_definition': 'Sum of per-query minima across the two complete actions of the actual full-suite group',
        'selection_definition': 'Second-action SQL plus cleanup, confirmed in ABBA; worst candidate must beat best control by at least three percent',
        'baseline_best_sql_sum_seconds': score(baseline['samples'], 'sql_seconds'),
        'baseline_best_total_sum_seconds': score(baseline['samples'], 'sql_plus_cleanup_seconds'),
        'sql_only_best_sql_sum_seconds': score(control['samples'], 'sql_seconds'),
        'sql_only_best_total_sum_seconds': score(control['samples'], 'sql_plus_cleanup_seconds'),
        'final_best_sql_sum_seconds': score(final['samples'], 'sql_seconds'),
        'final_best_total_sum_seconds': score(final['samples'], 'sql_plus_cleanup_seconds'),
        'decisions': {phase: value['decisions'] for phase, value in phases.items()},
        'excluded_probes': {phase: value['excluded_probes'] for phase, value in phases.items()
                            if 'excluded_probes' in value},
        'reader_cap_probe': tuned['reader_cap_probe'],
        'shuffle_runtime_audit': chosen.get('shuffle_runtime_audit', []),
        'invalidated_unfired_shuffle_settings': chosen.get('invalidated_unfired_shuffle_settings', []),
        'groups': [compact_samples(group) for phase in phases.values() for group in phase['groups']],
        'limits': ['No physical disk cache flush; each fresh application runs two actions per query',
                   'Full-scale outputs are checked against retained GPU references; CPU counterexamples cover the SQL rewrites',
                   'The combined score is measured, not a sum of independent candidate bests',
                   'SQL rewrites and per-query settings are experimental selections, not a general optimizer implementation',
                   'No claim that native kernels, reader architecture or every possible configuration has reached its physical limit']}
    audit_path = work / 'evidence/resource-log-audit.json'
    if audit_path.exists():
        audit = read(audit_path)
        evidence['completed_log_resource_observations'] = [
            {key: group[key] for key in ('run', 'status', 'files_checked', 'counts')}
            for group in audit['groups']]
        evidence['limits'].append('Shutdown resource errors, when recorded in the log audit, remain unresolved')
    assert evidence['final_best_sql_sum_seconds'] == chosen['combined_best_sql_sum_seconds']
    assert evidence['final_best_total_sum_seconds'] == chosen['combined_best_total_sum_seconds']
    write(output / 'selected-cases.json', selection)
    write(output / 'measurements.json', evidence)
    print('Exported the measured selection and compact evidence to', output)


if __name__ == '__main__':
    main()
