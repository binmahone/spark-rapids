"""Analyze closed critical-query trials and select only measured candidates for confirmation."""
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


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True)
    p.add_argument('--phase', choices=['screen', 'confirm', 'suite'], required=True)
    p.add_argument('--select', action='store_true')
    a = p.parse_args()
    work = Path(a.work)
    directory = work / 'critical'
    state = read(directory / (a.phase + '-state.json'))
    assert state['status'] == 'completed' and state['monitor_exit_codes'] == [0, 0]
    assert (work / 'evidence' / ('critical-' + a.phase + '.exit')).read_text().strip() == '0'
    host = [json.loads(line) for line in (directory / (a.phase + '-host.jsonl')).open()]
    times = [x['time_unix_ns'] for x in host]
    groups = []
    for group in state['groups']:
        data = summarize(work, Path(group['run']))
        original = read(Path(group['run']) / 'applications/suite/result.json')
        raw = {(s['query'], s['iteration']): s for s in original['samples']}
        for s in data['samples']:
            s['normalized_plan_sha256'] = hashlib.sha256(s.pop('normalized_plan').encode()).hexdigest()
            scans = [n for n in raw[(s['query'], s['iteration'])]['plan_metrics'] if n['node'].startswith('GpuScan')]
            s['scan_metrics'] = scans
            s['summed_scan_buffer_seconds'] = sum(n['metrics'].get('bufferTime', 0) for n in scans) / 1e9
            s['summed_scan_gpu_decode_seconds'] = sum(n['metrics'].get('gpuDecodeTime', 0) for n in scans) / 1e9
            window = s['execution_window_ms']
            begin, end = window['start'] * 1_000_000, window['end'] * 1_000_000
            rows = host[bisect_left(times, begin):bisect_right(times, end)]
            assert len(rows) >= 2
            before, after = ({v['pid']: v for v in x['executors']} for x in (rows[0], rows[-1]))
            assert len(before.keys() & after.keys()) == 8
            s['physical_read_bytes'] = sum(after[pid]['io']['read_bytes'] - before[pid]['io']['read_bytes'] for pid in before.keys() & after.keys())
            first, last = [int(re.search(r'^full.*total=(\d+)', x['pressure_memory'], re.M)[1]) for x in (rows[0], rows[-1])]
            s['memory_full_seconds'] = (last - first) / 1e6
            s['sampling_boundary_gap_seconds'] = ((rows[0]['time_unix_ns'] - begin) + (end - rows[-1]['time_unix_ns'])) / 1e9
        groups.append({'label': group['label'], **data})
    report = {'phase': a.phase, 'groups': groups, 'decisions': [],
              'limits': ['Operator durations are task sums, not wall time',
                         'No cache flush; physical reads and host pressure must be examined',
                         'Full-scale validation uses retained GPU references plus separate CPU adversarial fixtures']}
    if a.phase == 'screen':
        controls = groups[0]['samples']
        candidates = [s for g in groups[1:] for s in g['samples']]
        for q in (9, 8, 21):
            before = next(s for s in controls if s['query'] == q and s['iteration'] == 2)
            after = next((s for s in candidates if s['query'] == q and s['iteration'] == 2), None)
            report['decisions'].append({'query': q,
                'control_second_total': before['sql_plus_cleanup_seconds'],
                'candidate_second_total': after['sql_plus_cleanup_seconds'] if after else None,
                'eligible_for_confirmation': bool(after and after['sql_plus_cleanup_seconds'] <= .97 * before['sql_plus_cleanup_seconds'])})
    elif a.phase == 'confirm':
        controls = [s for g in groups if g['label'].startswith('A') for s in g['samples']]
        candidates = [s for g in groups if g['label'].startswith('B') for s in g['samples']]
        for q in sorted({s['query'] for s in controls}):
            before = [s['sql_plus_cleanup_seconds'] for s in controls if s['query'] == q and s['iteration'] == 2]
            after = [s['sql_plus_cleanup_seconds'] for s in candidates if s['query'] == q and s['iteration'] == 2]
            assert len(before) == len(after) == 2
            report['decisions'].append({'query': q, 'control_second_totals': before,
                'candidate_second_totals': after, 'accepted': max(after) <= .97 * min(before)})
    else:
        samples = groups[0]['samples']
        assert len(samples) == 44 and all(s['validation']['status'] == 'passed' for s in samples)
        report['best_sql_sum_seconds'] = sum(min(s['sql_seconds'] for s in samples if s['query'] == q) for q in range(1, 23))
        report['best_total_sum_seconds'] = sum(min(s['sql_plus_cleanup_seconds'] for s in samples if s['query'] == q) for q in range(1, 23))
    write(directory / (a.phase + '-analysis.json'), report)
    if a.select:
        assert a.phase in ('screen', 'confirm')
        key = 'eligible_for_confirmation' if a.phase == 'screen' else 'accepted'
        selected = [row['query'] for row in report['decisions'] if row[key]]
        target = directory / 'selection.json'
        if target.exists():
            assert a.phase == 'confirm'
            write(directory / 'screen-selection.json', read(target))
        write(target, {'queries': selected, 'confirmed': a.phase == 'confirm',
            'baseline_selection_sha256': state['baseline_selection_sha256'],
            'decision_source': a.phase + '-analysis.json'})
    print(json.dumps({k: v for k, v in report.items() if k != 'groups'}, indent=2))


if __name__ == '__main__':
    main()
