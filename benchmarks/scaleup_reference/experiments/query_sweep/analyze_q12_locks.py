#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare Q12 stage tails with separately measured reclaim, scan, and lock waits."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess

from analyze_q12_diagnosis import stage_windows, summarize


def parse_events(path):
    events, current = [], None
    if not path.exists():
        return events
    for line in path.read_text().splitlines():
        kind = line.split(' ', 1)[0]
        if kind in ('RECLAIM', 'EVICT', 'SCAN', 'LOCK_WAIT'):
            current = {'kind': kind, **dict(re.findall(r'(\w+)=(\S+)', line))}
            for key in current.keys() - {'kind', 'comm', 'lock'}:
                current[key] = int(current[key])
            current['stack'] = []
            assert current['begin_ns'] <= current['end_ns']
            assert current['end_ns'] - current['begin_ns'] == current['duration_ns']
            events.append(current)
        elif line.startswith('        ') and current is not None:
            current['stack'].append(line.strip())
        elif line.strip():
            current = None
    return events


def probe_audit(path, provider):
    if not path.exists():
        return {'provider': provider, 'attached': False}
    contents = path.read_text()
    scalars = {key: int(value) for key, value in
               re.findall(r'^@(\w+): (\d+)$', contents, re.M)}
    pairs = {kind: {'begins': scalars.get(kind + '_begins', 0),
                    'ends': scalars.get(kind + '_ends', 0)}
             for kind in ('reclaim', 'evict', 'scan', 'wait')}
    residual = [line for line in contents.splitlines()
                if re.match(r'^@(reclaim|evict|scan|wait)_begin\[', line)]
    return {'provider': provider, 'attached': 'READY monotonic_ns=' in contents,
            'function_counts': pairs,
            'reentry_counts': {kind: scalars.get(kind + '_reentry', 0)
                               for kind in ('evict', 'scan', 'wait')},
            'overlapping_scan_intervals': scalars.get('overlapping_scan_intervals', 0),
            'residual_intervals': residual,
            'output_loss_messages': [line for line in contents.splitlines()
                                     if re.search(r'\blost\b|\bdropped\b', line, re.I)],
            'boundary_limit': 'Attach/detach can straddle background reclaim; unmatched calls require inspection.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--phase', required=True)
    args = parser.parse_args()
    directory = args.work / 'q12-diagnosis' / args.phase
    state = json.loads((directory / 'state.json').read_text())
    assert state['status'] == 'completed'
    assert state['observer_mode'] in ('light', 'locks', 'batches') and state['cpu_event'] is None
    assert not state['gpm_enabled'] and state['native_pool_verification']['passed']
    assert all(g['status'] == 'completed' and g['driver_exit_code'] == 0 and
               all(s['validation']['status'] == 'passed' for s in g['samples']) for g in state['groups'])
    for record in [json.loads((directory / 'controller.json').read_text()), *state['profilers']]:
        path = Path('/proc') / str(record['pid']) / 'stat'
        assert not path.exists() or path.read_text().split(') ', 1)[1].split()[19] != str(record['start_time'])
    run = Path(state['groups'][0]['run'])
    data = summarize(args.work, run)
    stages = stage_windows(run)
    host = [json.loads(line) for line in (directory / 'host.jsonl').open()]
    vm = [json.loads(line) for line in (directory / 'vm.jsonl').open()]
    offset = int(statistics.median(row['time_unix_ns'] - row['monotonic_ns'] for row in host))
    events = parse_events(directory / 'reclaim.log')
    samples = []
    for sample in data['samples']:
        if sample['query'] != 12:
            continue
        sample['normalized_plan_sha256'] = hashlib.sha256(sample.pop('normalized_plan').encode()).hexdigest()
        window = sample['execution_window_ms']
        begin, end = window['start'] * 10**6, window['end'] * 10**6
        sample['stages'] = stages[f"q12-iteration{sample['iteration']}"]
        h = [row for row in host if begin <= row['time_unix_ns'] <= end]
        v = [row for row in vm if begin <= row['time_unix_ns'] <= end]
        assert len(h) >= 2 and len(v) >= 2
        sample['host_sampled_seconds'] = (h[-1]['time_unix_ns'] - h[0]['time_unix_ns']) / 1e9
        sample['host_max_gap_seconds'] = max((y['time_unix_ns'] - x['time_unix_ns']) / 1e9
                                             for x, y in zip(h, h[1:]))
        tick_hz = os.sysconf('SC_CLK_TCK')
        sample['host_cpu_seconds'] = {
            name: (h[-1]['cpu_ticks'][i] - h[0]['cpu_ticks'][i]) / tick_hz
            for i, name in enumerate(('user', 'nice', 'system', 'idle', 'iowait',
                                      'irq', 'softirq', 'steal', 'guest', 'guest_nice'))
            if i < len(h[0]['cpu_ticks'])}
        sample['host_max_sampling_seconds'] = max(r.get('sampling_seconds', 0) for r in h)
        sample['disk_read_bytes'] = {k: (h[-1]['diskstats'][k][2] - h[0]['diskstats'][k][2]) * 512
                                     for k in h[0]['diskstats']}
        sample['vm_deltas'] = {k: v[-1]['vmstat'][k] - v[0]['vmstat'][k] for k in v[0]['vmstat']}
        sample['memory_full_seconds'] = (int(re.search(r'^full.*total=(\d+)', h[-1]['pressure_memory'], re.M)[1]) -
                                         int(re.search(r'^full.*total=(\d+)', h[0]['pressure_memory'], re.M)[1])) / 1e6
        sample['memory_kib_range'] = {k: [min(r['memory_kib'][k] for r in h), max(r['memory_kib'][k] for r in h)]
                                      for k in h[0]['memory_kib']}
        selected = [{**event, 'overlap_ns': min(end, event['end_ns'] + offset) -
                     max(begin, event['begin_ns'] + offset)} for event in events
                    if event['begin_ns'] + offset < end and event['end_ns'] + offset > begin]
        sample['event_counts'] = dict(Counter(event['kind'] for event in selected))
        sample['longest_events'] = {kind: sorted([e for e in selected if e['kind'] == kind],
                                               key=lambda e: e['duration_ns'], reverse=True)[:30]
                                    for kind in ('RECLAIM', 'EVICT', 'SCAN', 'LOCK_WAIT')}
        samples.append(sample)
    output = {'phase': args.phase, 'observer_mode': state['observer_mode'], 'samples': samples,
              'probe_audit': probe_audit(directory / 'reclaim.log', state.get('lock_probe_provider')),
              'individual_event_thresholds_ns': state.get('probe_thresholds_ns',
                  {'RECLAIM': 50000000, 'EVICT': 10000000, 'SCAN': 1000000, 'LOCK_WAIT': 1000000}),
              'event_counts_whole_run': dict(Counter(e['kind'] for e in events)),
              'monotonic_to_unix_offset_ns': offset, 'runtime_source_revision': state['source_revision'],
              'analysis_source_revision': subprocess.check_output(
                  ['git', '-C', str(Path(__file__).parent), 'rev-parse', 'HEAD'], text=True).strip(),
              'limits': [
                  'Light mode retains one-second host/VM monitoring and native-pool verification; it is not observer-free.',
                  'Individual events are logged only above the recorded thresholds.',
                  'If output-loss messages are present, retained individual records do not establish complete event counts, distributions or global maxima.',
                  'Only known LRU-lock spin slow paths are instrumented at both entry and exit; other lock waits are not included.',
                  'Spin slow-path elapsed time measures that function, not the complete lock acquisition or holder duration.',
                  'Per-reclaim wait/scan subtotals include covered function calls below the individual logging threshold; evict time contains nested wait/scan time and must not be added to them.',
                  'scan_folios elapsed time is not proof of continuous lock ownership; kernel implementation and overlapping intervals require inspection.',
                  'scan_candidate_tid is a correlation candidate, not a proven lock owner.',
                  'Function entry/exit counts, reentry, residual intervals and dropped-output warnings must be audited before causal interpretation.',
                  'Summed concurrent thread intervals are not query wall time.',
                  'Host CPU seconds sum scheduler ticks across all logical CPUs during the sampled interval; they are not wall time.',
                  'In the inspected 6.8.0-1066-aws kernel, MGLRU pgscan accounts isolated pages while classic LRU accounts scanned pages; pgscan/pgsteal ratios are not equivalent across policies.',
                  'No cache flush is performed; separate runs have different cache histories.']}
    (directory / 'lock-analysis.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps([{'iteration': s['iteration'], 'sql_seconds': s['sql_seconds'],
                      'scan_span_seconds': max(s['stages'], key=lambda stage: stage['input_bytes'])['span_seconds'],
                      'longest_ns': {k: es[0]['duration_ns'] if es else None for k, es in s['longest_events'].items()}}
                     for s in samples], indent=2))


if __name__ == '__main__':
    main()
