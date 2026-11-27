#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Attribute CPU samples only within the longest measured Q12 reclaim intervals."""
import argparse
import json
from pathlib import Path
import subprocess
from analyze_q12_diagnosis import perf_summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory
    analysis = json.loads((directory / 'analysis.json').read_text())
    stalls = json.loads((directory / 'stall-analysis.json').read_text())
    state = json.loads((directory / 'state.json').read_text())
    assert state['status'] in ('completed', 'failed') and state['gpm_restored']
    for record in state['profilers']:
        proc = Path('/proc') / str(record['pid']) / 'stat'
        assert not proc.exists() or proc.read_text().split(') ', 1)[1].split()[19] != str(record['start_time'])
    offset = analysis['perf_monotonic_to_unix_offset_ns']
    windows, events = [], {}
    for sample in stalls['samples']:
        if not sample['slow_reclaim_events']:
            continue
        event = sample['slow_reclaim_events'][0]
        if event['overlap_seconds'] < 1:
            continue
        token = f"q12-iteration{sample['iteration']}-longest-reclaim"
        query = sample['execution_window_ms']
        windows.append((token, max(event['begin_ns'] + offset, query['start'] * 10**6),
                        min(event['end_ns'] + offset, query['end'] * 10**6)))
        events[token] = event
    pids = {e['pid'] for line in (directory / 'host.jsonl').open()
            for e in json.loads(line)['executors']}
    samples = perf_summary(directory / 'cpu.data', windows, pids, offset) if windows else {}
    assert all(s['samples'] > 0 for s in samples.values()), 'No CPU observations in a requested interval'
    event_threads = {}
    for window in windows:
        token = window[0]
        event_threads[token] = perf_summary(
            directory / 'cpu.data', [window], pids, offset,
            tids={events[token]['tid']})[token]
    output = {'event_thread_perf': event_threads, 'cpu_event': state.get('cpu_event', 'cpu-clock'), 'reclaim_events': events,
              'perf': samples, 'source_revision': subprocess.check_output(
                  ['git', '-C', str(Path(__file__).parent), 'rev-parse', 'HEAD'], text=True).strip(),
              'limits': ['One measured reclaim interval per Q12 action; concurrent waiters overlap',
                         'Sample counts are observations, not measured lock hold durations']}
    (directory / 'long-reclaim-perf.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps({k: {'samples': v['samples'], 'nonidle': v['nonidle_samples'],
                         'leaf': v['leaf'][:5], 'stacks': v['stacks'][:2]} for k, v in samples.items()}))


if __name__ == '__main__':
    main()
