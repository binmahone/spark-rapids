#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure per-process CUDA interval unions within recorded Spark SQL windows."""
import argparse
import json
from pathlib import Path
import sqlite3


def merged(intervals):
    result = []
    for start, end in sorted(intervals):
        if result and start <= result[-1][1]:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([start, end])
    return result


def duration(intervals):
    return sum(end - start for start, end in intervals) / 1e9


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--variant', choices=('frozen', 'copy-stream', 'join-stats'), default='frozen')
    parser.add_argument('--query', type=int, choices=(8, 9), default=8)
    args = parser.parse_args()
    trace = args.work / ('scan-timeline/Q' + str(args.query) + '-process-tree' +
                         ('' if args.variant == 'frozen' else '-' + args.variant))
    state = json.loads((trace / 'state.json').read_text())
    assert state['status'] == 'completed' and state['nsys_exit'] == 0
    app = Path(state['run']) / 'applications/suite'
    profile = json.loads((app / 'profile.json').read_text())
    events = Path(profile['spark.eventLog.dir'].removeprefix('file://'))
    windows = {}
    for path in events.iterdir():
        if not path.is_file() or path.name.startswith('.'):
            continue
        assert not path.name.endswith('.inprogress')
        for line in path.open():
            if 'SparkListenerSQLExecution' not in line:
                continue
            event = json.loads(line)
            key = event['executionId']
            if event.get('description', '').startswith(f'q{args.query}-iteration'):
                windows[key] = {'name': event['description'], 'start_ms': event['time']}
            elif event['Event'].endswith('SQLExecutionEnd') and key in windows:
                windows[key]['end_ms'] = event['time']
    assert len(windows) == 2 and all('end_ms' in row for row in windows.values())
    db = sqlite3.connect(f'file:{trace / ("q" + str(args.query) + ".sqlite")}?mode=ro', uri=True)
    epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
    pids = [row[0] for row in db.execute(
        'select distinct globalPid from CUPTI_ACTIVITY_KIND_KERNEL order by globalPid')]
    assert len(pids) == 8
    result = {'classification': 'Diagnostic; excluded from SOL scores',
              'trace_epoch_ns': epoch, 'source_revision': state['source_revision'],
              'limits': ['CUDA device IDs are process-local; results group by executor process.',
                         'Interval presence is not SM utilization or achieved hardware bandwidth.',
                         'Copy-only time is not an achievable query speedup estimate.',
                         'SQL event windows exclude some driver-side SQL action overhead.',
                         'Clock alignment uses trace UTC origin and millisecond Spark event times.'],
              'windows': []}
    for window in sorted(windows.values(), key=lambda row: row['start_ms']):
        start = window['start_ms'] * 1000000 - epoch
        end = window['end_ms'] * 1000000 - epoch
        assert 0 < start < end
        rows = []
        for pid in pids:
            def intervals(table, extra=''):
                return merged((max(a, start), min(b, end)) for a, b in db.execute(
                    f'select start,end from {table} where globalPid=? and end>? '
                    f'and start<? {extra}', (pid, start, end)))
            kernels = intervals('CUPTI_ACTIVITY_KIND_KERNEL')
            h2d = intervals('CUPTI_ACTIVITY_KIND_MEMCPY', 'and copyKind=1')
            combined = merged(kernels + h2d)
            overlap = duration(kernels) + duration(h2d) - duration(combined)
            streams = db.execute('select streamId,count(*),sum(bytes) '
                                 'from CUPTI_ACTIVITY_KIND_MEMCPY where globalPid=? '
                                 'and end>? and start<? and copyKind=1 group by streamId',
                                 (pid, start, end)).fetchall()
            rows.append({'global_pid': pid, 'h2d_stream_counts_bytes': streams,
                         'kernel_union_seconds': duration(kernels),
                         'h2d_union_seconds': duration(h2d),
                         'h2d_kernel_overlap_seconds': overlap,
                         'h2d_without_kernel_seconds': duration(h2d) - overlap,
                         'neither_kernel_nor_h2d_seconds': (end-start)/1e9-duration(combined)})
        top = db.execute('select s.value,sum(min(k.end,?)-max(k.start,?))/1e9 as seconds '
                         'from CUPTI_ACTIVITY_KIND_KERNEL k join StringIds s on s.id=k.shortName '
                         'where k.end>? and k.start<? group by s.value order by seconds desc limit 12',
                         (end, start, start, end)).fetchall()
        result['windows'].append({**window, 'sql_event_seconds': (end-start)/1e9,
                                  'executors': rows, 'top_kernels_summed_seconds': top})
    output = trace / 'timeline-analysis.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    for window in result['windows']:
        print(window['name'], 'event_seconds', window['sql_event_seconds'])
        for key in ('kernel_union_seconds', 'h2d_union_seconds',
                    'h2d_kernel_overlap_seconds', 'h2d_without_kernel_seconds'):
            values = [row[key] for row in window['executors']]
            print(key, 'min', min(values), 'mean', sum(values)/len(values), 'max', max(values))
        print('top_kernels_summed_seconds', window['top_kernels_summed_seconds'][:5])


if __name__ == '__main__':
    main()
