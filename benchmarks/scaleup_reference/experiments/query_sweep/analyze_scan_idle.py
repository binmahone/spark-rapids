#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Locate scan GPU gaps and their adjacent submissions without inferring CPU utilization."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from analyze_critical_path import schedule
from analyze_data_path_trace import merged, duration


def intersection(left, right):
    i = j = 0
    out = []
    while i < len(left) and j < len(right):
        start = max(left[i][0], right[j][0])
        end = min(left[i][1], right[j][1])
        if end > start:
            out.append((start, end))
        if left[i][1] < right[j][1]:
            i += 1
        else:
            j += 1
    return out


p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed' and state['nsys_exit'] == 0
assert state.get('full_sql_trace', True), 'Partial kernel proof is not a full-stage timeline'
timeline = schedule(Path(state['run']))[f'q{a.query}-iteration2']
stage = max(timeline['stages'], key=lambda s: s['tasks'])
db = sqlite3.connect('file:' + str(a.trace / f'q{a.query}.sqlite') + '?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
start = timeline['window']['start'] * 1000000 + round(stage['start_seconds'] * 1e9) - epoch
end = timeline['window']['start'] * 1000000 + round(stage['end_seconds'] * 1e9) - epoch
names = dict(db.execute('select id,value from StringIds'))
kinds = dict(db.execute('select id,label from ENUM_CUDA_MEMCPY_OPER'))
events, apis, correlations = defaultdict(list), defaultdict(lambda: defaultdict(list)), {}
for s, e, pid, name, corr, stream in db.execute('select start,end,globalPid,shortName,correlationId,streamId '
        'from CUPTI_ACTIVITY_KIND_KERNEL where end>? and start<?', (start, end)):
    events[pid].append({'start': s, 'end': e, 'kind': 'kernel', 'name': names[name],
                        'correlation': corr, 'stream': stream})
for s, e, pid, kind, size, corr, stream in db.execute('select start,end,globalPid,copyKind,bytes,correlationId,streamId '
        'from CUPTI_ACTIVITY_KIND_MEMCPY where end>? and start<?', (start, end)):
    events[pid].append({'start': s, 'end': e, 'kind': kinds[kind], 'bytes': size,
                        'correlation': corr, 'stream': stream})
for s, e, pid, corr, stream in db.execute('select start,end,globalPid,correlationId,streamId '
        'from CUPTI_ACTIVITY_KIND_MEMSET where end>? and start<?', (start, end)):
    events[pid].append({'start': s, 'end': e, 'kind': 'memset', 'correlation': corr, 'stream': stream})
tables = {row[0] for row in db.execute("select name from sqlite_master where type='table'")}
for table in ('CUPTI_ACTIVITY_KIND_RUNTIME', 'CUPTI_ACTIVITY_KIND_DRIVER'):
    if table not in tables:
        continue
    for s, e, tid, corr, name in db.execute('select start,end,globalTid,correlationId,nameId '
            f'from {table} where end>? and start<?', (start, end)):
        pid = (tid >> 24) << 24
        apis[pid][names[name]].append((max(s, start), min(e, end)))
        correlations[(pid, corr)] = {'start': s, 'end': e, 'name': names[name], 'tid': tid}
result = {'stage': {k: v for k, v in stage.items() if k != 'operator_metrics'},
    'limits': ['Activity intervals are not SM utilization.',
               'The next operation labels a gap; it does not prove why that operation was delayed.',
               'Overlapping CUDA API categories cannot be added.',
               'Only this process GPU activity is counted; other processes and CPU-only work are not attributed.'],
    'executors': []}
assert len(events) == 8
for pid, rows in sorted(events.items()):
    rows.sort(key=lambda r: r['start'])
    gaps, labelled = [], defaultdict(lambda: {'count': 0, 'seconds': 0.0})
    cursor = start
    details = []
    for row in rows:
        s, e = max(start, row['start']), min(end, row['end'])
        if s > cursor:
            gaps.append((cursor, s))
            label = row['kind']
            if label == 'Host-to-Device':
                label += '-bulk' if row['bytes'] > 65536 else '-small'
            labelled[label]['count'] += 1
            labelled[label]['seconds'] += (s - cursor) / 1e9
            if s - cursor >= 10000000:
                api = correlations.get((pid, row['correlation']))
                details.append({'start_seconds': (cursor - start) / 1e9,
                    'gap_ms': (s - cursor) / 1e6, 'next': row, 'submission': api})
        cursor = max(cursor, e)
    if cursor < end:
        gaps.append((cursor, end))
        labelled['tail']['count'] += 1
        labelled['tail']['seconds'] += (end - cursor) / 1e9
    overlap = {name: duration(intersection(gaps, merged(intervals)))
               for name, intervals in apis[pid].items()}
    row = {'pid': pid, 'gpu_gap_seconds': duration(gaps), 'by_next_operation': dict(labelled),
           'api_overlap_seconds': sorted(overlap.items(), key=lambda r: -r[1])[:20],
           'largest_gaps': sorted(details, key=lambda r: -r['gap_ms'])[:25]}
    result['executors'].append(row)
    print(pid, 'gap_seconds', row['gpu_gap_seconds'], 'by_next', dict(labelled),
          'api_overlap', row['api_overlap_seconds'][:6], flush=True)
(a.trace / 'scan-idle-analysis.json').write_text(json.dumps(result, indent=2) + '\n')
