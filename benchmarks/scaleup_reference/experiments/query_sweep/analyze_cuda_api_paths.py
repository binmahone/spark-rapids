#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Attribute CUDA API waits to enclosing native ranges in each SQL action."""
import argparse
from collections import defaultdict
import heapq
import json
from pathlib import Path
import sqlite3

from analyze_data_path_trace import merged, duration

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed' and state['nsys_exit'] == 0
db = sqlite3.connect(f'file:{a.trace / ("q" + str(a.query) + ".sqlite")}?mode=ro', uri=True)
names = dict(db.execute('select id,value from StringIds'))
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
result = {'limits': ['API durations summed across threads are not SQL wall time.',
    'Enclosing native ranges identify the caller; overlapping ranges are inclusive.',
    'GPU activity includes all streams of the executor, not only the synchronized stream.',
    'An API wait overlapping GPU work is not automatically removable overhead.'], 'windows': []}
for sample in state['result']['samples']:
    start, end = (sample[k] - epoch for k in ('started_unix_ns', 'finished_unix_ns'))
    calls = defaultdict(list)
    for row in db.execute('select start,end,globalTid,nameId,callchainId from '
                          'CUPTI_ACTIVITY_KIND_RUNTIME where end>? and start<?', (start, end)):
        if row[1] - row[0] >= 100000:
            calls[row[2]].append(row)
    ranges = defaultdict(list)
    for begin, finish, tid, label, label_id in db.execute(
            'select start,end,globalTid,text,textId from NVTX_EVENTS where end>? and start<?',
            (start, end)):
        if tid in calls and finish is not None:
            ranges[tid].append((begin, finish, label or names.get(label_id, '')))
    groups = defaultdict(lambda: {'calls': 0, 'summed_seconds': 0.0, 'max_seconds': 0.0,
                                 'callchain_ids': []})
    sync_by_pid = defaultdict(list)
    for tid, items in calls.items():
        ordered = sorted(ranges[tid])
        active, deadlines, cursor = {}, [], 0
        for begin, finish, _, name_id, chain in sorted(items):
            while cursor < len(ordered) and ordered[cursor][0] <= begin:
                item = ordered[cursor]
                active[cursor] = item
                heapq.heappush(deadlines, (item[1], cursor))
                cursor += 1
            while deadlines and deadlines[0][0] < finish:
                _, index = heapq.heappop(deadlines)
                active.pop(index, None)
            labels = tuple(r[2] for r in sorted(active.values()) if r[1] >= finish)
            name = names[name_id]
            group = groups[(name, labels)]
            seconds = (min(end, finish) - max(start, begin)) / 1e9
            group['calls'] += 1
            group['summed_seconds'] += seconds
            group['max_seconds'] = max(group['max_seconds'], seconds)
            if chain is not None and chain not in group['callchain_ids'] and len(group['callchain_ids']) < 3:
                group['callchain_ids'].append(chain)
            if 'Synchronize' in name:
                sync_by_pid[(tid >> 24) << 24].append((max(start, begin), min(end, finish)))
    executors = []
    for pid, intervals in sorted(sync_by_pid.items()):
        waits = merged(intervals)
        gpu = merged((max(start, begin), min(end, finish))
            for table in ('KERNEL', 'MEMCPY', 'MEMSET')
            for begin, finish in db.execute(f'select start,end from CUPTI_ACTIVITY_KIND_{table} '
                'where globalPid=? and end>? and start<?', (pid, start, end)))
        overlap = duration(waits) + duration(gpu) - duration(merged(waits + gpu))
        executors.append({'global_pid': pid, 'sync_union_seconds': duration(waits),
            'gpu_active_union_seconds': duration(gpu), 'sync_with_any_gpu_seconds': overlap,
            'sync_without_any_gpu_seconds': duration(waits) - overlap})
    result['windows'].append({'query': sample['query'], 'iteration': sample['iteration'],
        'sql_seconds': sample['sql_seconds'], 'executors': executors,
        'api_paths': [{'api': key[0], 'enclosing_native_ranges': key[1], **value}
            for key, value in sorted(groups.items(), key=lambda item: -item[1]['summed_seconds'])]})
(a.trace / 'api-path-analysis.json').write_text(json.dumps(result, indent=2) + '\n')
for w in result['windows']:
    print(w['query'], w['iteration'], w['sql_seconds'])
    for row in w['api_paths'][:12]:
        print(row['api'], round(row['summed_seconds'], 3), row['calls'], row['enclosing_native_ranges'])
    print('executors', w['executors'])
