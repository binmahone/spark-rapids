#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure host submission intervals after bulk input copies in a retained CUDA trace."""
import argparse
from bisect import bisect_left
from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from analyze_critical_path import schedule

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed' and state['nsys_exit'] == 0
query = schedule(Path(state['run']))[f'q{a.query}-iteration2']
stage = max(query['stages'], key=lambda row: row['tasks'])
db = sqlite3.connect('file:' + str(a.trace / f'q{a.query}.sqlite') + '?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
start = query['window']['start'] * 1000000 + round(stage['start_seconds'] * 1e9) - epoch
end = query['window']['start'] * 1000000 + round(stage['end_seconds'] * 1e9) - epoch
names = dict(db.execute('select id,value from StringIds'))
kinds = dict(db.execute('select id,label from ENUM_CUDA_MEMCPY_OPER'))
api, threads = {}, defaultdict(list)
for s, e, tid, corr, name in db.execute('select start,end,globalTid,correlationId,nameId '
        'from CUPTI_ACTIVITY_KIND_RUNTIME where end>? and start<?', (start, end)):
    row = {'start': s, 'end': e, 'tid': tid, 'name': names[name], 'correlation': corr}
    api[((tid >> 24) << 24, corr)] = row
    threads[tid].append(row)
for rows in threads.values():
    rows.sort(key=lambda r: r['start'])
kernels = defaultdict(list)
for s, e, pid, corr, stream in db.execute('select start,end,globalPid,correlationId,streamId '
        'from CUPTI_ACTIVITY_KIND_KERNEL where end>? and start<?', (start, end)):
    call = api.get((pid, corr))
    if call:
        kernels[call['tid']].append((call['start'], s, e, stream, corr))
for rows in kernels.values():
    rows.sort()
# Group a batched memcpy into one submission and its complete GPU transfer interval.
copies = {}
for s, e, pid, kind, size, corr, stream in db.execute('select start,end,globalPid,copyKind,bytes,correlationId,streamId '
        'from CUPTI_ACTIVITY_KIND_MEMCPY where end>? and start<?', (start, end)):
    call = api.get((pid, corr))
    if not call or kinds[kind] != 'Host-to-Device':
        continue
    key = (pid, corr)
    if key not in copies:
        copies[key] = {'start': s, 'end': e, 'bytes': 0, 'call': call, 'stream': stream}
    row = copies[key]
    row['start'] = min(row['start'], s)
    row['end'] = max(row['end'], e)
    row['bytes'] += size
records = defaultdict(list)
for (pid, corr), copy in copies.items():
    if copy['bytes'] < 32 * 1024**2:
        continue
    call = copy['call']; tid = call['tid']; launches = kernels[tid]
    index = bisect_left(launches, (call['end'],))
    if index == len(launches):
        continue
    launch, gpu_start, gpu_end, stream, kernel_corr = launches[index]
    if stream != copy['stream'] or launch - call['end'] > 1000000000:
        continue
    between = [r for r in threads[tid] if r['start'] >= call['end'] and r['end'] <= launch]
    syncs = [r for r in between if 'Synchronize' in r['name']]
    if not syncs:
        continue
    records[pid].append({'bytes': copy['bytes'], 'copy_correlation': corr,
        'kernel_correlation': kernel_corr, 'tid': tid, 'stream': stream,
        'sync_ms': sum(r['end']-r['start'] for r in syncs)/1e6,
        'copy_end_to_launch_ms': (launch-copy['end'])/1e6,
        'copy_end_to_gpu_kernel_ms': (gpu_start-copy['end'])/1e6,
        'last_sync_to_launch_ms': (launch-syncs[-1]['end'])/1e6,
        'host_submission_ms': (launch-call['end'])/1e6,
        'syncs': [{**r, 'start': r['start']-copy['start'], 'end': r['end']-copy['start']} for r in syncs]})
result = {'stage': {k:v for k,v in stage.items() if k!='operator_metrics'},
    'limits': ['Adjacent calls and matching CUDA streams are not a native call-stack attribution.',
               'Multiple bulk submissions can map to the same next kernel; unique kernel groups are reported.',
               'Post-copy host time includes CPU scheduling and other work, not just metadata construction.',
               'These diagnostic intervals are not predicted query speedups.'], 'executors': []}
for pid, rows in sorted(records.items()):
    unique = {}
    for row in rows:
        key=row['kernel_correlation']
        if key not in unique or row['copy_end_to_launch_ms'] < unique[key]['copy_end_to_launch_ms']:
            unique[key]=row
    values=list(unique.values())
    totals={key:sum(r[key] for r in values)/1000 for key in
        ('sync_ms','copy_end_to_launch_ms','copy_end_to_gpu_kernel_ms','last_sync_to_launch_ms','host_submission_ms')}
    item={'pid':pid,'bulk_submissions':len(rows),'unique_next_kernels':len(values),
          'summed_seconds':totals,'largest_handoffs':sorted(values,key=lambda r:-r['copy_end_to_launch_ms'])[:12]}
    result['executors'].append(item)
    print(pid,item['unique_next_kernels'],totals,flush=True)
assert len(result['executors'])==8
(a.trace/'scan-handoff-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
