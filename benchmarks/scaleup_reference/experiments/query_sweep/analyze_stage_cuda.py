#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Align long Spark stage intervals with observed CUDA work on all eight executors."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from analyze_critical_path import schedule
from analyze_data_path_trace import merged, duration

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed' and state['nsys_exit'] == 0
stages = schedule(Path(state['run']))
db = sqlite3.connect('file:' + str(a.trace / ('q' + str(a.query) + '.sqlite')) + '?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
names = dict(db.execute('select id,value from StringIds'))
kinds = dict(db.execute('select id,label from ENUM_CUDA_MEMCPY_OPER'))
pids = [r[0] for r in db.execute('select distinct globalPid from CUPTI_ACTIVITY_KIND_KERNEL')]
assert len(pids) == 8
result = {'limits': [
    'These are diagnostic profiled stage intervals, not benchmark scores.',
    'GPU work from overlapping Spark stages is included and cannot be assigned by interval alone.',
    'CUDA activity intervals do not measure SM utilization or maximum achievable bandwidth.',
    'Copy and kernel intervals overlap; summed activity and operator times are not stage wall time.',
    'GPU copy bytes count whole operations intersecting the window; boundaries have millisecond precision.'
], 'stages': []}
for token, query in stages.items():
    if not token.startswith('q' + str(a.query) + '-'): continue
    for stage in query['stages']:
        if stage['span_seconds'] < 1: continue
        begin = query['window']['start'] * 1000000 + round(stage['start_seconds'] * 1e9) - epoch
        end = query['window']['start'] * 1000000 + round(stage['end_seconds'] * 1e9) - epoch
        activity = defaultdict(lambda: defaultdict(list))
        kernel_totals, copy_bytes = defaultdict(float), defaultdict(int)
        for s, e, pid, name in db.execute('select start,end,globalPid,shortName '
                'from CUPTI_ACTIVITY_KIND_KERNEL where end>? and start<?', (begin, end)):
            activity[pid]['kernel'].append((max(s, begin), min(e, end)))
            kernel_totals[names[name]] += (min(e, end) - max(s, begin)) / 1e9
        for s, e, pid, kind, size in db.execute('select start,end,globalPid,copyKind,bytes '
                'from CUPTI_ACTIVITY_KIND_MEMCPY where end>? and start<?', (begin, end)):
            activity[pid][kinds[kind]].append((max(s, begin), min(e, end)))
            copy_bytes[kinds[kind]] += size
        for s, e, pid in db.execute('select start,end,globalPid '
                'from CUPTI_ACTIVITY_KIND_MEMSET where end>? and start<?', (begin, end)):
            activity[pid]['memset'].append((max(s, begin), min(e, end)))
        rows = []
        categories = sorted({kind for values in activity.values() for kind in values})
        for pid in pids:
            values = activity[pid]
            unions = {name: duration(merged(values[name])) for name in categories}
            any_gpu = duration(merged([interval for intervals in values.values() for interval in intervals]))
            rows.append({'pid': pid, 'union_seconds': unions, 'any_gpu_seconds': any_gpu,
                         'no_gpu_seconds': (end - begin) / 1e9 - any_gpu})
        row = {key: value for key, value in stage.items() if key != 'operator_metrics'}
        row.update(query_window=token, cuda_executors=rows, copy_bytes=dict(copy_bytes),
            top_kernels_summed_seconds=sorted(kernel_totals.items(), key=lambda x: -x[1])[:10],
            overlapping_stages=[other['stage'] for other in query['stages']
                if other['stage'] != stage['stage'] and other['start_seconds'] < stage['end_seconds']
                and other['end_seconds'] > stage['start_seconds']],
            operator_times=sorted([m for m in stage['operator_metrics']
                if m['type'] == 'nsTiming' and m['task_update_sum'] > 1e9
                and 'excl. SemWait' in m['name']], key=lambda m: -m['task_update_sum'])[:15])
        result['stages'].append(row)
        print(token, 'stage', stage['stage'], 'tasks', stage['tasks'], 'wall', stage['span_seconds'],
              'mean_activity', {kind: round(sum(r['union_seconds'][kind] for r in rows) / 8, 3)
                                for kind in categories},
              'mean_no_gpu', round(sum(r['no_gpu_seconds'] for r in rows) / 8, 3),
              'overlapping_stages', row['overlapping_stages'])
(a.trace / 'stage-cuda-analysis.json').write_text(json.dumps(result, indent=2) + '\n')
