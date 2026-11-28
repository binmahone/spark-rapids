#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure copy launch delays, separating small metadata copies from bulk transfers."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
import statistics

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed' and state['nsys_exit'] == 0
db = sqlite3.connect('file:' + str(a.trace / ('q' + str(a.query) + '.sqlite')) + '?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
names = dict(db.execute('select id,value from StringIds'))
out = {'limits': [
    'GPU start minus API end is a lower bound on launch-to-execution delay, not a direct queue metric.',
    'Delay may include stream dependencies, scheduling and shared copy engines.',
    'Summed delays across copies overlap and are not query wall time.',
    'Small copies are grouped by size; size alone does not establish a metadata call site.'
], 'windows': []}
for sample in state['result']['samples']:
    start, end = (sample[k] - epoch for k in ('started_unix_ns', 'finished_unix_ns'))
    apis = {}
    totals = defaultdict(lambda: [0, 0.0])
    for s, e, tid, corr, name in db.execute(
            'select start,end,globalTid,correlationId,nameId from CUPTI_ACTIVITY_KIND_RUNTIME '
            'where end>? and start<?', (start, end)):
        apis[((tid >> 24) << 24, corr)] = (s, e, names[name])
        if 'Memcpy' in names[name] or 'Synchronize' in names[name]:
            row = totals[names[name]]
            row[0] += 1
            row[1] += (min(e, end) - max(s, start)) / 1e9
    groups = defaultdict(list)
    for s, e, pid, corr, size, stream in db.execute(
            'select start,end,globalPid,correlationId,bytes,streamId '
            'from CUPTI_ACTIVITY_KIND_MEMCPY where end>? and start<? and copyKind=1', (start, end)):
        api = apis.get((pid, corr))
        bucket = 'small_le_64KiB' if size <= 65536 else (
            'medium_le_32MiB' if size <= 32 * 1024**2 else 'large_gt_32MiB')
        groups[bucket].append({'bytes': size, 'gpu_ms': (min(e, end) - max(s, start)) / 1e6,
            'delay_ms': max(0, s - api[1]) / 1e6 if api else None})
    buckets = {}
    for name, rows in groups.items():
        delays = sorted(r['delay_ms'] for r in rows if r['delay_ms'] is not None)
        assert delays
        buckets[name] = {'copies': len(rows), 'matched_apis': len(delays),
            'bytes': sum(r['bytes'] for r in rows),
            'gpu_summed_ms': sum(r['gpu_ms'] for r in rows),
            'delay_sum_ms': sum(delays), 'delay_median_ms': statistics.median(delays),
            'delay_p95_ms': delays[min(len(delays) - 1, int(len(delays) * .95))],
            'delay_max_ms': max(delays)}
    out['windows'].append({'query': sample['query'], 'iteration': sample['iteration'],
        'sql_seconds': sample['sql_seconds'], 'h2d': buckets,
        'api_totals': {k: {'calls': v[0], 'summed_thread_seconds': v[1]}
                       for k, v in sorted(totals.items(), key=lambda x: -x[1][1])}})
(a.trace / 'copy-queue-analysis.json').write_text(json.dumps(out, indent=2) + '\n')
for window in out['windows']:
    print(json.dumps(window))
