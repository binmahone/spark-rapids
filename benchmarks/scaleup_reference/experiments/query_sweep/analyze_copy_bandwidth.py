#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure observed H2D bytes and active intervals within each SQL action."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from analyze_data_path_trace import merged, duration

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed'
db = sqlite3.connect(f'file:{a.trace / ("q" + str(a.query) + ".sqlite")}?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
memory_kinds = {r[0]: r[2] for r in db.execute('select * from ENUM_CUDA_MEM_KIND')}
report = {'limits': ['Active-copy bandwidth is not whole-query throughput.',
                    'Per-process bandwidth is bytes divided by the union of its H2D intervals.',
                    'Summed CUDA operation durations are not elapsed SQL time.'], 'windows': []}
for sample in state['result']['samples']:
    begin, end = (sample[k] - epoch for k in ('started_unix_ns', 'finished_unix_ns'))
    processes = defaultdict(list)
    kinds = defaultdict(lambda: {'bytes': 0, 'copies': 0})
    for start, finish, pid, size, kind in db.execute(
            'select start,end,globalPid,bytes,srcKind from CUPTI_ACTIVITY_KIND_MEMCPY '
            'where copyKind=1 and start>=? and end<=?', (begin, end)):
        processes[pid].append((start, finish, size))
        kinds[memory_kinds[kind]]['bytes'] += size
        kinds[memory_kinds[kind]]['copies'] += 1
    rows = []
    for pid, copies in processes.items():
        total = sum(row[2] for row in copies)
        active = duration(merged((r[0], r[1]) for r in copies))
        rows.append({'global_pid': pid, 'bytes': total, 'copies': len(copies),
                     'active_seconds': active, 'active_GB_per_second': total / active / 1e9})
    result = {'query': sample['query'], 'iteration': sample['iteration'],
              'sql_seconds': sample['sql_seconds'], 'source_memory_kinds': dict(kinds),
              'processes': rows}
    report['windows'].append(result)
    print(sample['query'], sample['iteration'], dict(kinds),
          [(r['global_pid'], round(r['active_GB_per_second'], 2)) for r in rows])
(a.trace / 'copy-bandwidth-analysis.json').write_text(json.dumps(report, indent=2) + '\n')
