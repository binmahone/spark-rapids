#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Import a completed capture whose CLI exited before report generation."""
import argparse
import json
from pathlib import Path
import sqlite3
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state_file = a.trace / 'state.json'
state = json.loads(state_file.read_text())
assert state['status'] == 'failed' and state['nsys_exit'] == 0
assert all(s['validation']['status'] == 'passed' for s in state['result']['samples'])
raw, = (a.trace / 'tmp').glob('nsys-*/nsys-report-*.qdstrm')
base = a.trace / ('q' + str(a.query))
assert not base.with_suffix('.nsys-rep').exists()
with (a.trace / 'recovery.log').open('x') as log:
    subprocess.run(['nsys', 'import', '--output-file=' + str(base.with_suffix('.nsys-rep')),
                    str(raw)], check=True, stdout=log, stderr=subprocess.STDOUT, timeout=600)
    subprocess.run(['nsys', 'export', '--type=sqlite', '--output=' + str(base.with_suffix('.sqlite')),
                    str(base.with_suffix('.nsys-rep'))], check=True,
                   stdout=log, stderr=subprocess.STDOUT, timeout=300)
db = sqlite3.connect(f'file:{base.with_suffix(".sqlite")}?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
coverage = []
for sample in state['result']['samples']:
    start, end = (sample[k] - epoch for k in ('started_unix_ns', 'finished_unix_ns'))
    rows = db.execute('select globalPid,min(start),max(end),count(*) from CUPTI_ACTIVITY_KIND_KERNEL '
                      'where end>? and start<? group by globalPid', (start, end)).fetchall()
    assert len(rows) == 8 and all(r[3] > 100 for r in rows)
    coverage.append({'iteration': sample['iteration'], 'executors': rows})
original = a.trace / 'state-before-import-recovery.json'
assert not original.exists()
original.write_text(state_file.read_text())
state['recovery'] = {'cause': 'CLI exited before importing retained QDSTRM',
                     'raw_capture': str(raw), 'coverage': coverage,
                     'original_error': state.pop('error')}
state['status'] = 'completed'
state_file.write_text(json.dumps(state, indent=2) + '\n')
print('Recovered both SQL actions with eight executor CUDA timelines')
