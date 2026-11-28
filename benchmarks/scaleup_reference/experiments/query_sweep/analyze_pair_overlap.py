#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Describe copy and kernel overlap on measured physical GPU pairs."""
import argparse
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
mapping = {pid: gpu for gpu, pid in db.execute('select gpuId,pid from TARGET_INFO_CUDA_DEVICE')}
assert set(mapping.values()) == set(range(8))
global_pids = [r[0] for r in db.execute('select distinct globalPid from CUPTI_ACTIVITY_KIND_KERNEL')]
gpu_pids = {mapping[(pid >> 24) & 0xffffff]: pid for pid in global_pids}
assert len(gpu_pids) == 8
def overlap(left, right):
    return duration(left) + duration(right) - duration(merged(left + right))
out = {'limits': ['Overlap does not prove dependency independence or achievable query savings.',
                  'Pair identities must be checked against retained PCIe topology.'], 'windows': []}
for sample in state['result']['samples']:
    start, end = (sample[key] - epoch for key in ('started_unix_ns', 'finished_unix_ns'))
    copies, kernels = {}, {}
    for gpu, pid in gpu_pids.items():
        for target, table, extra in ((copies, 'MEMCPY', 'and copyKind=1'),
                                     (kernels, 'KERNEL', '')):
            target[gpu] = merged((max(start, s), min(end, e)) for s, e in db.execute(
                f'select start,end from CUPTI_ACTIVITY_KIND_{table} where globalPid=? '
                f'and end>? and start<? {extra}', (pid, start, end)))
    rows = []
    for left, right in ((0, 1), (2, 3), (4, 5), (6, 7)):
        copy = merged(copies[left] + copies[right])
        kernel = merged(kernels[left] + kernels[right])
        row = {'gpus': [left, right], 'either_h2d_seconds': duration(copy),
               'both_h2d_seconds': overlap(copies[left], copies[right]),
               'either_kernel_seconds': duration(kernel),
               'copy_with_any_kernel_seconds': overlap(copy, kernel),
               'copy_without_any_kernel_seconds': duration(copy) - overlap(copy, kernel)}
        rows.append(row)
    out['windows'].append({'query': sample['query'], 'iteration': sample['iteration'],
                           'pairs': rows})
    print(sample['query'], sample['iteration'], rows)
(a.trace / 'pair-overlap-analysis.json').write_text(json.dumps(out, indent=2) + '\n')
