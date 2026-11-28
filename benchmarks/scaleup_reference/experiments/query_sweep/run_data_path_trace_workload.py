#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Launch one isolated query application as a child of the CUDA profiler."""
import argparse
from dataclasses import replace
import os
from pathlib import Path
import signal
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from critical_path_variants import controls
from run_screen import launch_group
import services

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
p.add_argument('--thread-id', required=True)
p.add_argument('--output', required=True, type=Path)
p.add_argument('--variant', choices=('control', 'scan-pack', 'scan-split', 'scan-split-cuda', 'shuffle-buffer', 'shuffle-wait'), default='control')
p.add_argument('--query', type=int, choices=(5, 8, 10, 18, 20), default=8)
a = p.parse_args()
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
write(a.output / 'workload-controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                            'start_time': services.start_time(os.getpid())})
def interrupt(signum, frame):
    raise InterruptedError('Signal ' + str(signum))
for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, interrupt)
started = time.time_ns()
assert (a.work / 'evidence/prepare.exit').read_text().strip() == '0'
selected = controls()[a.query]
selected = replace(selected, overrides={**selected.overrides,
    'spark.rapids.sql.join.hashTable.reuse': 'false'})
if a.variant in ('scan-split', 'scan-split-cuda'):
    selected = replace(selected, overrides={**selected.overrides,
        'spark.sql.files.maxPartitionBytes': '1g'})
path, result = launch_group(a.work, 'data-path-trace-Q' + str(a.query) + '-' + a.variant,
    0, [selected], {
    'spark.rapids.sql.reader.kvikio.numThreads': '32',
    'spark.rapids.sql.metrics.level': 'DEBUG',
    'spark.rapids.sql.nvtx.traceTaskGpuOwnership': 'false' if a.variant == 'scan-split-cuda' else 'true',
    'spark.task.resource.gpu.amount': '0.25',
    'spark.rapids.shuffle.compression.codec': 'none',
    'spark.rapids.sql.format.parquet.hybridScan.maxH2DPackSize': '128m' if a.variant == 'scan-pack' else '32m',
    'spark.rapids.shuffle.ucx.bounceBuffers.size': '16m' if a.variant == 'shuffle-buffer' else '4m'},
    experimental_engine=a.work / 'artifacts' / ('shuffle-wait' if a.variant == 'shuffle-wait' else 'decimal-null-copy'),
    runtime_hash_reuse_queries=[9] if a.query == 9 else None, diagnostic_nvtx=a.variant != 'scan-split-cuda')
write(a.output / 'workload.json', {'run': str(path), 'start_unix_ns': started,
                                  'end_unix_ns': time.time_ns(), 'result': result})
assert result['status'] == 'completed' and result['driver_exit_code'] == 0
assert len(result['samples']) == 2 and all(s['validation']['status'] == 'passed' for s in result['samples'])
if a.query == 9:
    for sample in result['samples']:
        assert sample['runtime_hash_reuse_proof']['physical_plan_unchanged']
        assert sum(n['metrics'].get('hashTableReuses', 0) for n in sample['plan_metrics']) > 0
