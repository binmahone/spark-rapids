#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Launch the isolated Q8 application as a child of the CUDA profiler."""
import argparse
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
p.add_argument('--variant', choices=('frozen', 'copy-stream'), default='frozen')
a = p.parse_args()
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
write(a.output / 'workload-controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                            'start_time': services.start_time(os.getpid())})
def interrupt(signum, frame):
    raise InterruptedError('Signal ' + str(signum))
for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, interrupt)
started = time.time_ns()
candidate = a.variant == 'copy-stream'
if candidate:
    assert (a.work / 'copy-stream-build/build.exit').read_text().strip() == '0'
path, result = launch_group(a.work, 'scan-timeline-Q8' + ('-copy-stream' if candidate else ''),
    0, [controls()[8]], {
    'spark.rapids.sql.reader.kvikio.numThreads': '32',
    'spark.rapids.sql.metrics.level': 'DEBUG'},
    experimental_engine=a.work / 'copy-stream-engine/.build/artifacts/amd64' if candidate else None)
write(a.output / 'workload.json', {'run': str(path), 'start_unix_ns': started,
                                  'end_unix_ns': time.time_ns(), 'result': result})
assert result['status'] == 'completed' and result['driver_exit_code'] == 0
assert len(result['samples']) == 2 and all(s['validation']['status'] == 'passed' for s in result['samples'])
