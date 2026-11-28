#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Run one predeclared sequential ABBA matrix without overlapping applications."""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
import services

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
p.add_argument('--family', choices=('scan-pack', 'scan-split', 'shuffle-buffer', 'shuffle-wait'), required=True)
a = p.parse_args()
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
directory = a.work / 'data-path' / (a.family + '-matrix')
directory.mkdir(parents=True, exist_ok=False)
write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
    'start_time': services.start_time(os.getpid())})
state = {'status': 'running', 'family': a.family, 'completed': []}

def interrupt(signum, frame):
    raise InterruptedError('Signal ' + str(signum))

for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, interrupt)
try:
    for phase in ('A0', 'B1', 'B2', 'A3'):
        state['phase'] = phase
        write(directory / 'state.json', state)
        command = ['bash', str(HERE / 'start_data_path.sh'), str(a.work), a.thread_id,
                   phase, a.family]
        child, record = services.launch(command, directory / (phase + '.log'), dict(os.environ))
        receipt = directory / (phase + '-service.json')
        write(receipt, [record])
        try:
            assert child.wait(timeout=1000) == 0, phase
            outcome = read(a.work / 'data-path' / (a.family + '-' + phase) / 'state.json')
            assert outcome['status'] == 'completed', outcome.get('error')
        finally:
            services.stop(receipt)
        state['completed'].append(phase)
    subprocess.run([sys.executable, str(HERE / 'analyze_data_path.py'), '--work', str(a.work),
                    '--family', a.family], check=True)
    state['status'] = 'completed'
except BaseException as error:
    state.update(status='failed', error=repr(error))
    raise
finally:
    write(directory / 'state.json', state)
