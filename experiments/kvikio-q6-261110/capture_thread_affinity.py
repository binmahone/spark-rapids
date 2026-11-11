#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Read thread affinity for this experiment's executors on one allocated node."""
import argparse
import collections
import json
import os
from pathlib import Path
import socket
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--job', type=int, required=True)
parser.add_argument('--timeout', type=float, default=150)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
assert args.timeout <= 180
job_marker = f'/logs/job-{args.job}/hs_err_pid'


def snapshot():
    executors = []
    for process in Path('/proc').iterdir():
        if not process.name.isdigit():
            continue
        try:
            if process.stat().st_uid != os.getuid():
                continue
            command = (process / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
            if 'CoarseGrainedExecutorBackend' not in command or job_marker not in command:
                continue
            threads = []
            for task in (process / 'task').iterdir():
                try:
                    status = dict(line.split(':', 1) for line in (task / 'status').read_text().splitlines()
                                  if ':' in line)
                    threads.append({'tid': int(task.name), **{k: status[k].strip() for k in
                                    ('Name', 'State', 'Cpus_allowed_list', 'Mems_allowed_list')}})
                except (OSError, KeyError):
                    continue
            executors.append({'pid': int(process.name), 'threads': threads})
        except OSError:
            continue
    return {'epoch_ns': time.time_ns(), 'hostname': socket.gethostname(), 'executors': executors}


deadline = time.monotonic() + args.timeout
samples = []
while time.monotonic() < deadline:
    sample = snapshot()
    active = False
    for executor in sample['executors']:
        names = [t['Name'] for t in executor['threads']]
        active |= any(n.startswith('paul-range-io') for n in names) or names.count('multithreaded f') > 20
    if active:
        samples.append(sample)
        if len(samples) == 2:
            break
        time.sleep(0.5)
    else:
        time.sleep(1)
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps({'job': args.job, 'samples': samples}, indent=2) + '\n')
print(json.dumps({'job': args.job, 'samples': len(samples), 'thread_cpu_masks': dict(collections.Counter(
    t['Cpus_allowed_list'] for s in samples for e in s['executors'] for t in e['threads']))}))
if not samples:
    raise SystemExit('No active range-reader threads were observed before the deadline')
