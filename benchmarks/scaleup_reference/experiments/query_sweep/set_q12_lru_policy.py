#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Change MGLRU only between experiments on the recorded dedicated instance."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
p.add_argument('--thread-id', required=True)
p.add_argument('--phase', required=True, choices=['lruoff0', 'lruon0', 'lruoff1', 'lruon1', 'restore-original'])
a = p.parse_args()
work = a.work.resolve()
owner = json.loads((work / 'evidence/owner.json').read_text())
assert owner['thread_id'] == a.thread_id
for receipt in (work / 'q12-diagnosis').glob('*/controller.json'):
    r = json.loads(receipt.read_text())
    stat = Path('/proc') / str(r['pid']) / 'stat'
    assert not stat.exists() or stat.read_text().split(') ', 1)[1].split()[19] != str(r['start_time'])
assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid',
                                    '--format=csv,noheader'], text=True).strip()
for port in (19077, 19080):
    with socket.socket() as s:
        assert s.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
setting = Path('/sys/kernel/mm/lru_gen/enabled')
before = int(setting.read_text().strip(), 0)
target = 0 if a.phase.startswith('lruoff') else 7
assert before in (0, 7)
path = work / 'evidence' / ('lru-policy-' + a.phase + '.json')
assert not path.exists()
report = {'status': 'changing', 'thread_id': a.thread_id, 'instance_id': owner['instance_id'],
          'phase': a.phase, 'before': before, 'target': target, 'kernel': os.uname().release,
          'start_unix_ns': time.time_ns(), 'memory_before': Path('/proc/meminfo').read_text(),
          'transition_policy': 'Outside query timing; no cache flush. LRU lists are rebuilt by the kernel.'}
path.write_text(json.dumps(report, indent=2) + '\n')
start = time.monotonic()
try:
    if before != target:
        subprocess.run(['sudo', '-n', 'tee', str(setting)], input=str(target) + '\n',
                       text=True, capture_output=True, check=True)
    report['observed'] = int(setting.read_text().strip(), 0)
    assert report['observed'] == target
    report['status'] = 'completed'
except BaseException as error:
    report.update(status='failed', error=repr(error))
    raise
finally:
    report['transition_seconds'] = time.monotonic() - start
    report['end_unix_ns'] = time.time_ns()
    report['memory_after'] = Path('/proc/meminfo').read_text()
    path.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({k: report[k] for k in ('status', 'before', 'target', 'transition_seconds')}))
