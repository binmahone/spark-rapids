#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Read NUMA watermarks and executor kernel stacks during Q12; do not change host policy."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import time


def read(path):
    try:
        return Path(path).read_text()
    except (OSError, ProcessLookupError) as error:
        return 'UNAVAILABLE:' + str(error)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--directory', required=True)
    args = parser.parse_args()
    work, directory = Path(args.work), Path(args.directory)
    owner = json.loads((work / 'evidence/owner.json').read_text())
    controller = json.loads((directory / 'controller.json').read_text())
    assert os.geteuid() == 0
    running = True
    def stop(*unused):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    known, pids, tick = set(), [], 0
    result_path = work / 'screen' / ('q12-diagnosis-' + directory.name + '-0') / 'applications/suite/result.json'
    with (directory / 'threads.jsonl').open('x', buffering=1) as output:
        while running:
            begin = time.monotonic_ns()
            stat = read(f'/proc/{controller["pid"]}/stat')
            if stat.startswith('UNAVAILABLE') or stat.split(') ', 1)[1].split()[19] != str(controller['start_time']):
                break
            if tick % 4 == 0:
                pids = []
                for proc in Path('/proc').iterdir():
                    if not proc.name.isdigit():
                        continue
                    cmd = read(proc / 'cmdline')
                    if 'CoarseGrainedExecutorBackend' in cmd and str(work) in cmd:
                        pids.append(int(proc.name))
            active = None
            if result_path.exists():
                active = json.loads(result_path.read_text()).get('active_query')
            row = {'time_unix_ns': time.time_ns(), 'monotonic_ns': begin,
                   'active_query': active, 'executors': [], 'kernel_threads': []}
            zone = None
            row['zones'] = []
            for line in read('/proc/zoneinfo').splitlines():
                if line.startswith('Node '):
                    zone = {'name': line}; row['zones'].append(zone)
                elif zone is not None and re.match(r'\s*(pages free|boost |min |low |high |managed |protection:|nr_free_pages |nr_zone_inactive_file |nr_zone_active_file )', line):
                    zone.setdefault('values', []).append(line.strip())
            row['cpu_ticks'] = [line for line in read('/proc/stat').splitlines() if line.startswith('cpu')]
            for pid in pids:
                proc = Path('/proc') / str(pid)
                entry = {'pid': pid, 'threads': []}
                if pid not in known:
                    entry['status'] = read(proc / 'status')
                    entry['numa_maps'] = read(proc / 'numa_maps')
                    entry['cgroup'] = read(proc / 'cgroup')
                    known.add(pid)
                if active == 12:
                    for task in (proc / 'task').glob('*'):
                        value = read(task / 'stat')
                        if value.startswith('UNAVAILABLE'):
                            continue
                        name = value.split('(', 1)[1].rsplit(') ', 1)[0]
                        fields = value.rsplit(') ', 1)[1].split()
                        item = {'tid': int(task.name), 'name': name, 'state': fields[0],
                                'utime': int(fields[11]), 'stime': int(fields[12]),
                                'processor': int(fields[36])}
                        if fields[0] in ('R', 'D') or name.startswith(('kvikio-', 'Executor task')):
                            item['stack'] = read(task / 'stack')
                            item['wchan'] = read(task / 'wchan')
                        entry['threads'].append(item)
                row['executors'].append(entry)
            if active == 12:
                for proc in Path('/proc').iterdir():
                    if proc.name.isdigit() and read(proc / 'comm').startswith(('kswapd', 'kcompactd', 'khugepaged')):
                        row['kernel_threads'].append({'pid': int(proc.name), 'stat': read(proc / 'stat'),
                                                      'stack': read(proc / 'stack')})
            row['sampler_seconds'] = (time.monotonic_ns() - begin) / 1e9
            output.write(json.dumps(row, separators=(',', ':')) + '\n')
            tick += 1
            time.sleep(max(0.01, 0.5 - row['sampler_seconds']))
    (directory / 'threads-receipt.json').write_text(json.dumps({
        'thread_id': owner['thread_id'], 'exit': 'clean', 'samples': tick,
        'executor_pids': sorted(known)}) + '\n')


if __name__ == '__main__':
    main()
