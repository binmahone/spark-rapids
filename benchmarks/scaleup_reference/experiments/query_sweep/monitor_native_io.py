#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Capture CPU stacks and CPU accounting of the owned Kvikio read threads."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time


def counters(pid, tid):
    root = Path('/proc') / str(pid) / 'task' / str(tid)
    fields = (root / 'stat').read_text().rsplit(')', 1)[1].split()
    io = dict(line.split(':', 1) for line in (root / 'io').read_text().splitlines())
    return {'pid': pid, 'tid': tid, 'name': (root / 'comm').read_text().strip(),
            'start_time': int(fields[19]), 'state': fields[0],
            'user_ticks': int(fields[11]), 'system_ticks': int(fields[12]),
            'rchar': int(io['rchar']), 'read_bytes': int(io['read_bytes'])}


def discover(work):
    found = {}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            command = (proc / 'cmdline').read_bytes()
            if (proc.stat().st_uid != os.getuid() or work not in command or
                    b'CoarseGrainedExecutorBackend' not in command):
                continue
            threads = []
            for path in (proc / 'task').iterdir():
                if re.fullmatch(r'kvikio-\d+', (path / 'comm').read_text().strip()):
                    threads.append(counters(int(proc.name), int(path.name)))
            found[proc.name] = threads
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--thread-id', required=True)
    parser.add_argument('--controller', type=Path, required=True)
    args = parser.parse_args()
    assert json.loads((args.work / 'evidence/owner.json').read_text())['thread_id'] == args.thread_id
    controller = json.loads(args.controller.read_text())
    assert controller['owner_uid'] == os.getuid()
    running = True

    def stop(*unused):
        nonlocal running
        running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    data = args.scratch / 'native-io.perf.data'
    assert not data.exists()
    receipt = {'status': 'waiting', 'hz': os.sysconf('SC_CLK_TCK'),
               'unix_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
               'controller': controller, 'data': str(data)}
    process = None
    log = None
    targets = []
    started = time.monotonic()
    last = {}
    try:
        with args.output.open('x', buffering=1) as output:
            while running:
                parent = Path('/proc') / str(controller['pid'])
                fields = (parent / 'stat').read_text().rsplit(')', 1)[1].split()
                assert int(fields[19]) == int(controller['start_time']), 'Controller changed'
                assert time.monotonic() - started < 1000, 'Native IO observer timeout'
                if process is None:
                    found = discover(str(args.work.resolve()).encode())
                    if len(found) == 8 and all(len(threads) == 32 for threads in found.values()):
                        targets = [item for threads in found.values() for item in threads]
                        command = ['sudo', '-n', 'perf', 'record', '-e', 'cpu-clock',
                                   '-F', '19', '--call-graph', 'dwarf,4096', '--clockid', 'mono',
                                   '--sample-cpu', '--no-buildid-cache', '--max-size', '2G',
                                   '-t', ','.join(str(item['tid']) for item in targets),
                                   '-o', str(data)]
                        log = args.output.with_suffix('.perf.log').open('x')
                        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                                   start_new_session=True)
                        receipt.update(status='recording', command=command, targets=targets,
                                       perf_group=process.pid, attach_unix_ns=time.time_ns(),
                                       attach_monotonic_ns=time.monotonic_ns())
                    else:
                        assert time.monotonic() - started < 300, 'Eight native pools did not appear'
                current = []
                for target in targets:
                    try:
                        item = counters(target['pid'], target['tid'])
                        assert item['start_time'] == target['start_time'], 'Thread ID reused'
                        last[str(item['tid'])] = item
                        current.append(item)
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                output.write(json.dumps({'unix_ns': time.time_ns(),
                    'monotonic_ns': time.monotonic_ns(), 'threads': current}) + '\n')
                if process is not None and process.poll() is not None:
                    assert process.returncode == 0 and not current, 'Perf ended before target threads'
                args.output.with_suffix('.receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
                time.sleep(0.5)
        assert process is not None, 'No native threads were captured'
        receipt['status'] = 'completed'
    except BaseException as error:
        receipt.update(status='failed', error=repr(error))
        raise
    finally:
        if process is not None:
            if process.poll() is None:
                assert os.getpgid(process.pid) == process.pid
                subprocess.run(['sudo', '-n', 'kill', '-INT', '--', '-' + str(process.pid)], check=True)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    subprocess.run(['sudo', '-n', 'kill', '-TERM', '--', '-' + str(process.pid)], check=True)
                    process.wait(timeout=5)
            receipt['perf_exit'] = process.returncode
            if process.returncode not in (0, 130, -signal.SIGINT):
                receipt['status'] = 'failed'
            if log:
                log.close()
        receipt.update(last_observed_threads=last, finished_unix_ns=time.time_ns(),
                       finished_monotonic_ns=time.monotonic_ns())
        if data.exists():
            receipt['bytes'] = data.stat().st_size
        if receipt.get('bytes', 0) <= 1024:
            receipt['status'] = 'failed'
        args.output.with_suffix('.receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    assert receipt['status'] == 'completed', receipt


if __name__ == '__main__':
    main()
