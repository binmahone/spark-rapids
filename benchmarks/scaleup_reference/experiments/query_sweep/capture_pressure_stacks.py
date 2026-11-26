#!/usr/bin/env python3
"""Capture bounded, read-only executor kernel stacks during host memory stalls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
parser.add_argument('--thread-id', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
assert json.loads((work / 'evidence/owner.json').read_text())['thread_id'] == args.thread_id
controller = json.loads((work / 'ceiling/controller.json').read_text())


def start_time(pid):
    return Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[19]


def active():
    try:
        return (Path(f'/proc/{controller["pid"]}').stat().st_uid == controller['owner_uid']
                and start_time(controller['pid']) == controller['start_time'])
    except (FileNotFoundError, ProcessLookupError):
        return False


assert active()
output = work / 'evidence/ceiling-pressure-stacks.jsonl'
assert not output.exists()
subprocess.run(['sudo', '-n', 'true'], check=True)
receipt = {'pid': os.getpid(), 'start_time': start_time(os.getpid()), 'thread_id': args.thread_id,
    'controller': controller, 'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'policy': 'At most six captures, at least 60 seconds apart; only owned executor D-state threads',
    'trigger': 'At least 250000 microseconds of memory full PSI increase between one-second polls'}
(work / 'evidence/ceiling-pressure-stacks-owner.json').write_text(json.dumps(receipt, indent=2) + '\n')
running = True


def stop(*unused):
    global running
    running = False


def pressure():
    value = Path('/proc/pressure/memory').read_text()
    return int(re.search(r'^full .*total=(\d+)', value, re.M)[1])


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
previous, last_capture, captures = pressure(), 0., 0
with output.open('x', buffering=1) as stream:
    while running and active() and captures < 6:
        time.sleep(1)
        current = pressure()
        delta, previous = current - previous, current
        if delta < 250000 or time.monotonic() - last_capture < 60:
            continue
        stamp, blocked = time.time_ns(), []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                if proc.stat().st_uid != controller['owner_uid']:
                    continue
                command = (proc / 'cmdline').read_bytes()
                if b'CoarseGrainedExecutorBackend' not in command or str(work).encode() not in command:
                    continue
                process_start = start_time(proc.name)
                for task in (proc / 'task').iterdir():
                    stat = (task / 'stat').read_text()
                    if stat.split(') ', 1)[1].split()[0] != 'D':
                        continue
                    entry = {'pid': int(proc.name), 'tid': int(task.name),
                             'name': (task / 'comm').read_text().strip(),
                             'wchan': (task / 'wchan').read_text().strip(), 'stat': stat.strip()}
                    if len(blocked) < 12 and start_time(proc.name) == process_start:
                        result = subprocess.run(['sudo', '-n', 'cat', str(task / 'stack')],
                            text=True, capture_output=True, timeout=3)
                        entry.update(kernel_stack=result.stdout, stack_error=result.stderr,
                                     stack_exit=result.returncode)
                    blocked.append(entry)
            except (FileNotFoundError, ProcessLookupError, PermissionError, subprocess.TimeoutExpired):
                continue
        apps = sorted(work.glob('screen/*/applications/suite/result.json'), key=lambda p: p.stat().st_mtime)
        state = json.loads(apps[-1].read_text()) if apps else {}
        stream.write(json.dumps({'time_unix_ns': stamp, 'psi_full_delta_us': delta,
            'capture_seconds': (time.time_ns() - stamp) / 1e9,
            'application_result': str(apps[-1]) if apps else None,
            'query': state.get('active_query'), 'completed_actions': len(state.get('samples', [])),
            'blocked_executor_threads': blocked}, separators=(',', ':')) + '\n')
        captures += 1
        last_capture = time.monotonic()
(work / 'evidence/ceiling-pressure-stacks.exit').write_text('0\n')
