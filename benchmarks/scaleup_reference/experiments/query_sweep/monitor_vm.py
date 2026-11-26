#!/usr/bin/env python3
"""Sample VM allocation and NUMA counters until the recorded experiment controller exits."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
parser.add_argument('--thread-id', required=True)
parser.add_argument('--controller', default='ceiling/controller.json')
parser.add_argument('--output', default='evidence/ceiling-vm-monitor.jsonl')
args = parser.parse_args()
work = Path(args.work)
assert json.loads((work / 'evidence/owner.json').read_text())['thread_id'] == args.thread_id
controller = json.loads((work / args.controller).read_text())


def start_time(pid):
    return Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[19]


def active():
    try:
        return (Path(f'/proc/{controller["pid"]}').stat().st_uid == controller['owner_uid']
                and start_time(controller['pid']) == controller['start_time'])
    except FileNotFoundError:
        return False


assert active(), 'The recorded controller has already exited'
output = work / args.output
output.parent.mkdir(parents=True, exist_ok=True)
assert not output.exists()
settings = ['/proc/sys/vm/zone_reclaim_mode', '/proc/sys/vm/min_free_kbytes',
    '/proc/sys/vm/watermark_scale_factor', '/proc/sys/vm/watermark_boost_factor',
    '/sys/kernel/mm/transparent_hugepage/enabled', '/sys/kernel/mm/transparent_hugepage/defrag']
receipt = {'pid': os.getpid(), 'start_time': start_time(os.getpid()), 'thread_id': args.thread_id,
    'controller': controller, 'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'settings': {path: Path(path).read_text().strip() for path in settings},
    'policy': 'Read-only one-second sampler; exits when the owned controller exits'}
output.with_name(output.stem + '-owner.json').write_text(json.dumps(receipt, indent=2) + '\n')
running = True


def stop(*unused):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
with output.open('x', buffering=1) as stream:
    while running and active():
        vm = {line.split()[0]: int(line.split()[1]) for line in Path('/proc/vmstat').read_text().splitlines()
              if line.startswith(('allocstall', 'compact_', 'thp_', 'pgscan_', 'pgsteal_', 'workingset_'))}
        nodes = {}
        for path in Path('/sys/devices/system/node').glob('node*/meminfo'):
            values = {}
            for line in path.read_text().splitlines():
                parts = line.split()
                key = parts[2].rstrip(':')
                if key in ('MemFree', 'FilePages', 'AnonPages', 'AnonHugePages', 'Dirty', 'Writeback'):
                    values[key] = int(parts[3])
            nodes[path.parent.name] = values
        stream.write(json.dumps({'time_unix_ns': time.time_ns(), 'vmstat': vm,
                                 'node_memory_kib': nodes,
                                 'watermark_scale_factor': int(Path('/proc/sys/vm/watermark_scale_factor').read_text())},
                                separators=(',', ':')) + '\n')
        time.sleep(1)
output.with_suffix('.exit').write_text('0\n')
