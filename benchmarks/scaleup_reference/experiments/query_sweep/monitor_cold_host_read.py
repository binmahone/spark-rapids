#!/usr/bin/env python3
"""Sample the native adapter's persistent per-process counters without GPU polling."""
import argparse
import json
from pathlib import Path
import signal
import struct
import time

NAMES = ['eligible', 'buffered', 'direct', 'direct_bytes', 'cold', 'resident',
         'mixed', 'inspect_fail', 'direct_fail', 'request_bytes']

def snapshot(directory):
    return {p.stem: dict(zip(NAMES, struct.unpack('=10Q', p.read_bytes())))
            for p in directory.glob('*.bin') if p.stat().st_size == 80}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--directory', required=True)
    a = p.parse_args()
    directory = Path(a.directory)
    running = True
    def stop(*unused):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with (directory / 'native.jsonl').open('x', buffering=1) as output:
        while running:
            processes = snapshot(directory / 'native-counters')
            totals = {k: sum(v[k] for v in processes.values()) for k in NAMES}
            output.write(json.dumps({'time_unix_ns': time.time_ns(), 'processes': processes,
                                    'totals': totals}) + '\n')
            time.sleep(1)

if __name__ == '__main__':
    main()
