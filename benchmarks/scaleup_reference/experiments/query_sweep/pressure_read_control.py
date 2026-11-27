#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Publish a bounded-age read policy from measured host reclaim and memory stalls."""
import argparse
import ctypes
import json
import mmap
import os
from pathlib import Path
import re
import signal
import time


class ControlWord:
    def __init__(self, path, create=False):
        flags = os.O_RDWR | (os.O_CREAT | os.O_EXCL if create else 0)
        fd = os.open(path, flags, 0o600)
        try:
            if create:
                os.ftruncate(fd, 8)
            assert os.fstat(fd).st_size == 8
            self.mapping = mmap.mmap(fd, 8)
        finally:
            os.close(fd)
        self.storage = ctypes.c_uint64.from_buffer(self.mapping)
        self.atomic = ctypes.CDLL('libatomic.so.1')
        self.store = getattr(self.atomic, '__atomic_store_8')
        self.store.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_int]
        self.store.restype = None

    def publish(self, active, stamp_ms=None):
        stamp_ms = time.monotonic_ns() // 1000000 if stamp_ms is None else stamp_ms
        self.store(ctypes.addressof(self.storage), (stamp_ms << 1) | int(active), 3)


class PressurePolicy:
    """Require sustained joint pressure, a minimum hold, and sustained recovery."""
    def __init__(self):
        self.previous = None
        self.active = False
        self.high_since = None
        self.low_since = None
        self.enabled_at = None

    def update(self, now, stalls, full_us):
        previous, self.previous = self.previous, (now, stalls, full_us)
        if previous is None:
            return {'active': False, 'reason': 'initial', 'stall_rate': 0, 'full_fraction': 0}
        elapsed = now - previous[0]
        assert elapsed > 0 and stalls >= previous[1] and full_us >= previous[2]
        rate = (stalls - previous[1]) / elapsed
        full = (full_us - previous[2]) / (elapsed * 1000000)
        high = rate >= 128 and full >= 0.02
        low = rate < 32 and full < 0.005
        self.high_since = (now if self.high_since is None else self.high_since) if high else None
        self.low_since = (now if self.low_since is None else self.low_since) if low else None
        reason = 'hold'
        if not self.active and self.high_since is not None and now - self.high_since >= 0.25:
            self.active, self.enabled_at, self.low_since = True, now, None
            reason = 'sustained_pressure'
        elif self.active and self.low_since is not None and now - self.low_since >= 2:
            if now - self.enabled_at >= 2:
                self.active = False
                reason = 'sustained_recovery'
        return {'active': self.active, 'reason': reason, 'stall_rate': rate, 'full_fraction': full}


def read_pressure():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    stalls = sum(int(v) for k, v in vm.items() if k.startswith('allocstall_'))
    text = Path('/proc/pressure/memory').read_text()
    full_us = int(re.search(r'^full.*total=(\d+)', text, re.M)[1])
    return stalls, full_us


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--directory', required=True, type=Path)
    a = p.parse_args()
    control = ControlWord(a.directory / 'pressure-control.bin', create=True)
    policy = PressurePolicy()
    running = True
    def stop(*unused):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with (a.directory / 'pressure-policy.jsonl').open('x', buffering=1) as output:
        try:
            while running:
                before = time.monotonic()
                stalls, full_us = read_pressure()
                result = policy.update(time.monotonic(), stalls, full_us)
                control.publish(result['active'])
                output.write(json.dumps({'time_unix_ns': time.time_ns(),
                                         'monotonic_ns': time.monotonic_ns(),
                                         'allocstall': stalls, 'full_us': full_us, **result}) + '\n')
                if not (a.directory / 'pressure-ready.json').exists():
                    (a.directory / 'pressure-ready.json').write_text(json.dumps({
                        'pid': os.getpid(), 'sample_seconds': 0.25, 'heartbeat_ttl_ms': 2000,
                        'on_stalls_per_second': 128, 'on_full_fraction': 0.02,
                        'on_sustained_seconds': 0.25, 'off_stalls_per_second': 32,
                        'off_full_fraction': 0.005, 'off_sustained_seconds': 2,
                        'minimum_hold_seconds': 2}) + '\n')
                time.sleep(max(0, 0.25 - (time.monotonic() - before)))
        finally:
            control.publish(False)


if __name__ == '__main__':
    main()
