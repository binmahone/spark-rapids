#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Check live read-branch transitions, byte parity, heartbeat expiry, and hysteresis."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import time

from monitor_cold_host_read import snapshot
from pressure_read_control import ControlWord, PressurePolicy

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', required=True, type=Path)
p.add_argument('--expect-fallback', action='store_true')
a = p.parse_args()
control = ControlWord(os.environ['SCALEUP_COLD_READ_PRESSURE'])
policy = PressurePolicy()
assert not policy.update(0, 0, 0)['active']
assert not policy.update(0.25, 100, 20000)['active']
assert policy.update(0.5, 200, 40000)['active']
assert policy.update(0.75, 300, 60000)['active']
assert policy.update(1, 300, 60000)['active']
assert policy.update(2.5, 300, 60000)['active']
assert not policy.update(3, 300, 60000)['active']
assert not policy.update(3.25, 400, 80000)['active']
assert policy.update(3.5, 500, 100000)['active']
# Either signal alone must not activate the production policy.
for stalls, full in ((1000, 0), (0, 100000)):
    alone = PressurePolicy()
    assert not alone.update(0, 0, 0)['active']
    assert not alone.update(1, stalls, full)['active']
    assert not alone.update(2, stalls * 2, full * 2)['active']

data = hashlib.shake_256(b'pressure-host-read-byte-parity').digest(16 * 1024 * 1024)
path = a.root / 'fixture.parquet'
with path.open('xb') as stream:
    stream.write(data)
    stream.flush()
    os.fsync(stream.fileno())
fd = os.open(path, os.O_RDONLY)
lib = ctypes.CDLL(None, use_errno=True)
lib.pread64.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_longlong]
lib.pread64.restype = ctypes.c_ssize_t
assert lib.prctl(15, ctypes.c_char_p(b'kvikio-0'), 0, 0, 0) == 0
stats = Path(os.environ['SCALEUP_COLD_READ_STATS'])

def counts():
    return snapshot(stats)[str(os.getpid())]

def evict():
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)

def read():
    offset, size = 17, 2 * 1024 * 1024 + 31
    buffer = ctypes.create_string_buffer(size + 37)
    pointer = ctypes.addressof(buffer) + 13
    assert lib.pread64(fd, pointer, size, offset) == size
    assert ctypes.string_at(pointer, size) == data[offset:offset + size]
    assert buffer.raw[:13] == bytes(13) and buffer.raw[13 + size:] == bytes(24)

try:
    evict()
    control.publish(False)
    read()
    low = counts()
    assert low['pressure_inactive'] == 1 and low['direct'] == 0
    # Active pressure preserves the buffered path for already resident data.
    control.publish(True)
    read()
    hot = counts()
    assert hot['resident'] > low['resident'] and hot['direct'] == 0
    evict()
    control.publish(policy.active)
    read()
    high = counts()
    assert high['pressure_active'] > hot['pressure_active']
    if a.expect_fallback:
        assert high['direct_fail'] > hot['direct_fail'] and high['direct'] == 0
    else:
        assert high['direct'] > hot['direct'] and high['direct_bytes'] > 0
    assert policy.update(4, 500, 100000)['active']
    assert not policy.update(6, 500, 100000)['active']
    evict()
    control.publish(policy.active)
    read()
    recovered = counts()
    assert recovered['pressure_inactive'] > high['pressure_inactive']
    assert recovered['direct'] == high['direct']
    # A dead controller's heartbeat and an invalid future timestamp both fall back.
    for stamp in (time.monotonic_ns() // 1000000 - 3000,
                  time.monotonic_ns() // 1000000 + 100000, 0):
        evict()
        before = counts()
        control.publish(True, stamp)
        read()
        after = counts()
        assert after['pressure_invalid'] == before['pressure_invalid'] + 1
        assert after['direct'] == before['direct']
    print(json.dumps({'status': 'passed', 'counters': counts(),
                      'checks': ['joint pressure requirement', 'sustained activation',
                                 'minimum hold and recovery hysteresis', 'reactivation',
                                 'real buffered/direct branch transition', 'resident reuse',
                                 'stale/future/zero heartbeat fallback', 'unaligned byte parity',
                                 'destination guards', 'direct error fallback' if a.expect_fallback
                                 else 'successful direct read']}, indent=2))
finally:
    control.publish(False)
    os.close(fd)
    path.unlink()
