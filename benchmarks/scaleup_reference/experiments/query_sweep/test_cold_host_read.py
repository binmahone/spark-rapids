#!/usr/bin/env python3
"""Exercise the actual interposed read path on private, immutable test fixtures."""
import argparse
import concurrent.futures
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import struct

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', required=True)
p.add_argument('--expect-direct', action='store_true')
p.add_argument('--expect-fallback', action='store_true')
a = p.parse_args()
root = Path(a.root)
root.mkdir(exist_ok=True)
path = root / 'fixture.parquet'
data = hashlib.shake_256(b'cold-host-read-byte-parity').digest(72 * 1024 * 1024 + 713)
with path.open('xb') as output:
    output.write(data)
    output.flush()
    os.fsync(output.fileno())
fd = os.open(path, os.O_RDONLY)
lib = ctypes.CDLL(None, use_errno=True)
lib.pread64.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_longlong]
lib.pread64.restype = ctypes.c_ssize_t
lib.pread.argtypes = lib.pread64.argtypes
lib.pread.restype = ctypes.c_ssize_t
stats = Path(os.environ['SCALEUP_COLD_READ_STATS']) / (str(os.getpid()) + '.bin')
names = ['eligible', 'buffered', 'direct', 'direct_bytes', 'cold', 'resident',
         'mixed', 'inspect_fail', 'direct_fail', 'request_bytes', 'mixed_direct']

def counters():
    return dict(zip(names, struct.unpack('=11Q', stats.read_bytes())))

def read(offset, size, symbol='pread64', handle=fd):
    assert lib.prctl(15, ctypes.c_char_p(b'kvikio-0'), 0, 0, 0) == 0
    storage = ctypes.create_string_buffer(size + 37)
    destination = ctypes.addressof(storage) + 13
    ctypes.set_errno(errno.EBUSY)
    got = getattr(lib, symbol)(handle, destination, size, offset)
    if handle == -1:
        assert got == -1 and ctypes.get_errno() == errno.EBADF
        return
    expected = data[offset:offset + size]
    assert got == len(expected), (offset, size, got, ctypes.get_errno())
    assert ctypes.string_at(destination, got) == expected, (offset, size)
    assert storage.raw[:13] == bytes(13) and storage.raw[13 + size:] == bytes(24)

try:
    # Eviction is limited to this test fixture; benchmark files are never flushed.
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    before = counters()
    read(17, 2 * 1024 * 1024 + 31)
    after_cold = counters()
    if a.expect_direct:
        assert after_cold['direct'] > before['direct'], after_cold
    if a.expect_fallback:
        assert after_cold['direct_fail'] > before['direct_fail'], after_cold
        assert after_cold['direct'] == 0, after_cold
    os.lseek(fd, 0, os.SEEK_SET)
    os.read(fd, 4 * 1024 * 1024)
    read(123, 2 * 1024 * 1024, 'pread')
    after_hot = counters()
    if a.expect_direct:
        assert after_hot['resident'] > after_cold['resident'], after_hot
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    os.lseek(fd, 8 * 1024 * 1024, os.SEEK_SET)
    os.read(fd, 8192)
    read(8 * 1024 * 1024, 2 * 1024 * 1024)
    after_mixed = counters()
    if a.expect_direct:
        assert after_mixed['mixed'] > after_hot['mixed'], after_mixed
        assert after_mixed['mixed_direct'] > after_hot['mixed_direct'], after_mixed
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(lambda i: read((i + 2) * 2 * 1024 * 1024 + 3,
                                     1024 * 1024 + 127), range(32)))
    for offset, size in [(0, 0), (7, 511), (4095, 8193),
                         (len(data) - 2 * 1024 * 1024, 2 * 1024 * 1024),
                         (len(data) - 100, 2 * 1024 * 1024),
                         (len(data) + 5, 2 * 1024 * 1024)]:
        read(offset, size)
    read(0, 1024 * 1024, handle=-1)
    print(json.dumps({'status': 'passed', 'pid': os.getpid(), 'counters': counters(),
                      'checks': ['cold', 'hot', 'mixed', '32 concurrent readers',
                                 'unaligned offsets and destination', 'both pread symbols',
                                 'zero length', 'small range', 'EOF', 'bad fd', 'buffer guards']}, indent=2))
finally:
    os.close(fd)
    path.unlink()
