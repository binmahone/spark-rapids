#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Separate queue delay and real read-system-call time from bounded native trace records."""
import argparse
from collections import Counter, defaultdict, deque, namedtuple
import csv
import json
import os
from pathlib import Path
import statistics
import struct

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('directory', type=Path)
parser.add_argument('--probe', action='store_true')
parser.add_argument('--summary', type=Path)
args = parser.parse_args()
Record = namedtuple('Record', 'kind call task tid fd offset bytes result buffer begin end cpu flags file')


def stats(values):
    if not values:
        return None
    values = sorted(v / 1e6 for v in values)
    return {'count': len(values), 'mean_ms': statistics.mean(values),
            'p50_ms': statistics.median(values), 'p95_ms': values[int(.95 * (len(values) - 1))]}


def peak(reads):
    events = [(r.begin, 1) for r in reads] + [(r.end, -1) for r in reads]
    active = maximum = 0
    for _, delta in sorted(events):
        active += delta
        maximum = max(maximum, active)
    return maximum


output = []
for path in sorted(args.directory.glob('*.bin')):
    data = path.read_bytes()
    assert data[:8] == b'RAPIDIO1', path
    version, count, dropped, width, mono, real = struct.unpack_from('<6Q', data, 8)
    assert version == 1 and dropped == 0 and width == 112, (path, version, dropped, width)
    assert len(data) == 56 + count * width, path
    records = sorted((Record(*r) for r in struct.iter_unpack('<14Q', data[56:])), key=lambda r: r.begin)
    reads = [r for r in records if r.kind == 6]
    assert reads, path
    if not args.probe:
        assert all(r.result == r.bytes and r.bytes > 0 for r in reads), path
    calls = {r.call: r for r in records if r.kind == 1}
    ends = {r.call: r for r in records if r.kind == 5}
    enqueues = {(r.call, r.task): r for r in records if r.kind == 2}
    starts = {(r.call, r.task): r for r in records if r.kind == 3}
    keys = defaultdict(deque)
    for r in records:
        if r.kind == 2:
            keys[(r.fd, r.buffer, r.offset, r.bytes)].append(r)
    assigned = []
    unmatched = 0
    for r in reads:
        call = r.call
        enqueue = enqueues.get((call, r.task)) if call else None
        if not call:
            candidates = keys[(r.fd, r.buffer, r.offset, r.bytes)]
            if candidates and candidates[0].begin <= r.begin:
                enqueue = candidates.popleft()
                call = enqueue.call
            else:
                unmatched += 1
        if args.probe or not calls or (call in calls and calls[call].bytes > 100_000_000):
            assigned.append((r, call, enqueue))
    selected = [r for r, _, _ in assigned]
    assert selected, path
    if calls and not args.probe:
        assert unmatched == 0, (path, unmatched)
        assert sum(r.result for r in selected) == sum(r.bytes for r in calls.values() if r.bytes > 100_000_000), path
    per_file = defaultdict(list)
    for r, call, _ in assigned:
        per_file[call or r.file].append(r)
    queue = [starts[key].begin - r.begin for key, r in enqueues.items()
             if key in starts and (args.probe or calls[key[0]].bytes > 100_000_000)]
    public_queue = [r.begin - enq.begin for r, _, enq in assigned if enq and not r.call]
    entry = {'file': path.name, 'records': count, 'read_calls': len(selected),
             'read_bytes': sum(r.result for r in selected), 'unmatched_reads': unmatched,
             'direct_io_read_calls': sum(bool(r.flags & os.O_DIRECT) for r in selected),
             'read_wall': stats([r.end - r.begin for r in selected]),
             'read_cpu': stats([r.cpu for r in selected]), 'task_queue': stats(queue),
             'public_submit_to_io_start': stats(public_queue),
             'file_peak_concurrency': dict(Counter(peak(v) for v in per_file.values())),
             'bulk_call_wall': stats([ends[k].begin - r.begin for k, r in calls.items()
                                     if k in ends and (args.probe or r.bytes > 100_000_000)])}
    if args.summary:
        iterations = []
        for row in csv.DictReader(args.summary.open()):
            subset = [r for r in selected if int(row['started_epoch_ns']) <= real + r.begin - mono <= int(row['ended_epoch_ns'])]
            iterations.append({'iteration': int(row['iteration']), 'calls': len(subset),
                               'io_peak': peak(subset), 'read_wall': stats([r.end - r.begin for r in subset]),
                               'average_inflight': sum(r.end - r.begin for r in subset) / (max(r.end for r in subset) - min(r.begin for r in subset)) if subset else None})
        entry['iterations'] = iterations
    output.append(entry)
assert output, 'No tracer output was produced'
target = args.directory / 'analysis.json'
target.write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps(output, indent=2))
