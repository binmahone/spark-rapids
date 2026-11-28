#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure peer-transfer topology and copy activity in validated SQL windows."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from analyze_critical_path import schedule
from analyze_data_path_trace import merged, duration

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace', type=Path, required=True)
p.add_argument('--query', type=int, required=True)
a = p.parse_args()
state = json.loads((a.trace / 'state.json').read_text())
assert state['status'] == 'completed' and state.get('full_sql_trace', True)
assert all(s['validation']['status'] == 'passed' for s in state['result']['samples'])
db = sqlite3.connect('file:' + str(a.trace / f'q{a.query}.sqlite') + '?mode=ro', uri=True)
epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
devices = {}
for index, bus, uuid in db.execute('select id,busLocation,uuid from TARGET_INFO_GPU'):
    node = int((Path('/sys/bus/pci/devices') / bus / 'numa_node').read_text())
    assert node >= 0
    devices[index] = dict(bus=bus, uuid=uuid, numa=node)
assert len(devices) == 8
kinds = dict(db.execute('select id,label from ENUM_CUDA_MEMCPY_OPER'))
timeline = schedule(Path(state['run']))[f'q{a.query}-iteration2']
sample = next(s for s in state['result']['samples'] if s['iteration'] == 2)
windows = [('whole-query', sample['started_unix_ns']-epoch, sample['finished_unix_ns']-epoch)]
# Merge overlapping stages so a group never counts the same interval twice.
groups = []
for stage in sorted(timeline['stages'], key=lambda s: s['start_seconds']):
    if stage['span_seconds'] < 1:
        continue
    start = timeline['window']['start'] * 1000000 + round(stage['start_seconds']*1e9)-epoch
    end = timeline['window']['start'] * 1000000 + round(stage['end_seconds']*1e9)-epoch
    if groups and start < groups[-1][2]:
        groups[-1][0].append(stage['stage'])
        groups[-1][2] = max(groups[-1][2], end)
    else:
        groups.append([[stage['stage']], start, end])
windows += [('stages-' + '-'.join(map(str, ids)), start, end) for ids, start, end in groups]
result = {'devices': devices, 'windows': [], 'limits': [
    'This is a profiled application; durations are not unprofiled benchmark scores.',
    'Stage groups are interval associations, not exclusive attribution of copies to operators.',
    'Bytes include whole copies intersecting each window; activity is clipped to the window.',
    'Whole-query and stage-group rows overlap and must not be added together.',
    'Summed copy durations include concurrency and are not query wall time.',
    'NUMA placement is read from the same host PCI addresses; GPU IDs come from the trace.',
    'CUDA activity device indicates the recording context; source and destination identify direction.'
]}
for label, begin, end in windows:
    grouped = defaultdict(lambda: {'bytes': 0, 'count': 0, 'intervals': [], 'streams': set()})
    for start, stop, gpu, pid, stream, size, kind, src, dst in db.execute(
            'select start,end,deviceId,globalPid,streamId,bytes,copyKind,srcDeviceId,dstDeviceId '
            'from CUPTI_ACTIVITY_KIND_MEMCPY where end>? and start<?', (begin, end)):
        name = kinds[kind]
        if name not in ('P2P', 'D2D'):
            continue
        topology = 'unknown'
        if src in devices and dst in devices:
            topology = ('same-gpu' if src == dst else 'within-numa'
                        if devices[src]['numa'] == devices[dst]['numa'] else 'across-numa')
        key = (name, topology, src, dst, gpu)
        row = grouped[key]
        row['bytes'] += size
        row['count'] += 1
        row['intervals'].append((max(begin,start), min(end,stop)))
        row['streams'].add((pid, stream))
    rows = []
    for (kind, topology, src, dst, gpu), row in sorted(grouped.items()):
        rows.append(dict(kind=kind, topology=topology, source_device=src, destination_device=dst,
            activity_device=gpu, bytes=row['bytes'], count=row['count'],
            streams=len(row['streams']), active_seconds=duration(merged(row['intervals'])),
            summed_seconds=sum(stop-start for start,stop in row['intervals'])/1e9))
    summary = defaultdict(lambda: {'bytes': 0, 'copies': 0, 'summed_seconds': 0.0})
    for row in rows:
        item = summary[row['kind'] + ':' + row['topology']]
        item['bytes'] += row['bytes']
        item['copies'] += row['count']
        item['summed_seconds'] += row['summed_seconds']
    result['windows'].append(dict(label=label, wall_seconds=(end-begin)/1e9,
                                  summary=dict(summary), pairs=rows))
    print(json.dumps(result['windows'][-1] | {'pairs': len(rows)}), flush=True)
db.close()
(a.trace/'shuffle-fabric-analysis.json').write_text(json.dumps(result, indent=2)+'\n')
