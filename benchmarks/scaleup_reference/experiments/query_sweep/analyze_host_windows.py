#!/usr/bin/env python3
"""Correlate completed SQL execution windows with sampled host state, without assigning causality."""
import argparse
from bisect import bisect_left, bisect_right
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
parser.add_argument('--disk-device', required=True,
                    help='Verified Linux diskstats device backing the dataset mount, such as dm-0')
args = parser.parse_args()
work = Path(args.work)
analysis = json.loads((work / 'evidence/sweep-analysis.json').read_text())
records = []
for path in sorted((work / 'evidence').glob('ceiling-*-host-monitor.jsonl')):
    with path.open() as stream:
        for line in stream:
            if line.endswith('\n'):
                records.append(json.loads(line))
records.sort(key=lambda record: record['time_unix_ns'])
times = [record['time_unix_ns'] for record in records]
rows = []
for group in analysis['groups']:
    for sample in group['samples']:
        start, end = (sample['execution_window_ms'][key] * 1_000_000 for key in ('start', 'end'))
        selected = records[bisect_left(times, start):bisect_right(times, end)]
        if len(selected) < 2:
            raise RuntimeError('No host sampling coverage for ' + group['run'])
        first, last = selected[0], selected[-1]
        before = {item['pid']: item for item in first['executors']}
        after = {item['pid']: item for item in last['executors']}
        common = before.keys() & after.keys()
        device = args.disk_device
        assert device in first['diskstats'] and device in last['diskstats'], device
        gpus = [gpu for record in selected for gpu in record['gpus']]
        busy = [gpu for gpu in gpus if gpu.get('util_gpu', 0) >= 50]
        delta = {key: last['vmstat'][key] - first['vmstat'][key] for key in first['vmstat']}
        rows.append({'run': group['run'], 'query': sample['query'], 'iteration': sample['iteration'],
            'sql_seconds': sample['sql_seconds'], 'execution_window_ms': sample['execution_window_ms'],
            'sampled_seconds': (last['time_unix_ns'] - first['time_unix_ns']) / 1e9,
            'sampling_boundary_gap_seconds': ((first['time_unix_ns'] - start) + (end - last['time_unix_ns'])) / 1e9,
            'executor_pids_covered': sorted(common),
            'executor_physical_read_bytes': sum(after[pid]['io'].get('read_bytes', 0) -
                before[pid]['io'].get('read_bytes', 0) for pid in common),
            'dataset_block_device': device,
            'whole_device_read_bytes': (last['diskstats'][device][2] - first['diskstats'][device][2]) * 512,
            'vmstat_delta': delta,
            'min_mem_available_kib': min(record['memory_kib']['MemAvailable'] for record in selected),
            'gpu_max_temperature_c': max(gpu['temperature_c'] for gpu in gpus),
            'busy_gpu_throttle_masks': sorted({gpu['throttle_reasons'] for gpu in busy}),
            'busy_sm_clock_mhz_min': min((gpu['sm_clock_mhz'] for gpu in busy), default=None),
            'busy_sm_clock_mhz_max': max((gpu['sm_clock_mhz'] for gpu in busy), default=None)})
report = {'classification': 'Observed host correlation, not causal attribution', 'samples': rows,
    'limits': ['One-second sampling excludes incomplete boundary intervals',
               'SQL execution windows exclude some client planning and post-query validation',
               'Host disk and memory counters may include activity outside the Spark executors',
               'No physical disk cache flush; native reader bytes are not physical disk bytes']}
output = work / 'evidence/ceiling-host-correlation.json'
output.write_text(json.dumps(report, indent=2) + '\n')
print('Correlated', len(rows), 'completed query actions')
