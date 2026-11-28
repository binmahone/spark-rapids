#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Align native read-thread CPU accounting and stack samples with SQL stages."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess

from analyze_critical_path import schedule, pressure

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--directory', type=Path, required=True)
args = parser.parse_args()
directory = args.directory
state = json.loads((directory / 'state.json').read_text())
assert state['status'] == 'completed'
receipt = state['native_io']
assert receipt['status'] == 'completed' and len(receipt['targets']) == 256
timeline = schedule(Path(state['run']))
(directory / 'stage-schedule.json').write_text(json.dumps(timeline, indent=2) + '\n')
rows = [json.loads(line) for line in (directory / 'native-io.jsonl').open()]
windows = {}
for token, query in timeline.items():
    start = query['window']['start'] * 1000000
    end = query['window']['end'] * 1000000
    windows[token] = (start, end)
    for stage in query['stages']:
        if stage['span_seconds'] >= 1:
            windows[token + '-stage' + str(stage['stage'])] = (
                start + round(stage['start_seconds'] * 1e9),
                start + round(stage['end_seconds'] * 1e9))
report = {'limits': ['Stage windows can overlap; CPU samples are not query wall time.',
                    'Accounting uses the inner half-second snapshots; boundary fragments are omitted.',
                    'A stack hotspot does not establish the achievable query speedup.'], 'windows': {}}
for name, (start, end) in windows.items():
    selected = [r for r in rows if start <= r['unix_ns'] <= end and len(r['threads']) == 256]
    assert len(selected) >= 2, name
    before = {r['tid']: r for r in selected[0]['threads']}
    delta = Counter()
    for after in selected[-1]['threads']:
        previous = before[after['tid']]
        for key in ('user_ticks', 'system_ticks', 'rchar', 'read_bytes'):
            delta[key] += after[key] - previous[key]
    seconds = (selected[-1]['unix_ns'] - selected[0]['unix_ns']) / 1e9
    report['windows'][name] = {'span_seconds': (end - start) / 1e9,
        'sampled_seconds': seconds, 'user_cpu_seconds': delta['user_ticks'] / receipt['hz'],
        'system_cpu_seconds': delta['system_ticks'] / receipt['hz'],
        'mean_busy_cpu_cores': (delta['user_ticks'] + delta['system_ticks']) / receipt['hz'] / seconds,
        'read_requested_bytes': delta['rchar'], 'storage_read_bytes': delta['read_bytes'],
        'samples': 0, 'period_ns': 0, 'leaf_period_ns': Counter(), 'stack_period_ns': Counter()}
    if '-stage' not in name:
        report['windows'][name]['host_pressure'] = pressure(directory, timeline[name]['window'])
symbols = directory / 'kernel-symbols.txt'
with symbols.open('w') as output:
    subprocess.run(['sudo', '-n', 'cat', '/proc/kallsyms'], stdout=output, check=True)
command = ['sudo', '-n', 'perf', 'script', '--ns', '--kallsyms', str(symbols),
           '-i', receipt['data'], '-F', 'comm,pid,tid,time,period,event,ip,sym,dso']
offset = receipt['attach_unix_ns'] - receipt['attach_monotonic_ns']
header = re.compile(r'^\s*\S+\s+\d+/?\s*\d*\s+(\d+\.\d+):\s+(\d+)\s+cpu-clock:')
current = None
stack = []

def flush():
    if current is None or not stack:
        return
    timestamp, period = current
    for name, (start, end) in windows.items():
        if start <= timestamp < end:
            row = report['windows'][name]
            row['samples'] += 1
            row['period_ns'] += period
            row['leaf_period_ns'][stack[0]] += period
            row['stack_period_ns'][';'.join(stack)] += period

with (directory / 'perf-script.err').open('w') as error:
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error, text=True)
    for line in process.stdout:
        match = header.match(line)
        if match:
            flush()
            current = (round(float(match[1]) * 1e9) + offset, int(match[2]))
            stack = []
        elif line.startswith('\t') and line.strip():
            fields = line.strip().split(maxsplit=1)
            if len(fields) == 2:
                stack.append(fields[1])
    flush()
    assert process.wait() == 0
for name, row in report['windows'].items():
    assert row['samples'] > 0, (name, 'No parsed samples')
    for key in ('leaf_period_ns', 'stack_period_ns'):
        row[key] = row[key].most_common(30)
    print(name, {k: v for k, v in row.items() if k not in ('leaf_period_ns', 'stack_period_ns')}, flush=True)
    print('Top leaves', row['leaf_period_ns'][:6], flush=True)
(directory / 'native-io-analysis.json').write_text(json.dumps(report, indent=2) + '\n')
