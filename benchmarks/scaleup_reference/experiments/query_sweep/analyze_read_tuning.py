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
"""Correlate closed query windows with sampled kernel stacks and GPM counters."""
import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from analyze_sweep import summarize


def records(path):
    return [json.loads(line) for line in path.open()]


def distribution(values):
    values = sorted(values)
    if not values:
        return None
    return {'mean': statistics.mean(values), 'median': statistics.median(values),
            'p95': values[min(len(values) - 1, int(len(values) * .95))], 'max': values[-1]}


def perf_summary(path, windows, executor_pids, offset_ns):
    fields = 'comm,pid,tid,time,event,period,ip,sym,dso'
    if path.stem == 'reclaim':
        fields += ',trace'
    command = ['sudo', '-n', 'perf', 'script', '-i', str(path), '-F', fields,
               '--ns', '--max-stack', '64']
    summaries = {name: {'samples': 0, 'nonidle_samples': 0, 'executor_samples': 0,
                        'sample_period_sum': 0, 'leaf': Counter(), 'inclusive': Counter(),
                        'comm': Counter(), 'stacks': Counter(), 'trace': Counter(),
                        'executor_leaf': Counter(), 'executor_inclusive': Counter()}
                 for name, _, _ in windows}
    header = re.compile(r'^(.*?)\s+(\d+)/(\d+)\s+(\d+)\.(\d+):\s+(\d+)\s+(.*)$')
    sample = None
    def consume(value):
        if not value:
            return
        stamp, comm, pid, period, trace, stack = value
        for name, begin, end in windows:
            if begin <= stamp <= end:
                target = summaries[name]
                target['samples'] += 1
                if comm.startswith('swapper'):
                    return
                target['nonidle_samples'] += 1
                target['sample_period_sum'] += period
                target['comm'][comm] += 1
                target['trace'][trace] += 1
                target['stacks'][';'.join(stack[:32])] += 1
                if stack:
                    target['leaf'][stack[0]] += 1
                target['inclusive'].update(set(stack))
                if pid in executor_pids:
                    target['executor_samples'] += 1
                    if stack:
                        target['executor_leaf'][stack[0]] += 1
                    target['executor_inclusive'].update(set(stack))
                return
    with path.with_suffix('.decode.log').open('w') as error:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error, text=True)
        for line in process.stdout:
            match = header.match(line)
            if match:
                consume(sample)
                comm, pid, tid, sec, fraction, period, trace = match.groups()
                stamp = int(sec) * 10**9 + int(fraction.ljust(9, '0')) + offset_ns
                sample = (stamp, comm.strip(), int(pid), int(period), trace.strip(), [])
            elif line.startswith('\t') and sample:
                parts = line.strip().split(' ', 1)
                if len(parts) == 2:
                    sample[-1].append(parts[1])
        consume(sample)
        assert process.wait() == 0
    for value in summaries.values():
        for key in ['leaf', 'inclusive', 'comm', 'stacks', 'trace', 'executor_leaf', 'executor_inclusive']:
            value[key] = value[key].most_common(30 if key != 'stacks' else 12)
    return summaries


def stage_windows(run):
    profile = read(run / 'applications/suite/profile.json')
    root = Path(urlparse(profile['spark.eventLog.dir']).path)
    executions, roots, tasks = {}, defaultdict(set), defaultdict(list)
    for path in root.iterdir():
        if not path.is_file() or path.name.startswith('.'):
            continue
        assert not path.name.endswith('.inprogress'), 'Only closed event logs are eligible'
        for line in path.open():
            e = json.loads(line)
            kind = e.get('Event', '')
            if kind.endswith('SparkListenerSQLExecutionStart'):
                if re.fullmatch(r'q\d+-iteration\d+', e.get('description', '')):
                    executions[str(e['executionId'])] = e['description']
            elif kind == 'SparkListenerJobStart':
                execution = (e.get('Properties') or {}).get('spark.sql.execution.root.id')
                if execution is not None:
                    for stage in e['Stage IDs']:
                        roots[stage].add(str(execution))
            elif kind == 'SparkListenerTaskEnd':
                info, metrics = e['Task Info'], e.get('Task Metrics') or {}
                tasks[e['Stage ID']].append({
                    'start': info['Launch Time'], 'end': info['Finish Time'],
                    'failed': bool(info.get('Failed') or info.get('Killed')),
                    'input_bytes': (metrics.get('Input Metrics') or {}).get('Bytes Read', 0),
                    'shuffle_write_bytes': (metrics.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written', 0)})
    result = defaultdict(list)
    for stage, values in tasks.items():
        tokens = {executions[x] for x in roots[stage] if x in executions}
        if not tokens:
            continue
        assert len(tokens) == 1
        begin, end = min(v['start'] for v in values), max(v['end'] for v in values)
        result[next(iter(tokens))].append({'stage': stage, 'start_ms': begin, 'end_ms': end,
            'span_seconds': (end - begin) / 1000, 'tasks': len(values),
            'task_seconds': distribution([(v['end'] - v['start']) / 1000 for v in values]),
            'failed_or_killed': sum(v['failed'] for v in values),
            'input_bytes': sum(v['input_bytes'] for v in values),
            'shuffle_write_bytes': sum(v['shuffle_write_bytes'] for v in values)})
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True)
    p.add_argument('--phase', choices=['A0', 'B1', 'B2', 'A3', 'C1', 'suite'], required=True)
    a = p.parse_args()
    work = Path(a.work)
    directory = work / 'read-tuning' / a.phase
    state = read(directory / 'state.json')
    assert state['status'] == 'completed' and state['gpm_restored']
    assert (work / 'evidence' / ('read-tuning-' + a.phase + '.exit')).read_text().strip() == '0'
    controller = read(directory / 'controller.json')
    proc = Path('/proc') / str(controller['pid']) / 'stat'
    assert not proc.exists() or proc.read_text().split(') ', 1)[1].split()[19] != str(controller['start_time'])
    host, vm, gpm = [records(directory / (name + '.jsonl')) for name in ('host', 'vm', 'gpm')]
    offset = int(statistics.median(row['time_unix_ns'] - row['monotonic_ns'] for row in host))
    times = [row['time_unix_ns'] for row in host]
    data = summarize(work, Path(state['groups'][0]['run']))
    raw = read(Path(state['groups'][0]['run']) / 'applications/suite/result.json')
    original = {(s['query'], s['iteration']): s for s in raw['samples']}
    stages = stage_windows(Path(state['groups'][0]['run']))
    windows = []
    for s in data['samples']:
        s['normalized_plan_sha256'] = hashlib.sha256(s.pop('normalized_plan').encode()).hexdigest()
        window = s['execution_window_ms']
        begin, end = window['start'] * 10**6, window['end'] * 10**6
        token = f"q{s['query']}-iteration{s['iteration']}"
        windows.append((token, begin, end))
        h = host[bisect_left(times, begin):bisect_right(times, end)]
        assert len(h) >= 2
        before, after = h[0], h[-1]
        s['host_sampled_seconds'] = (after['time_unix_ns'] - before['time_unix_ns']) / 1e9
        s['host_boundary_gap_seconds'] = (end - begin) / 1e9 - s['host_sampled_seconds']
        disks = {k: after['diskstats'][k][2] - before['diskstats'][k][2] for k in before['diskstats']}
        s['disk_read_bytes'] = {k: v * 512 for k, v in disks.items()}
        def pressure(row):
            return int(re.search(r'^full.*total=(\d+)', row['pressure_memory'], re.M)[1])
        s['memory_full_seconds'] = (pressure(after) - pressure(before)) / 1e6
        cpu = [b - a for a, b in zip(before['cpu_ticks'][:8], after['cpu_ticks'][:8])]
        s['cpu_percent'] = dict(zip(['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal'],
                                   [100 * v / sum(cpu) for v in cpu]))
        s['min_free_gib'] = min(r['memory_kib']['MemFree'] for r in h) / 2**20
        v = [r for r in vm if begin <= r['time_unix_ns'] <= end]
        assert len(v) >= 2
        s['vm_deltas'] = {k: v[-1]['vmstat'][k] - v[0]['vmstat'][k] for k in v[0]['vmstat']}
        s['min_node_free_gib'] = {k: min(r['node_memory_kib'][k]['MemFree'] for r in v) / 2**20
                                  for k in v[0]['node_memory_kib']}
        g = [r for r in gpm if begin <= r['start_unix_ns'] and r['end_unix_ns'] <= end]
        assert len(g) >= 2
        s['gpm_samples'] = len(g)
        s['gpm_sampled_seconds'] = sum(r['end_unix_ns'] - r['start_unix_ns'] for r in g) / 1e9
        metrics = ['sm_active_percent', 'sm_occupancy_percent', 'dram_bandwidth_percent',
                   'pcie_tx_mib_per_second', 'pcie_rx_mib_per_second']
        s['gpm_by_gpu'] = [{name: distribution([r['gpus'][i][name] for r in g if r['gpus'][i][name] is not None])
                            for name in metrics} for i in range(8)]
        s['gpm_all_gpu_mean'] = {name: statistics.mean(gpu[name]['mean'] for gpu in s['gpm_by_gpu']) for name in metrics}
        s['stages'] = stages[token]
        for stage in s['stages']:
            stage_rows = [r for r in gpm if stage['start_ms'] * 10**6 <= r['start_unix_ns']
                          and r['end_unix_ns'] <= stage['end_ms'] * 10**6]
            stage['gpm_samples'] = len(stage_rows)
            if stage_rows:
                stage['gpm_by_gpu'] = [{name: distribution([r['gpus'][i][name] for r in stage_rows
                                            if r['gpus'][i][name] is not None]) for name in metrics}
                                      for i in range(8)]
        s['gpm_link_states'] = sorted({(gpu['pcie_generation'], gpu['pcie_width']) for r in g for gpu in r['gpus']})
        nodes = original[(s['query'], s['iteration'])]['plan_metrics']
        scans = [n for n in nodes if n['node'].startswith('GpuScan')]
        s['scan_task_seconds'] = {name: sum(n['metrics'].get(name, 0) for n in scans) / 1e9
                                  for name in ['bufferTime', 'gpuDecodeTime', 'opTimeNew']}
    pids = {e['pid'] for h in host for e in h['executors']}
    perf = {}
    for name in ('cpu', 'reclaim'):
        path = directory / (name + '.data')
        if path.exists():
            perf[name] = perf_summary(path, windows, pids, offset)
    result = {'phase': a.phase, 'kvikio_threads': state['kvikio_threads'], 'pool_verified': state['pool_verification']['passed'], 'samples': data['samples'], 'perf': perf,
              'source_revision': state['source_revision'], 'perf_monotonic_to_unix_offset_ns': offset,
              'limits': ['Identical host, VM, GPM and pool monitoring in all arms; no kernel profiling',
                         'Fresh applications and fixed query order do not guarantee identical page-cache contents',
                         'GPM percentages and throughput are half-second interval observations',
                         'Whole-query averages do not establish a critical-stage hardware ceiling',
                         'CPU memory-controller counters are unavailable in the exposed PMUs']}
    write(directory / 'analysis.json', result)
    print(json.dumps([{'q':s['query'],'iteration':s['iteration'],'sql':s['sql_seconds'],
                      'read_gb':s['disk_read_bytes'].get('dm-0', 0)/1e9,
                      'allocstall':sum(v for k,v in s['vm_deltas'].items() if k.startswith('allocstall')),
                      'gpu':s['gpm_all_gpu_mean']} for s in data['samples']], indent=2))


if __name__ == '__main__':
    main()
