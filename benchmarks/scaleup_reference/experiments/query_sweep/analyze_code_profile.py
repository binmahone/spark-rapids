#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Crop mixed-stack profiles to SQL windows and retain thread and frame counts."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--label', required=True)
a = p.parse_args()
directory = a.work / 'code-profile' / a.label
state = json.loads((directory / 'state.json').read_text())
assert state['status'] == 'completed' and state['mode'] == 'stacks'
profiles = [r['path'] for r in state['profiles']]
output = directory / 'analysis'
output.mkdir(exist_ok=True)
summary = {'limits': ['Sample counts are not a decomposition of SQL wall time.',
    'Inclusive frames overlap; idle background threads must be separated from task critical paths.',
    'Thread names may reflect later tasks; stage attribution requires timestamps and Spark events.',
    'CPU and wall samples are separate populations and cannot be added.'], 'windows': []}
for sample in state['result']['samples']:
    window = {'query': sample['query'], 'iteration': sample['iteration'], 'sql_seconds': sample['sql_seconds']}
    for mode in ('cpu', 'wall'):
        path = output / f"q{sample['query']}-iteration{sample['iteration']}-{mode}.collapsed"
        per_process = path.with_suffix('')
        per_process.mkdir(exist_ok=True)
        command = [str(a.work / 'jdk17/bin/java'), '-jar', str(a.work / 'profilers/jfr-converter.jar'),
            '--' + mode, '--threads', '--lines', '--from', str(sample['started_unix_ns'] // 1000000),
            '--to', str(sample['finished_unix_ns'] // 1000000), '-o', 'collapsed', *profiles, str(per_process)]
        with path.with_suffix('.log').open('w') as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        coverage = {}
        with path.open('w') as merged:
            for profile in profiles:
                source = per_process / (Path(profile).stem + '.collapsed')
                pid = Path(profile).stem.removeprefix('profile-')
                count = 0
                for line in source.read_text().splitlines():
                    count += int(line.rsplit(' ', 1)[1])
                    merged.write('[pid=' + pid + '] ' + line + '\n')
                assert count > 0, 'No samples from PID ' + pid
                coverage[pid] = count
        assert len(coverage) == len(profiles) == 9, 'Missing process profiles'
        threads, leaves, task_frames, task_leaves, all_frames = (Counter() for _ in range(5))
        domain_frames = Counter()
        total = task_total = 0
        for line in path.read_text().splitlines():
            stack, count = line.rsplit(' ', 1)
            count = int(count)
            frames = stack.split(';')
            total += count
            threads[frames[0]] += count
            leaves[frames[-1]] += count
            for frame in set(frames[1:]):
                all_frames[frame] += count
            for frame in set(re.sub(r'(?::\d+)?_\[[^]]+\]$', '', f) for f in frames[1:]):
                if (any(term in frame for term in ('com/nvidia/', 'ai/rapids/', 'cudf::', 'kvikio::', 'rmm::', 'ucp_', 'uct_', 'ucs_'))
                    and not any(term in frame for term in ('Lambda', '$anonfun', 'GpuMetric', 'Nvtx', '/Arm$', 'GpuOpTimeTracking'))):
                    domain_frames[frame] += count
            if 'org/apache/spark/executor/Executor$TaskRunner.run' in stack:
                task_total += count
                task_leaves[frames[-1]] += count
                for frame in set(frames[1:]):
                    task_frames[frame] += count
        assert total > 0, 'No samples in SQL window: ' + str(path)
        window[mode] = {'total_samples': total, 'task_samples': task_total, 'samples_by_pid': coverage,
            'threads': threads.most_common(30), 'leaves': leaves.most_common(35),
            'inclusive_frames': all_frames.most_common(60),
            'task_inclusive_frames': task_frames.most_common(70), 'task_leaves': task_leaves.most_common(45),
            'domain_inclusive_frames': domain_frames.most_common(100)}
    summary['windows'].append(window)
(output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps([{k: row[k] for k in ('query', 'iteration', 'sql_seconds')} for row in summary['windows']]))
