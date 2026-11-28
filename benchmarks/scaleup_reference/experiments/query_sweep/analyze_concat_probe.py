#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare retained native concatenation trials and their actual stage schedules."""
import argparse
import json
from pathlib import Path

from analyze_critical_path import pressure, schedule


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--labels', nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = {}
    for label in args.labels:
        directory = args.work / 'code-profile' / label
        state = json.loads((directory / 'state.json').read_text())
        assert state['status'] != 'running'
        run = args.work / 'screen' / ('code-profile-' + label + '-0')
        result = json.loads((run / 'applications/suite/result.json').read_text())
        assert result['status'] == 'completed'
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        timeline = schedule(run)
        samples = []
        for sample in result['samples']:
            token = f"q{sample['query']}-iteration{sample['iteration']}"
            native = [node for node in sample['plan_metrics']
                      if node['node'] == 'GpuCoalesceBatches']
            row = {key: sample[key] for key in ('query', 'iteration', 'sql_seconds',
                   'cleanup_seconds', 'sql_plus_cleanup_seconds', 'sql_sha256',
                   'case_conf', 'reader_metrics')}
            row['concatenations'] = [node['metrics'] for node in native]
            row['schedule'] = timeline[token]
            row['pressure'] = pressure(directory, timeline[token]['window'])
            samples.append(row)
            largest = max((n['metrics'].get('concatTime_exSemWait', 0) for n in native), default=0)
            print(label, token, 'sql', round(row['sql_seconds'], 3),
                  'largest_concat_task_seconds', round(largest / 1e9, 3),
                  'psi', round(row['pressure']['sampled_memory_full_seconds'], 3))
            if sample['iteration'] == 2:
                for stage in timeline[token]['stages']:
                    if stage['span_seconds'] >= 1:
                        print('  stage', stage['stage'], 'parents', stage['parents'],
                              'start/end', stage['start_seconds'], stage['end_seconds'],
                              'tasks', stage['tasks'], 'max_task', stage['task_wall_max_seconds'])
        report[label] = {'state_status': state['status'], 'error': state.get('error'),
                         'native_probe': state['concat_probe'], 'samples': samples}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
