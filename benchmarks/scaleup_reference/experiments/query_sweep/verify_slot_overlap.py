#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Verify observed Spark task overlap inside the second scored SQL action."""
from collections import defaultdict
import json
from pathlib import Path
import re


def scheduler_overlap(app, query, slots, executor_logs=None):
    profile = json.loads((app / 'profile.json').read_text())
    events = Path(profile['spark.eventLog.dir'].removeprefix('file://'))
    root = None
    window = {}
    stages = defaultdict(set)
    tasks = defaultdict(list)
    for path in sorted(events.iterdir()):
        if not path.is_file() or path.name.startswith('.'):
            continue
        assert not path.name.endswith('.inprogress')
        for line in path.open('rb'):
            if not any(k in line for k in (b'SparkListenerSQLExecution',
                       b'SparkListenerJobStart', b'SparkListenerTaskEnd')):
                continue
            event = json.loads(line)
            kind = event['Event']
            if kind.endswith('SparkListenerSQLExecutionStart'):
                if event.get('description') == f'q{query}-iteration2':
                    assert root is None
                    root = str(event['executionId'])
                    window['start'] = event['time']
            elif kind.endswith('SparkListenerSQLExecutionEnd'):
                if str(event['executionId']) == root:
                    window['end'] = event['time']
            elif kind == 'SparkListenerJobStart':
                props = event.get('Properties') or {}
                execution = props.get('spark.sql.execution.root.id', props.get('spark.sql.execution.id'))
                if execution is not None:
                    for stage in event['Stage IDs']:
                        stages[stage].add(str(execution))
            else:
                assert event['Task End Reason']['Reason'] == 'Success'
                info = event['Task Info']
                tasks[event['Stage ID']].append((info['Task ID'], info['Executor ID'],
                    info['Launch Time'], info['Finish Time']))
    assert root is not None and window['end'] > window['start']
    intervals = defaultdict(list)
    task_ids = defaultdict(set)
    for stage, rows in tasks.items():
        if root in stages[stage]:
            for task, executor, start, end in rows:
                start, end = max(start, window['start']), min(end, window['end'])
                if end > start:
                    intervals[executor].extend(((start, 1), (end, -1)))
                    task_ids[executor].add(task)
    peaks = {}
    for executor, points in intervals.items():
        current = maximum = 0
        for _, delta in sorted(points):
            current += delta
            assert current >= 0
            maximum = max(maximum, current)
        assert current == 0
        peaks[executor] = maximum
    if executor_logs is None:
        run = app.parents[1]
        config = json.loads((run / 'config.json').read_text())
        result = json.loads((app / 'result.json').read_text())
        scratch = Path(config['deployment']['scratch_root']) / run.name / 'suite'
        executor_logs = {p.parent.name: p for p in
            scratch.glob(f'worker-*/work/{result["application_id"]}/*/stderr')}
    assert set(executor_logs) == set(task_ids) and len(task_ids) == 8
    execution_peaks = {}
    matched = {}
    pattern = re.compile(r'INFO Executor: (Running|Finished) task .*\(TID (\d+)\)')
    for executor, path in executor_logs.items():
        active, started, finished = set(), set(), set()
        maximum = 0
        for line in path.open():
            match = pattern.search(line)
            if not match:
                continue
            action, task = match[1], int(match[2])
            if task not in task_ids[executor]:
                continue
            if action == 'Running':
                assert task not in started
                started.add(task)
                active.add(task)
                maximum = max(maximum, len(active))
            else:
                assert task in active
                active.remove(task)
                finished.add(task)
        assert not active and started == finished == task_ids[executor]
        execution_peaks[executor] = maximum
        matched[executor] = len(started)
    assert max(execution_peaks.values()) == slots, (slots, execution_peaks)
    assert all(1 <= value <= slots for value in execution_peaks.values())
    return {'maximum_simultaneous_executor_tasks': execution_peaks,
            'matched_query_tasks': matched,
            'driver_observed_interval_peaks': peaks,
            'limits': ['Executor task overlap is not simultaneous GPU kernel execution.',
                       'Driver-side task end timestamps include result handling and can overstate occupancy.']}
