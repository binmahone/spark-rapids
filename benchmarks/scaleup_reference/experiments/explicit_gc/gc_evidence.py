# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Read actual JVM flags and GC causes; never infer activation from a Spark setting."""
import json
from pathlib import Path
import re

FLAGS = ('UseG1GC', 'DisableExplicitGC', 'ExplicitGCInvokesConcurrent')


def startup_flags(text):
    return {name: value == 'true' for name, value in re.findall(
        r'^\s*bool\s+(UseG1GC|DisableExplicitGC|ExplicitGCInvokesConcurrent)\s*:?=\s*(true|false)',
        text, re.MULTILINE)}


def gc_file(path):
    explicit = set()
    full_explicit = set()
    pauses = []
    for line in Path(path).read_text(errors='replace').splitlines():
        match = re.search(r'GC\((\d+)\)', line)
        if match and '(System.gc())' in line:
            explicit.add(int(match[1]))
            if 'Pause Full' in line:
                full_explicit.add(int(match[1]))
        match = re.search(r'GC\(\d+\) Pause .*? (\d+(?:\.\d+)?)ms\s*$', line)
        if match:
            pauses.append(float(match[1]))
    return {'file': str(path), 'system_gc_requests': len(explicit),
            'full_system_gc_events': len(full_explicit), 'pause_events': len(pauses),
            'sum_jvm_pause_ms': sum(pauses), 'max_jvm_pause_ms': max(pauses, default=0)}


def case_evidence(run, enabled, executors):
    run = Path(run)
    summary = json.loads((run / 'summary.json').read_text())
    assert summary['status'] == 'completed' and len(summary['samples']) == 44
    assert all(s['validation']['status'] == 'passed' for s in summary['samples'])
    applications = list(run.glob('applications/*/result.json'))
    expected_jvms = len(applications) * (executors + 1)
    startup = []
    for node in run.glob('gc/*/startup-flags.json'):
        startup.extend(json.loads(node.read_text()))
    for driver in run.glob('applications/*/driver.log'):
        startup.append({'source': str(driver), 'flags': startup_flags(driver.read_text(errors='replace'))})
    assert len(startup) == expected_jvms, (len(startup), expected_jvms)
    for record in startup:
        assert record['flags'] == {'UseG1GC': True, 'DisableExplicitGC': False,
                                   'ExplicitGCInvokesConcurrent': enabled}, record
    logs = [gc_file(p) for p in run.glob('gc/*/gc-*.log')]
    assert len(logs) == expected_jvms, (len(logs), expected_jvms)
    receipts = [json.loads(p.read_text()) for p in run.glob('applications/*/*.cleanup.json')]
    assert len(receipts) == 44 and all(len(r) == executors for r in receipts)
    second = [s for s in summary['samples'] if s['iteration'] == 2]
    return {'run': str(run), 'enabled': enabled, 'validated_samples': 44,
            'verified_application_jvms': expected_jvms,
            'system_gc_requests': sum(x['system_gc_requests'] for x in logs),
            'full_system_gc_events': sum(x['full_system_gc_events'] for x in logs),
            'sum_jvm_pause_ms': sum(x['sum_jvm_pause_ms'] for x in logs),
            'max_jvm_pause_ms': max(x['max_jvm_pause_ms'] for x in logs),
            'second_execution_sql_sum_seconds': sum(s['sql_seconds'] for s in second),
            'second_execution_sql_plus_cleanup_sum_seconds': sum(s['sql_plus_cleanup_seconds'] for s in second),
            'best_of_two_sql_sum_seconds': summary['best_sql_sum_seconds'],
            'scope': 'All driver/executor lifecycle GC, including startup, reference, validation and cleanup; pause sums are across JVMs, not wall time',
            'jvm_logs': logs}
