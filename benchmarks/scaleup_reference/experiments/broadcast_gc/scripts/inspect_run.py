#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Read compact progress without loading complete event logs or printing JVM environments."""
import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])


def read(path):
    return json.loads(path.read_text())


print('JOB', read(root / 'submission.json')['job_id'])
print('STATUS', json.dumps(read(root / 'status.json')))
for case in sorted((root / 'runs').iterdir()):
    for path in case.glob('applications/*/result.json'):
        result = read(path)
        print('APPLICATION', case.name, result['status'], json.dumps([
            {k: s.get(k) for k in ['query', 'sql_seconds', 'cleanup_seconds', 'validation']}
            for s in result.get('samples', [])]))
    for path in case.glob('applications/*/driver.log'):
        flags = re.findall(r'^\s*bool\s+(UseG1GC|DisableExplicitGC|ExplicitGCInvokesConcurrent)\s*:?=\s*(true|false)',
                           path.read_text(errors='replace'), re.M)
        if flags:
            print('FLAGS', case.name, json.dumps(dict(flags)))
    for path in sorted(case.glob('applications/*/*.observation.json'), key=lambda p: p.stat().st_mtime)[-1:]:
        rows = read(path)
        print('OBSERVATION', case.name, path.name, json.dumps({
            'executors': len(rows), 'ordinary_objects': sum(len(r['objects']) for r in rows),
            'host_blocks': sum(o['host_block_present'] for r in rows for o in r['objects']),
            'rmm_bytes': sum(r['rmm_bytes'] for r in rows),
            'host_allocated_bytes': sum(r['host_allocated_bytes'] for r in rows)}))
    evidence = case / 'case-evidence.json'
    if evidence.exists():
        result = read(evidence)
        if 'mechanism' in result:
            result['mechanism']['targets'] = {k: len(v) for k, v in result['mechanism']['targets'].items()}
        print('EVIDENCE', json.dumps(result))
failures = list((root / 'failures').glob('*.json'))
for path in failures:
    print('FAILURE', path.name, json.dumps(read(path)))
if failures:
    for case in sorted((root / 'runs').iterdir()):
        for path in case.glob('applications/*/driver.log'):
            print('DRIVER_END', case.name, path.read_text(errors='replace')[-3500:])
        for path in case.glob('node-controller/*/console.log'):
            content = path.read_text(errors='replace')
            if 'Traceback' in content:
                print('NODE_ERROR', path.parent.name, content[-2500:])
