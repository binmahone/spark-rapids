#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Run real GPU ownership and packed-table tests, rejecting skipped tests."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET
import zipfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
p.add_argument('--label', required=True)
p.add_argument('--mutation', action='store_true')
a = p.parse_args()
assert json.loads((a.work / 'evidence/owner.json').read_text())['thread_id'] == a.thread_id
assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
assert a.label.replace('-', '').isalnum()
out = a.work / 'code-profile' / a.label
out.mkdir(exist_ok=False)
support = a.work / 'packed-view-tests'
manifest = json.loads((support / 'manifest.json').read_text())
for name, digest in manifest['sha256'].items():
    assert hashlib.sha256((support / name).read_bytes()).hexdigest() == digest
engine = a.work / 'artifacts/packed-view/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
em = json.loads((engine.parent / 'manifest.json').read_text())
assert hashlib.sha256(engine.read_bytes()).hexdigest() == em['files'][engine.name]['sha256']
env = dict(os.environ)
env['CUDA_VISIBLE_DEVICES'] = subprocess.check_output(['nvidia-smi', '--id=0', '--query-gpu=uuid', '--format=csv,noheader'], text=True).strip()
with zipfile.ZipFile(support / 'test-resources.zip') as resources:
    assert all(not Path(n).is_absolute() and '..' not in Path(n).parts for n in resources.namelist())
    resources.extractall(out / 'resources')
classpath = [out / 'resources', support / 'tests.jar', engine, support / 'junit-platform-console-standalone-1.4.2.jar',
             a.work / 'spark-venv/lib/python3.10/site-packages/pyspark/jars/*']
if a.mutation:
    classpath.insert(0, support / 'missing-retain-mutation.jar')
command = [str(a.work / 'jdk17/bin/java'), '-ea', '-XX:ErrorFile=' + str(out / 'hs_err-%p.log'),
           '-Dai.rapids.cudf.lib-native-dir=' + str(a.work / 'native-concat/lib'),
           '-cp', ':'.join(map(str, classpath)), 'org.junit.platform.console.ConsoleLauncher',
           '--disable-ansi-colors', '--fail-if-no-tests', '--reports-dir', str(out),
           '--select-class', 'ai.rapids.cudf.PackedViewOwnershipTest']
if not a.mutation:
    for method in ['testContiguousSplit', 'testChunkedPackBasic', 'testChunkedPackTwoPasses',
                   'testContiguousSplitWithStrings', 'testContiguousSplitWithStringsChunked',
                   'testSerializationReconstructFromMetadata']:
        command += ['--select-method', 'ai.rapids.cudf.TableTest#' + method]
with (out / 'console.log').open('x') as log:
    result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=180)
reports = list(out.glob('TEST-*.xml'))
assert reports, 'No JUnit reports produced'
cases = [case for report in reports for case in ET.parse(report).getroot().iter('testcase')]
assert len(cases) == (1 if a.mutation else 7), len(cases)
assert not any(case.find('skipped') is not None for case in cases), 'A GPU test was skipped'
failures = [case for case in cases if case.find('failure') is not None or case.find('error') is not None]
if a.mutation:
    assert result.returncode != 0 and len(failures) == 1
    assert 'expected: <3> but was: <1>' in (out / 'console.log').read_text(), 'Mutation failed for an unrelated reason'
else:
    assert result.returncode == 0 and not failures
receipt = {'status': 'passed', 'mutation_rejected': a.mutation, 'tests': len(cases),
           'engine_sha256': em['files'][engine.name]['sha256'], 'support': manifest,
           'command': command, 'gpu': env['CUDA_VISIBLE_DEVICES']}
(out / 'result.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'status': 'passed', 'mutation_rejected': a.mutation, 'tests': len(cases)}))
