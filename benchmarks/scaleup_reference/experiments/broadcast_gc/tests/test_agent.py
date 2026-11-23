#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Exercise the transformer and GC completion observer without using a GPU."""
import argparse
import json
from pathlib import Path
import subprocess
from zipfile import ZipFile

HERE = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--jar', type=Path, required=True)
parser.add_argument('--spark-home', type=Path, required=True)
parser.add_argument('--java-home', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
(args.output / 'jvm-gc').mkdir()
classes = args.output / 'classes'
classes.mkdir()
with ZipFile(args.jar) as archive:
    assert not any('execution/SerializeConcatHostBuffersDeserializeBatch' in n for n in archive.namelist())
classpath = ':'.join(map(str, [classes.resolve(), args.jar.resolve(), *args.spark_home.glob('jars/*xbean*.jar')]))
subprocess.run([str(args.java_home / 'bin/javac'), '--release', '8', '-cp', classpath,
                '-d', str(classes), *map(str, HERE.rglob('*.java'))], check=True)
records = []
for enabled, copies in [(False, 1), (True, 1), (False, 3), (True, 3)]:
    label = ('on' if enabled else 'off') + '-agents' + str(copies)
    folder = (args.output / label).resolve()
    command = [str(args.java_home / 'bin/java'), '-Xms128m', '-Xmx128m', '-XX:+UseG1GC',
               '-XX:-DisableExplicitGC', '-XX:' + ('+' if enabled else '-') + 'ExplicitGCInvokesConcurrent',
               '-Xlog:gc*:file=' + str(args.output.resolve() / 'jvm-gc/gc-%p.log') +
               ':utctime,uptimemillis,level,tags:filecount=0',
               *['-javaagent:' + str(args.jar.resolve()) + '=' + str(folder)] * copies,
               '-cp', classpath, 'AgentProbe']
    result = subprocess.run(command, capture_output=True, text=True, timeout=20)
    (args.output / (label + '.console.log')).write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    events = [json.loads(line) for p in folder.glob('*.jsonl') for line in p.read_text().splitlines()]
    names = [e['event'] for e in events]
    assert names.count('class_transformed') == 1
    assert names.count('duplicate_agent_skipped') == copies - 1
    assert names.count('<init>_return') == 1
    assert names.count('closeInternal_throw') == 1
    assert names.count('finalize_return') == 1
    assert names.count('closeInternal_return') == 2
    assert names.count('retireAfterIndependentQuery_enter') == 1
    assert names.count('retireAfterIndependentQuery_return') == 1
    records.append({'enabled': enabled, 'agent_copies': copies, 'status': 'passed', 'events': names})
build = json.loads((args.jar.parent / 'build-receipt.json').read_text())
real_classes = args.output / 'real-classes'
real_classes.mkdir()
real_classpath = str(real_classes.resolve()) + ':' + build['commands'][0][4]
subprocess.run([str(args.java_home / 'bin/javac'), '--release', '8', '-cp', real_classpath,
                '-d', str(real_classes), str(HERE / 'RealClassProbe.java')], check=True)
engine = next(p for p in build['dependency_sha256'] if Path(p).name.startswith('cudf-spark_'))
result = subprocess.run([str(args.java_home / 'bin/java'),
    '-javaagent:' + str(args.jar.resolve()) + '=' + str(args.output.resolve() / 'real-engine'),
    '-cp', real_classpath, 'RealClassProbe', Path(engine).name], capture_output=True, text=True, timeout=20)
(args.output / 'real-engine.console.log').write_text(result.stdout + result.stderr)
assert result.returncode == 0, result.stdout + result.stderr
records.append({'real_engine_class': True, 'status': 'passed'})
(args.output / 'test-results.json').write_text(json.dumps(records, indent=2) + '\n')
print(json.dumps(records))
