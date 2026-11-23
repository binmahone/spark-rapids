#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Build a separate Spark 3.5.3 diagnostic bridge/agent without modifying the engine."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
from zipfile import ZipFile

HERE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', type=Path, required=True)
    parser.add_argument('--base-tools', type=Path, required=True)
    parser.add_argument('--spark-home', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, required=True)
    parser.add_argument('--dependency-repository', type=Path, required=True,
                        help='Read-only dependency inputs; Maven never runs against this path')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    classes = output / 'classes'
    classes.mkdir()
    repository = args.dependency_repository
    scala = [repository / 'org/scala-lang' / ('scala-' + name) / '2.12.21' /
             ('scala-' + name + '-2.12.21.jar') for name in ['library', 'reflect', 'compiler']]
    spark = sorted(p for p in (repository / 'org/apache/spark').glob('*/3.5.3/*-3.5.3.jar')
                   if '_2.13' not in p.name)
    assert any(p.name == 'spark-core_2.12-3.5.3.jar' for p in spark)
    support = [p for p in sorted((args.spark_home / 'jars').glob('*.jar'))
               if not p.name.startswith(('spark-', 'scala-'))]
    inputs = [*scala, *spark, *support, args.engine.resolve(), args.base_tools.resolve()]
    assert all(p.is_file() for p in inputs)
    with ZipFile(args.base_tools) as archive:
        archive.extractall(classes, members=[n for n in archive.namelist() if n.endswith('.class')])
    classpath = ':'.join(map(str, [classes, *inputs]))
    commands = [
        [str(args.java_home / 'bin/javac'), '--release', '8', '-cp', classpath, '-d', str(classes), str(HERE / 'BroadcastGcAgent.java')],
        [str(args.java_home / 'bin/java'), '-Xmx2g', '-cp', classpath, 'scala.tools.nsc.Main', '-target:jvm-1.8',
         '-classpath', classpath, '-d', str(classes),
         *map(str, sorted(HERE.glob('*.scala')))],
    ]
    for command in commands:
        if subprocess.run(command).returncode:
            raise SystemExit('Compilation failed: ' + Path(command[0]).name)
    manifest = output / 'MANIFEST.MF'
    manifest.write_text('Manifest-Version: 1.0\nPremain-Class: com.nvidia.spark.rapids.benchmark.gc.BroadcastGcAgent\n\n')
    jar = output / 'broadcast-gc-tools.jar'
    command = [str(args.java_home / 'bin/jar'), 'cfm', str(jar), str(manifest), '-C', str(classes), '.']
    commands.append(command)
    subprocess.run(command, check=True)
    receipt = {'jar': str(jar), 'sha256': sha(jar), 'commands': commands,
               'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
               'source_dirty': bool(subprocess.check_output(['git', '-C', str(HERE), 'status', '--porcelain'], text=True).strip()),
               'java_version': subprocess.check_output([str(args.java_home / 'bin/java'), '-version'], stderr=subprocess.STDOUT, text=True),
               'engine_sha256': sha(args.engine), 'base_tools_sha256': sha(args.base_tools),
               'dependency_sha256': {str(p): sha(p) for p in inputs},
               'source_sha256': {p.name: sha(p) for p in HERE.iterdir()
                                 if p.suffix in ['.java', '.scala', '.py']}}
    (output / 'build-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({'jar': str(jar), 'sha256': receipt['sha256']}))


if __name__ == '__main__':
    main()
