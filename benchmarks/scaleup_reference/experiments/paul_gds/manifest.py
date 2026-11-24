#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Retain build identities for isolated before/after PR artifacts."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from zipfile import ZipFile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source', required=True, type=Path)
parser.add_argument('--artifacts', required=True, type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[4]
tools = root.parent / '261123_scaleup_reference/.build/artifacts/arm64/benchmark-tools.jar'
shutil.copyfile(tools, args.artifacts / tools.name)
engine = next(args.artifacts.glob('cudf-spark_*.jar'))
components = {}
with ZipFile(engine) as archive:
    for filename in ('rapids4spark-version-info.properties', 'cudf-spark-jni-version-info.properties',
                     'cudf-java-version-info.properties', 'cudf-spark-private-version-info.properties'):
        names = [n for n in archive.namelist() if n == filename or n.endswith('/' + filename)]
        assert len(names) == 1, (filename, names)
        components[filename] = dict(line.split('=', 1) for line in archive.read(names[0]).decode().splitlines()
                                    if '=' in line and not line.startswith('#'))
revision = subprocess.check_output(['git', '-C', str(args.source), 'rev-parse', 'HEAD'], text=True).strip()
assert components['rapids4spark-version-info.properties']['revision'] == revision
assert components['cudf-spark-jni-version-info.properties']['revision'] == 'bf63ec567eabc0bcac5366f6afba0b0bba40f30a'
assert components['cudf-spark-private-version-info.properties']['revision'] == '61e9e96803771371dec116593f0c4eced9ac44d8'
assert not subprocess.check_output(['git', '-C', str(args.source), 'status', '--porcelain'], text=True).strip()
files = {p.name: {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
         for p in (engine, args.artifacts / tools.name, *args.artifacts.glob('cudf-spark-integration-tests*.jar'))}
(args.artifacts / 'manifest.json').write_text(json.dumps({'architecture': 'arm64',
    'files': files, 'components': components, 'source_revision': revision, 'source_dirty': False}, indent=2) + '\n')
print('ARTIFACT_READY', revision, files[engine.name]['sha256'], flush=True)
