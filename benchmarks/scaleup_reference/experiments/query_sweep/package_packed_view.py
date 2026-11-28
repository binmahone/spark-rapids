#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Overlay one audited Java class while preserving every frozen native artifact."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--cudf-source', type=Path, required=True)
p.add_argument('--baseline', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
sha = lambda data: hashlib.sha256(data).hexdigest()
revision = subprocess.check_output(['git', '-C', str(a.cudf_source), 'rev-parse', 'HEAD'], text=True).strip()
assert revision == '32218d4495e035b4b10034d7a13f38f8c0143dd0'
assert not subprocess.check_output(['git', '-C', str(a.cudf_source), 'status', '--porcelain'], text=True).strip()
engine = 'cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
base = a.baseline / engine
assert sha(base.read_bytes()) == 'ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
target = 'ai/rapids/cudf/ColumnVector$OffHeapState.class'
bytecode = (a.cudf_source / 'java/target/classes' / target).read_bytes()
assert bytecode[:4] == b'\xca\xfe\xba\xbe' and int.from_bytes(bytecode[6:8], 'big') == 52
overlay = {'repository': 'https://github.com/binmahone/cudf', 'revision': revision,
           'native_base_revision': '5a2cad06ba5ef1b77a5cc3f1fc61fc0cee5bf57c',
           'baseline_engine_sha256': sha(base.read_bytes()),
           'class_sha256': {target: sha(bytecode)},
           'method': 'Maven compiler:compile and compiler:testCompile with -am; one class overlay; native entries byte-identical'}
info_name = 'cudf-java-version-info.properties'
with zipfile.ZipFile(base) as source:
    assert len(source.namelist()) == len(set(source.namelist()))
    props = source.read(info_name).decode()
    assert 'revision=' + overlay['native_base_revision'] in props
    props = props.replace('revision=' + overlay['native_base_revision'], 'revision=' + revision)
    props += '\nnative_base_revision=' + overlay['native_base_revision'] + '\n'
    changes = {target: bytecode, info_name: props.encode()}
    a.output.mkdir(parents=True, exist_ok=False)
    with zipfile.ZipFile(a.output / engine, 'w') as dest:
        for info in source.infolist():
            dest.writestr(copy.copy(info), changes.get(info.filename, source.read(info.filename)))
        dest.writestr('packed-view-java-overlay.json', json.dumps(overlay, indent=2) + '\n')
    with zipfile.ZipFile(a.output / engine) as dest:
        for name in source.namelist():
            assert dest.read(name) == changes.get(name, source.read(name)), name
        assert set(dest.namelist()) == set(source.namelist()) | {'packed-view-java-overlay.json'}
manifest = json.loads((a.baseline / 'manifest.json').read_text())
manifest['files'] = {engine: {'bytes': (a.output / engine).stat().st_size,
                            'sha256': sha((a.output / engine).read_bytes())}}
manifest['components'][info_name]['revision'] = revision
manifest['components'][info_name]['native_base_revision'] = overlay['native_base_revision']
manifest['java_source_overlay'] = overlay
(a.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(manifest['files'], indent=2))
