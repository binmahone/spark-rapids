#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Package compiled tests and a missing-retain mutation separately from production."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--cudf-source', type=Path, required=True)
p.add_argument('--engine', type=Path, required=True)
p.add_argument('--console', type=Path, required=True)
p.add_argument('--javac', type=Path, required=True)
p.add_argument('--slf4j', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
a.output.mkdir(exist_ok=False)
revision = subprocess.check_output(['git', '-C', str(a.cudf_source), 'rev-parse', 'HEAD'], text=True).strip()
assert revision == '32218d4495e035b4b10034d7a13f38f8c0143dd0'
for directory, name, suffix in [('java/target/test-classes', 'tests.jar', '*.class'),
                                ('java/src/test/resources', 'test-resources.zip', '*')]:
    base = a.cudf_source / directory
    with zipfile.ZipFile(a.output / name, 'w', zipfile.ZIP_DEFLATED) as dest:
        for source in sorted(base.rglob(suffix)):
            if source.is_file():
                dest.write(source, source.relative_to(base))
shutil.copyfile(a.console, a.output / a.console.name)
with tempfile.TemporaryDirectory(prefix='packed-view-retain-mutation-') as temporary:
    temporary = Path(temporary)
    text = (a.cudf_source / 'java/src/main/java/ai/rapids/cudf/ColumnVector.java').read_text()
    assert text.count('      contiguousBuffer.incRefCount();') == 1
    text = text.replace('      contiguousBuffer.incRefCount();', '')
    source = temporary / 'ColumnVector.java'
    source.write_text(text)
    subprocess.run([str(a.javac), '--release', '8', '-cp', str(a.engine) + ':' + str(a.slf4j),
                    '-d', str(temporary / 'classes'), str(source)], check=True)
    name = 'ai/rapids/cudf/ColumnVector$OffHeapState.class'
    with zipfile.ZipFile(a.output / 'missing-retain-mutation.jar', 'w') as dest:
        dest.write(temporary / 'classes' / name, name)
manifest = {'cudf_source_revision': revision,
            'mutation': 'Remove contiguousBuffer.incRefCount in packed view constructor',
            'sha256': {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(a.output.iterdir())}}
(a.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(manifest, indent=2))
