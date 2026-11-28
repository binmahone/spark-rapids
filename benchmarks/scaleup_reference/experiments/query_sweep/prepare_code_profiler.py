#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Install a hash-pinned mixed Java/native stack profiler on the experiment host."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
a = p.parse_args()
assert json.loads((a.work / 'evidence/owner.json').read_text())['thread_id'] == a.thread_id
assert platform.machine() == 'x86_64'
root = a.work / 'profilers'
root.mkdir(exist_ok=True)
items = {
    'async-profiler-4.6-linux-x64.tar.gz': '1e79ed54d8952889a2e74f993d22ee925e3322460d4f37a2ed95bfb88585da36',
    'jfr-converter.jar': 'd12072c704ab62dd942a47d4c452121311e2c9f528a05bce1a81f09143ab5047'}
for name, digest in items.items():
    path = root / name
    if not path.exists():
        urllib.request.urlretrieve('https://github.com/async-profiler/async-profiler/releases/download/v4.6/' + name, path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, name
target = root / 'async-profiler-4.6-linux-x64'
if not target.exists():
    subprocess.run(['tar', '-xzf', str(root / 'async-profiler-4.6-linux-x64.tar.gz'), '-C', str(root)], check=True)
version = subprocess.check_output([str(target / 'bin/asprof'), '--version'], text=True)
assert '4.6' in version
manifest = {'version': version, 'files': items, 'library': str(target / 'lib/libasyncProfiler.so')}
manifest['library_sha256'] = hashlib.sha256(Path(manifest['library']).read_bytes()).hexdigest()
(root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(version)
