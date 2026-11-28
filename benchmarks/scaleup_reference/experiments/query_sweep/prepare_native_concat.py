#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Prepare a pinned SDK and native probe without rebuilding unrelated operators."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import subprocess
from urllib.request import urlopen
import zipfile
import zlib

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
p.add_argument('--cudf-revision', required=True)
a = p.parse_args()
assert json.loads((a.work / 'evidence/owner.json').read_text())['thread_id'] == a.thread_id
assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
root = a.work / 'native-concat'
root.mkdir(exist_ok=True)
def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(8 * 1024**2), b''): h.update(block)
    return h.hexdigest()
def get(url, path):
    if not path.exists():
        subprocess.run(['curl', '--fail', '--location', '--retry', '3', '--output', str(path), url], check=True)
def command(*args):
    subprocess.run(args, check=True)
packages = ['cuda-nvcc-13-1', 'cuda-cudart-dev-13-1', 'g++-12']
simulation = subprocess.check_output(['sudo', 'apt-get', '--simulate', '--no-install-recommends',
                                       'install', *packages], text=True)
(root / 'apt-simulation.txt').write_text(simulation)
assert not any(line.startswith(('Inst nvidia-', 'Remv ')) for line in simulation.splitlines())
command('sudo', 'apt-get', 'install', '-y', '--no-install-recommends', *packages)
source = root / 'cudf'
if not (source / '.git').exists():
    command('git', 'clone', '--filter=blob:none', '--no-checkout',
            'https://github.com/binmahone/cudf.git', str(source))
command('git', '-C', str(source), 'fetch', 'origin', a.cudf_revision)
command('git', '-C', str(source), 'checkout', '--detach', a.cudf_revision)
assert not subprocess.check_output(['git', '-C', str(source), 'status', '--porcelain'], text=True).strip()
pins_url = ('https://raw.githubusercontent.com/NVIDIA/cudf-spark-jni/'
            'bd15415531c40a2e83067d91147fcfa51a317b1d/thirdparty/cudf-pins/versions.json')
get(pins_url, root / 'pins.json')
pins = json.loads((root / 'pins.json').read_text())['packages']
records = {}
for name in ['CCCL', 'rmm', 'nvtx3', 'rapids_logger', 'spdlog']:
    pin = pins[name]
    url = pin.get('url') or pin['git_url'].removesuffix('.git') + '/archive/' + pin['git_tag'] + '.tar.gz'
    archive = root / (name + '.tar.gz')
    get(url, archive)
    digest = sha(archive)
    if 'url_hash' in pin: assert digest == pin['url_hash'].split('=', 1)[1]
    target = root / name
    if not target.exists():
        target.mkdir()
        command('tar', '-xzf', str(archive), '--strip-components=1', '-C', str(target))
    records[name] = {'url': url, 'sha256': digest}
engine = a.work / 'artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
assert sha(engine) == 'ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
native = root / 'lib'
native.mkdir(exist_ok=True)
with zipfile.ZipFile(engine) as jar:
    for name in jar.namelist():
        if name.startswith('amd64/Linux/') and name.endswith('.so'):
            target = native / Path(name).name
            if not target.exists(): target.write_bytes(jar.read(name))
    target = native / 'libcudf.so'
    if not target.exists():
        manifest = 'amd64/Linux/libcudf.so.chunks.properties'
        values = dict(line.split('=', 1) for line in jar.read(manifest).decode().splitlines()
                      if line and not line.startswith('#'))
        assert values['chunk.compression'] == 'zstd'
        zstd = ctypes.CDLL('libzstd.so.1')
        zstd.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
        zstd.ZSTD_decompress.restype = ctypes.c_size_t
        with target.open('wb') as output:
            for i in range(int(values['chunk.count'])):
                chunk = jar.read(manifest.removesuffix('.properties') + f'/{i:05d}')
                size = min(int(values['chunk.size']), int(values['library.size']) - i * int(values['chunk.size']))
                decoded = ctypes.create_string_buffer(size)
                assert zstd.ZSTD_decompress(decoded, size, chunk, len(chunk)) == size
                assert zlib.crc32(decoded.raw) == int(values[f'chunk.{i:05d}.crc32'], 16)
                output.write(decoded.raw)
        assert target.stat().st_size == int(values['library.size'])
body = (source / 'cpp/src/strings/copying/concatenate.cu').read_text()
needle = 'namespace cudf {\nnamespace strings {\nnamespace detail {'
assert body.count(needle) == 1
body = body.replace(needle, 'namespace cudf {\nnamespace strings {\nnamespace offset_experiment {\nusing namespace cudf::strings::detail;')
(root / 'candidate.cu').write_text(body)
record = {'cudf_revision': a.cudf_revision, 'dependencies': records, 'libcudf_sha256': sha(target),
          'candidate_source_sha256': sha(root / 'candidate.cu'),
          'transformation': 'Only the implementation namespace changes for side-by-side comparison'}
(root / 'sdk-manifest.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record))
