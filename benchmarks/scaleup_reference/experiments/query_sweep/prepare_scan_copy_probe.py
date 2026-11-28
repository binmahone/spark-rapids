#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Prepare a pinned native copy implementation with an isolated admission experiment."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
a = p.parse_args()
assert json.loads((a.work / 'evidence/owner.json').read_text())['thread_id'] == a.thread_id
root = a.work / 'native-scan-copy'
root.mkdir(exist_ok=False)
revision = 'bd15415531c40a2e83067d91147fcfa51a317b1d'
pins = {'MemoryUtilsJni.cpp': 'bbe51caa1dc47275d970c0698543913b3096e9956950c39fbaa8acac2db2ebd8',
        'memory_utils.cu': '149f116653c76b1131be9c78a34777c49db4b401012bb3bbae4abf06483167c6',
        'memory_utils.hpp': '6d07c66d367c14c8be852f2e57d22498f709165f3d9ca940655de88830452d8e'}
manifest = {'jni_revision': revision, 'files': {}, 'changes': [
    'Use an isolated namespace for the pinned packing and copy implementation.',
    'Allocate and prepare all copies before admission.',
    'For large copies, compare original, synchronization-only, and physical-pair admission.',
    'The synchronization control and pair mode finish preceding GPU work before admission.']}
here = Path(__file__).resolve().parent
manifest['source_revision'] = subprocess.check_output(
    ['git', '-C', str(here), 'rev-parse', 'HEAD'], text=True).strip()
manifest['probe_sources'] = {name: hashlib.sha256((here / name).read_bytes()).hexdigest()
    for name in ('copy_admission.hpp', 'scan_copy_admission.hpp', 'probe_allocator.hpp', 'scan_copy_probe_jni.cpp',
                 'ScanCopyProbe.java', 'build_scan_copy_probe.sh')}
for name, expected in pins.items():
    url = f'https://raw.githubusercontent.com/NVIDIA/cudf-spark-jni/{revision}/src/main/cpp/src/{name}'
    data = urllib.request.urlopen(url, timeout=30).read()
    assert hashlib.sha256(data).hexdigest() == expected
    (root / (name + '.original')).write_bytes(data)
    source = data.decode().replace('namespace spark_rapids_jni', 'namespace scan_copy_experiment')
    source = source.replace('spark_rapids_jni::', 'scan_copy_experiment::')
    if name == 'memory_utils.cu':
        source = source.replace('#include "memory_utils.hpp"',
            '#include "memory_utils.hpp"\n#include "scan_copy_admission.hpp"\n#include <numeric>')
        marker = '  // Queue the copies in one call.\n'
        assert source.count(marker) == 1
        source = source.replace(marker, '  copy_admission admission(stream,\n'
            '    std::accumulate(copies.sizes.begin(), copies.sizes.end(), std::size_t{0}));\n\n' + marker)
        marker = 'copies.dsts.data(), copies.srcs.data(), copies.sizes.data(), copies.dsts.size(), stream));'
        assert source.count(marker) == 1
        source = source.replace(marker, marker + '\n      admission.finish();')
    (root / name).write_text(source)
    manifest['files'][name] = {'url': url, 'original_sha256': expected,
                              'transformed_sha256': hashlib.sha256(source.encode()).hexdigest()}
template = a.work / 'native-concat/rapids_logger/cmake/logger_macros.hpp.in'
header = template.read_text().replace('@_RAPIDS_LOGGER_MACRO_PREFIX@', 'CUDF').replace(
    '@_RAPIDS_LOGGER_DEFAULT_LOGGER@', 'cudf::default_logger()')
assert '@_RAPIDS_LOGGER_' not in header
(root / 'cudf').mkdir()
(root / 'cudf/logger_macros.hpp').write_text(header)
manifest['logger_macros'] = {'template_sha256': hashlib.sha256(template.read_bytes()).hexdigest(),
                            'generated_sha256': hashlib.sha256(header.encode()).hexdigest()}
(root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
