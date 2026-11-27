#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Record exact packaged differences from the retained reference engine."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


def hashes(path):
    with zipfile.ZipFile(path) as archive:
        return {name: hashlib.sha256(archive.read(name)).hexdigest()
                for name in archive.namelist() if not name.endswith('/')}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    a = p.parse_args()
    name = 'cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
    base = a.work / 'artifacts/main' / name
    candidate = a.work / 'copy-stream-engine/.build/artifacts/amd64' / name
    before, after = hashes(base), hashes(candidate)
    changed = sorted(n for n in before.keys() & after.keys() if before[n] != after[n])
    added, removed = sorted(after.keys() - before.keys()), sorted(before.keys() - after.keys())
    native = [n for n in changed + added + removed if '.so' in n]
    assert not native, native
    unexpected_classes = [n for n in changed + added + removed if n.endswith('.class') and
                          '/parquet/HybridScan' not in n]
    result = {'baseline': str(base), 'candidate': str(candidate), 'changed': changed,
              'added': added, 'removed': removed, 'native_libraries_identical': True,
              'other_changed_classes': unexpected_classes}
    output = a.work / 'copy-stream-build/artifact-diff.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    print('Changed', len(changed), 'added', len(added), 'removed', len(removed),
          'other classes', len(unexpected_classes))
    print(json.dumps(unexpected_classes[:30]))


if __name__ == '__main__':
    main()
