#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Recount retained events including UUID-group asynchronous broadcast work."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
target = root/'retained/query-root-recount'
target.mkdir(exist_ok=False)
archives = {
    'all-query-evidence.tar.gz': '0f641b39c4b4e132f0e0f0651747c203a3059f8c9347c88584f47666b8446f8b',
    'time-trial-evidence.tar.gz': '36120de6b1df7cbdcf628834a33d6b342c5342e5e9d5d3f139b0a3bdd88fa1e5'}
for name, expected in archives.items():
    archive = root/'retained'/name
    with archive.open('rb') as stream:
        assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected
    with tarfile.open(archive) as source:
        for member in source:
            if not member.isfile() or not (member.name.startswith('results/') or '/events/' in member.name):
                continue
            path = target/member.name
            assert path.resolve().is_relative_to(target.resolve())
            path.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as incoming, path.open('xb') as outgoing:
                shutil.copyfileobj(incoming, outgoing)
labels = [p.parent.name for p in (target/'results').glob('*/matrix.json')]
command = [sys.executable, str(Path(__file__).with_name('suite_status.py')),
           *labels, '--retained-root', str(target), '--analyze', '--recent', '1']
subprocess.run(command, check=True)
print(json.dumps({'verified_archives': archives, 'extracted_root': str(target),
                  'labels': labels}), flush=True)
