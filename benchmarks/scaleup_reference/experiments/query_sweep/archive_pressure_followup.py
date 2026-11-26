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
"""Archive the closed pressure experiment without duplicating earlier experiment logs."""
import argparse
from pathlib import Path
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True)
p.add_argument('--mode', choices=('screen', 'full'), required=True)
a = p.parse_args()
work = Path(a.work).resolve()
directory = work / 'pressure-followup' / a.mode
state = read(directory / 'state.json')
assert state['status'] == 'completed' and state['restored']['settings']['watermark_scale_factor'] == '10'
assert (work / 'evidence' / ('pressure-' + a.mode + '.exit')).read_text().strip() == '0'
archive = work / 'evidence' / ('pressure-' + a.mode + '-complete.tar.gz')
assert not archive.exists()
paths = {path: str(path.relative_to(work)) for path in directory.rglob('*')
         if path.is_file() and not path.is_symlink()}
for group in state['groups']:
    run = Path(group['run'])
    for path in run.rglob('*'):
        if path.is_file() and not path.is_symlink():
            paths[path] = str(path.relative_to(work))
    config = read(run / 'config.json')
    scratch = Path(config['deployment']['scratch_root']) / run.name
    for path in scratch.rglob('*'):
        if path.is_file() and not path.is_symlink() and (
                'events' in path.relative_to(scratch).parts or path.name in ('stdout', 'stderr') or path.suffix == '.log'):
            paths[path] = str(Path('scratch') / run.name / path.relative_to(scratch))
for name in ('owner.json', 'deployment.json', 'host-prerequisites.json',
             'pressure-' + a.mode + '.log', 'pressure-' + a.mode + '.exit'):
    path = work / 'evidence' / name
    paths[path] = str(path.relative_to(work))
manifest = []
with tarfile.open(archive, 'w:gz', compresslevel=1) as target:
    for path, name in sorted(paths.items(), key=lambda item: item[1]):
        stat = path.stat()
        target.add(path, arcname=name, recursive=False)
        assert path.stat().st_mtime_ns == stat.st_mtime_ns and path.stat().st_size == stat.st_size
        manifest.append({'name': name, 'bytes': stat.st_size})
write(archive.with_name(archive.name.removesuffix('.tar.gz') + '-manifest.json'), {
    'archive': archive.name, 'bytes': archive.stat().st_size, 'sha256': sha(archive), 'files': manifest,
    'instance_policy': 'Retain until explicit user release'})
print(archive, archive.stat().st_size, sha(archive))
