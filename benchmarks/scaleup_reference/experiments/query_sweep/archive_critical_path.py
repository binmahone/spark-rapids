#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Preserve critical-path evidence after checking the recorded processes exited."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
import services

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
a = p.parse_args()
work = a.work.resolve()
directory = work / 'critical-path'
files = {}
def add_tree(root, prefix):
    for path in root.rglob('*'):
        if path.is_file() and not path.is_symlink():
            files[path] = str(Path(prefix) / path.relative_to(root))
audit = []
for path in directory.glob('*/state.json'):
    state = json.loads(path.read_text())
    assert state['status'] in ('completed', 'failed')
    for record in [json.loads((path.parent / 'controller.json').read_text()), *state['profilers']]:
        stat = Path('/proc') / str(record['pid']) / 'stat'
        live = stat.exists() and stat.read_text().split(') ', 1)[1].split()[19] == str(record['start_time'])
        assert not live, record
        audit.append({'pid': record['pid'], 'start_time': record['start_time'], 'live': live})
for run in (work / 'screen').glob('critical-path-*'):
    config = json.loads((run / 'config.json').read_text())
    for path in run.rglob('*.json'):
        if path.name == 'driver-service.json' or ('services' in path.parts and not path.name.endswith('.stopped.json')):
            for record in json.loads(path.read_text()):
                assert not services.group_members(record), path
    add_tree(run, str(run.relative_to(work)))
    scratch = Path(config['deployment']['scratch_root']) / run.name
    for path in scratch.rglob('*'):
        if path.is_file() and not path.is_symlink() and ('events' in path.relative_to(scratch).parts
                or path.name in ('stdout', 'stderr') or path.suffix == '.log'):
            files[path] = str(Path('scratch') / run.name / path.relative_to(scratch))
assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
(directory / 'lifecycle-audit.json').write_text(json.dumps({'all_recorded_processes_exited': True,
    'gpu_compute_processes': [], 'records': audit, 'instance_retained': True}, indent=2) + '\n')
add_tree(directory, 'critical-path')
for name in ('owner.json', 'deployment.json', 'host-prerequisites.json'):
    path = work / 'evidence' / name
    files[path] = 'evidence/' + name
archive = work / 'evidence/critical-path-complete.tar.gz'
assert not archive.exists()
manifest = []
with tarfile.open(archive, 'w:gz', compresslevel=1) as target:
    for path, name in sorted(files.items(), key=lambda x: x[1]):
        before = path.stat()
        target.add(path, arcname=name, recursive=False)
        assert path.stat().st_size == before.st_size and path.stat().st_mtime_ns == before.st_mtime_ns
        manifest.append({'name': name, 'bytes': before.st_size})
digest = hashlib.sha256()
with archive.open('rb') as stream:
    for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
        digest.update(block)
receipt = {'archive': archive.name, 'bytes': archive.stat().st_size, 'sha256': digest.hexdigest(),
           'files': manifest, 'source_revision': subprocess.check_output(
               ['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()}
(work / 'evidence/critical-path-complete-manifest.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(archive, receipt['bytes'], receipt['sha256'])
