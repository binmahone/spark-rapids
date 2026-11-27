#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Preserve completed or failed diagnostic attempts after verifying owned processes exited."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
import services


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    args = parser.parse_args()
    work = Path(args.work).resolve()
    directory = work / 'q12-diagnosis'
    audit = []
    for path in directory.glob('*/state.json'):
        state = read(path)
        assert state['status'] in ('completed', 'failed'), path
        for record in [read(path.parent / 'controller.json'), *state['profilers']]:
            proc = Path('/proc') / str(record['pid']) / 'stat'
            live = proc.exists() and proc.read_text().split(') ', 1)[1].split()[19] == str(record['start_time'])
            assert not live, record
            audit.append({'pid': record['pid'], 'start_time': record['start_time'], 'live': live})
        for artifact in path.parent.glob('*'):
            if artifact.is_file() and not artifact.is_symlink() and artifact.stat().st_uid == 0:
                subprocess.run(['sudo', '-n', 'chown', f'{os.getuid()}:{os.getgid()}', str(artifact)], check=True)
    for path in (work / 'screen').rglob('*.json'):
        if path.name == 'driver-service.json' or ('services' in path.parts and not path.name.endswith('.stopped.json')):
            for record in read(path):
                assert not services.group_members(record), path
    gpu = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    assert not gpu, gpu
    write(directory / 'lifecycle-audit.json', {'all_recorded_processes_exited': True,
          'gpu_compute_processes': [], 'records': audit})
    archive = work / 'evidence/q12-diagnosis-complete.tar.gz'
    assert not archive.exists()
    files = {}
    for root in [directory, work / 'screen', work / 'evidence']:
        for path in root.rglob('*'):
            if path.is_file() and not path.is_symlink() and path != archive:
                files[path] = str(path.relative_to(work))
    for config in (work / 'screen').glob('*/config.json'):
        run = config.parent
        scratch = Path(read(config)['deployment']['scratch_root']) / run.name
        for path in scratch.rglob('*'):
            if path.is_file() and not path.is_symlink() and (
                    'events' in path.relative_to(scratch).parts or
                    path.name in ('stdout', 'stderr') or path.suffix == '.log'):
                files[path] = str(Path('scratch') / run.name / path.relative_to(scratch))
    manifest = []
    with tarfile.open(archive, 'w:gz', compresslevel=1) as target:
        for path, name in sorted(files.items(), key=lambda item: item[1]):
            stat = path.stat()
            target.add(path, arcname=name, recursive=False)
            assert path.stat().st_mtime_ns == stat.st_mtime_ns and path.stat().st_size == stat.st_size
            manifest.append({'name': name, 'bytes': stat.st_size})
    receipt = {'archive': archive.name, 'bytes': archive.stat().st_size,
               'sha256': sha(archive), 'files': manifest,
               'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()}
    write(work / 'evidence/q12-diagnosis-complete-manifest.json', receipt)
    print(archive, receipt['bytes'], receipt['sha256'])


if __name__ == '__main__':
    main()
