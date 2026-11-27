#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Archive selected SOL applications after verifying their owned processes have exited."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
import services


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--phases', nargs='+', required=True)
    p.add_argument('--name', default='sol-suite-complete')
    a = p.parse_args()
    assert a.name and all(c.isalnum() or c == '-' for c in a.name)
    work = a.work.resolve()
    files, audit = {}, []

    def add_tree(root, prefix):
        for path in root.rglob('*'):
            if path.is_file() and not path.is_symlink():
                files[path] = str(Path(prefix) / path.relative_to(root))

    for phase in a.phases:
        assert phase in ('S0', 'S1', 'S2', 'S3', 'R0', 'R1', 'R2', 'R3',
                         'K0', 'K1', 'K2', 'K3')
        directory = work / 'critical-path' / phase
        state = read(directory / 'state.json')
        assert state['status'] in ('completed', 'failed')
        for record in [read(directory / 'controller.json'), *state['profilers']]:
            stat = Path('/proc') / str(record['pid']) / 'stat'
            assert not stat.exists() or stat.read_text().split(') ', 1)[1].split()[19] != str(record['start_time'])
            audit.append({'pid': record['pid'], 'start_time': record['start_time'], 'live': False})
        add_tree(directory, 'critical-path/' + phase)
        run = work / 'screen' / ('critical-path-' + phase + '-0')
        config = read(run / 'config.json')
        for path in run.rglob('*.json'):
            if path.name == 'driver-service.json' or ('services' in path.parts and not path.name.endswith('.stopped.json')):
                for record in read(path):
                    assert not services.group_members(record), path
        add_tree(run, str(run.relative_to(work)))
        scratch = Path(config['deployment']['scratch_root']) / run.name
        for path in scratch.rglob('*'):
            if path.is_file() and not path.is_symlink() and ('events' in path.relative_to(scratch).parts
                    or path.name in ('stdout', 'stderr') or path.suffix == '.log'):
                files[path] = str(Path('scratch') / run.name / path.relative_to(scratch))
        for path in (work / 'evidence').glob('critical-path-' + phase + '.*'):
            files[path] = 'evidence/' + path.name
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    lifecycle = work / 'critical-path/sol-lifecycle-audit.json'
    write(lifecycle, {'all_recorded_processes_exited': True, 'gpu_compute_processes': [],
                      'records': audit, 'instance_retained': True})
    for path in (work / 'critical-path').glob('sol-*.json'):
        files[path] = 'critical-path/' + path.name
    for name in ('owner.json', 'deployment.json', 'host-prerequisites.json', 'sol-domain.log', 'sol-domain.exit'):
        path = work / 'evidence' / name
        assert path.exists()
        files[path] = 'evidence/' + name
    for name in ('critical/oracle.json', 'critical-path/oracle.json'):
        files[work / name] = name
    archive = work / 'evidence' / (a.name + '.tar.gz')
    assert not archive.exists()
    manifest = []
    with tarfile.open(archive, 'w:gz', compresslevel=1) as target:
        for path, name in sorted(files.items(), key=lambda item: item[1]):
            before = path.stat()
            target.add(path, arcname=name, recursive=False)
            after = path.stat()
            assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
            manifest.append({'name': name, 'bytes': before.st_size})
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            digest.update(block)
    receipt = {'archive': archive.name, 'bytes': archive.stat().st_size, 'sha256': digest.hexdigest(),
               'phases': a.phases, 'files': manifest, 'source_revision': subprocess.check_output(
                   ['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()}
    write(work / 'evidence' / (a.name + '-manifest.json'), receipt)
    print(archive, receipt['bytes'], receipt['sha256'])


if __name__ == '__main__':
    main()
