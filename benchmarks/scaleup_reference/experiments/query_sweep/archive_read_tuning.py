#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
"""Audit and retain closed read-tuning diagnostics without releasing the instance."""
import argparse
from pathlib import Path
import sys
import os
import re
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True)
a = p.parse_args()
work = Path(a.work).resolve()
directory = work / 'read-tuning'
states = sorted(directory.glob('*/state.json'))
assert len(states) >= 2 and (directory / 'A0/state.json').exists() and (directory / 'B1/state.json').exists()
lifecycle = read(directory / 'lifecycle-audit.json')
assert not lifecycle['live_recorded_groups'] and not lifecycle['gpu_compute_processes']
assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
revision = subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()
profile_audit = []
for state_path in states:
    state = read(state_path)
    assert state['status'] == 'completed' and state['gpm_restored']
    for record in [read(state_path.parent / 'controller.json'), *state['profilers']]:
        path = Path('/proc') / str(record['pid']) / 'stat'
        live = path.exists() and path.read_text().split(') ', 1)[1].split()[19] == str(record['start_time'])
        profile_audit.append({'pid': record['pid'], 'start_time': record['start_time'], 'live': live})
        assert not live
    for path in state_path.parent.glob('*.data'):
        assert path.is_file() and not path.is_symlink()
        subprocess.run(['sudo', '-n', 'chown', str(os.getuid()) + ':' + str(os.getgid()), str(path)], check=True)
    analysis_path = state_path.parent / 'analysis.json'
    if analysis_path.exists():
        analysis = read(analysis_path)
        analysis['finalizer_revision'] = revision
        old = 'Reclaim stacks sample every 257 events per CPU; counts are not exact event totals'
        new = 'The requested reclaim sample period was 257, but this kernel emitted approximately one record per allocstall; never multiply trace counts by 257'
        analysis['limits'] = [new if value == old else value for value in analysis['limits']]
        log = state_path.parent / 'reclaim.log'
        if log.exists():
            observed = int(re.search(r'\((\d+) samples\)', log.read_text())[1])
            import json
            rows = [json.loads(line) for line in (state_path.parent / 'vm.jsonl').open()]
            delta = sum(rows[-1]['vmstat'][key] - rows[0]['vmstat'][key]
                        for key in rows[0]['vmstat'] if key.startswith('allocstall'))
            analysis['reclaim_capture_calibration'] = {'requested_period': 257,
                'recorded_samples': observed, 'host_allocstall_delta': delta,
                'interpretation': 'Trace records were effectively not downsampled; count raw records and retain profiling overhead caveat'}
        write(analysis_path, analysis)
write(directory / 'profiler-audit.json', {'records': profile_audit, 'source_revision': revision,
    'all_stopped': True, 'watermark_scale_factor': 10, 'retention': 'Explicit user release only'})
archive = work / 'evidence/read-tuning-complete.tar.gz'
assert not archive.exists()
paths = {path: str(path.relative_to(work)) for path in directory.rglob('*')
         if path.is_file() and not path.is_symlink()}
for state_path in states:
    state = read(state_path)
    phase = state['phase']
    assert state['status'] == 'completed'
    assert (work / 'evidence' / ('read-tuning-' + phase + '.exit')).read_text().strip() == '0'
    for group in state['groups']:
        run = Path(group['run'])
        assert run.parent == work / 'screen' and run.name.startswith('read-tuning-')
        for path in run.rglob('*'):
            if path.is_file() and not path.is_symlink():
                paths[path] = str(path.relative_to(work))
        config = read(run / 'config.json')
        scratch = Path(config['deployment']['scratch_root']) / run.name
        for path in scratch.rglob('*'):
            if path.is_file() and not path.is_symlink() and (
                    'events' in path.relative_to(scratch).parts or
                    path.name in ('stdout', 'stderr') or path.suffix == '.log'):
                paths[path] = str(Path('scratch') / run.name / path.relative_to(scratch))
    for suffix in ('.log', '.exit'):
        path = work / 'evidence' / ('read-tuning-' + phase + suffix)
        paths[path] = str(path.relative_to(work))
for name in ('owner.json', 'deployment.json', 'host-prerequisites.json'):
    path = work / 'evidence' / name
    paths[path] = str(path.relative_to(work))
manifest = []
with tarfile.open(archive, 'w:gz', compresslevel=1) as target:
    for path, name in sorted(paths.items(), key=lambda item: item[1]):
        stat = path.stat()
        target.add(path, arcname=name, recursive=False)
        assert path.stat().st_mtime_ns == stat.st_mtime_ns and path.stat().st_size == stat.st_size
        manifest.append({'name': name, 'bytes': stat.st_size})
write(work / 'evidence/read-tuning-complete-manifest.json', {
    'archive': archive.name, 'bytes': archive.stat().st_size, 'sha256': sha(archive), 'files': manifest,
    'instance_policy': 'Retain until explicit user release'})
print(archive, archive.stat().st_size, sha(archive))
