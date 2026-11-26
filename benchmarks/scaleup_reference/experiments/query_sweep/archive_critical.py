"""Retain closed critical-query evidence and logs without releasing the host."""
import argparse
from pathlib import Path
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True)
a = p.parse_args()
work = Path(a.work).resolve()
directory = work / 'critical'
states = sorted(directory.glob('*-state.json'))
assert states and (directory / 'confirm-state.json').exists()
lifecycle = read(directory / 'lifecycle-audit.json')
assert not lifecycle['live_recorded_groups'] and not lifecycle['gpu_compute_processes']
archive = work / 'evidence/critical-complete.tar.gz'
assert not archive.exists()
paths = {path: str(path.relative_to(work)) for path in directory.rglob('*')
         if path.is_file() and not path.is_symlink()}
for state_path in states:
    state = read(state_path)
    phase = state['phase']
    assert state['status'] == 'completed'
    assert (work / 'evidence' / ('critical-' + phase + '.exit')).read_text().strip() == '0'
    for group in state['groups']:
        run = Path(group['run'])
        assert run.parent == work / 'screen' and run.name.startswith('critical-')
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
        path = work / 'evidence' / ('critical-' + phase + suffix)
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
write(work / 'evidence/critical-complete-manifest.json', {
    'archive': archive.name, 'bytes': archive.stat().st_size, 'sha256': sha(archive), 'files': manifest,
    'instance_policy': 'Retain until explicit user release'})
print(archive, archive.stat().st_size, sha(archive))
