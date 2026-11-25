#!/usr/bin/env python3
"""Archive execution evidence while excluding datasets, binaries, and worker caches."""
import argparse
from pathlib import Path
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
parser.add_argument('--name', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
if Path(args.name).name != args.name:
    raise ValueError('Archive name must be a simple filename stem')
archive = work / 'evidence' / (args.name + '.tar.gz')
if archive.exists():
    raise RuntimeError('Evidence archive exists; use a new snapshot name')
manifest = []
paths = {}
for name in ['baseline', 'screen', 'confirmation', 'followups']:
    for path in (work / name).rglob('*'):
        if path.is_file() and not path.is_symlink():
            paths[path] = str(path.relative_to(work))
for path in (work / 'evidence').iterdir():
    if path.is_file() and path.suffix in ('.json', '.jsonl', '.log', '.exit'):
        paths[path] = str(path.relative_to(work))
configs = [work / 'baseline/config.json'] + list((work / 'screen').glob('*/config.json'))
for config_path in configs:
    if not config_path.exists():
        continue
    config = read(config_path)
    scratch = Path(config['deployment']['scratch_root']) / config_path.parent.name
    for path in scratch.rglob('*'):
        if (path.is_file() and not path.is_symlink() and
                ('events' in path.relative_to(scratch).parts or
                 path.name in ('stdout', 'stderr') or path.suffix == '.log')):
            paths[path] = str(Path('scratch') / config_path.parent.name / path.relative_to(scratch))
with tarfile.open(archive, 'w:gz', compresslevel=1) as target:
    for path, name in sorted(paths.items(), key=lambda item: item[1]):
        size = path.stat().st_size
        target.add(path, arcname=name, recursive=False)
        manifest.append({'name': name, 'bytes': size})
digest = sha(archive)
write(work / 'evidence' / (args.name + '-manifest.json'), {
    'archive': archive.name, 'bytes': archive.stat().st_size,
    'sha256': digest, 'files': manifest,
    'exclusions': ['Dataset', 'JAR/native binaries', 'Worker dependency and spill caches'],
    'instance_policy': 'Evidence snapshot does not stop or release the host'})
print(archive, archive.stat().st_size, digest)
