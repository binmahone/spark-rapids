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

"""Verify packaged identities and write the runtime's immutable artifact manifest."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

suite = Path(__file__).resolve().parents[1]
root = suite.parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--arch', choices=('arm64', 'amd64'), required=True)
args = parser.parse_args()
directory = root / '.build/artifacts' / args.arch
engine = next(directory.glob('cudf-spark_*.jar'))
with zipfile.ZipFile(engine) as archive:
    names = archive.namelist()
    assert not any('NativeReaderBinding' in n or 'forecast_native_fileio' in n for n in names)
    assert 'com/nvidia/spark/rapids/jni/NativeFileSource.class' in names
    components = {}
    for name in ('rapids4spark-version-info.properties', 'cudf-spark-jni-version-info.properties',
                 'cudf-java-version-info.properties', 'cudf-spark-private-version-info.properties'):
        matching = [n for n in names if n.endswith('/' + name) or n == name]
        assert len(matching) == 1, (name, matching)
        components[name] = dict(line.split('=', 1) for line in archive.read(matching[0]).decode().splitlines()
                                if '=' in line and not line.startswith('#'))
lock = json.loads((suite / 'configs/sources.lock.json').read_text())
assert components['cudf-spark-private-version-info.properties']['revision'] == (
    lock['private_optimizer']['commit'])
revision = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                                   text=True).strip()
assert components['rapids4spark-version-info.properties']['revision'] == revision
files = {}
for path in (engine, directory / 'benchmark-tools.jar'):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    files[path.name] = {'bytes': path.stat().st_size, 'sha256': digest.hexdigest()}
manifest = {'architecture': args.arch, 'files': files, 'components': components,
            'source_revision': revision, 'upstream_main': lock['public_base']['commit'],
            'jni_maven_version': lock['jni']['maven_version'],
            'experimental_cleanup': True,
            'source_dirty': bool(subprocess.check_output(
                ['git', '-C', str(root), 'status', '--porcelain'], text=True).strip())}
(directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(manifest, indent=2))
