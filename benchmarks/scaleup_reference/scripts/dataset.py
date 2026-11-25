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

"""Check file completeness and reject visible non-Parquet files before Spark lists a table."""
import argparse
import json
from pathlib import Path

from common import SUITE, TABLES, read


def verify(dataset, expected, tables=TABLES):
    root = Path(dataset)
    records = {}
    for table in tables:
        directory = root / table
        if not directory.is_dir():
            raise ValueError('Dataset table is unavailable: ' + table)
        files = [p for p in directory.rglob('*') if p.is_file() and
                 not any(part.startswith(('_', '.')) for part in p.relative_to(directory).parts)]
        unexpected = [str(p) for p in files if p.suffix != '.parquet']
        if unexpected:
            raise ValueError('Spark would read unexpected table files: ' + repr(unexpected[:10]))
        if not files:
            raise ValueError('Dataset table contains no Parquet files: ' + table)
        for path in files:
            with path.open('rb') as stream:
                head = stream.read(4)
                stream.seek(-4, 2)
                tail = stream.read(4)
            if head != b'PAR1' or tail != b'PAR1':
                raise ValueError('Parquet magic differs: ' + str(path))
        records[table] = {'files': len(files), 'bytes': sum(p.stat().st_size for p in files)}
    count = sum(v['files'] for v in records.values())
    size = sum(v['bytes'] for v in records.values())
    if count != expected['parquet_files'] or size != expected['parquet_bytes']:
        raise ValueError(f'Dataset file inventory differs: {count} files, {size} bytes')
    return {'files': count, 'bytes': size, 'tables': records,
            'verification': 'Visible file names, sizes and Parquet magic; no full-payload hash'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    args = parser.parse_args()
    expected = read(SUITE / 'configs/aws48x.json')['dataset_contract']
    print(json.dumps(verify(args.dataset, expected), indent=2))
