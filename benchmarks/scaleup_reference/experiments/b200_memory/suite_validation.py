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

"""Retain complete GPU references; compare exact identities and bounded float differences."""
from collections import defaultdict
import json
import math
from pathlib import Path
from common import read, write
from b200_validation import Validator


class SuiteValidator:
    def __init__(self, work):
        self.root = Path(work) / 'evidence/suite-references'
        self.q18 = Validator(work)

    def validate(self, query, rows, schema, sql_sha, digest, jvm, unused=None):
        if query == 18:
            return self.q18.validate(query, rows, schema, sql_sha, digest, jvm)
        actual = {'schema': schema, 'rows': len(rows), 'sql_sha256': sql_sha,
                  'sum': str(digest[2]), 'xor': str(digest[3])}
        path = self.root / f'q{query}.json'
        generated = not path.exists()
        if query == 11:
            binary = self.root / 'q11.bin'
            if generated:
                path.parent.mkdir(parents=True, exist_ok=True)
                actual['binary_sha256'] = str(jvm.com.nvidia.spark.rapids.benchmark.FullResultReference.write(rows, str(binary)))
                write(path, actual)
            else:
                reference = read(path)
                if any(reference[k] != actual[k] for k in ('schema', 'rows', 'sql_sha256')):
                    raise AssertionError('Q11 schema, row count or original SQL differs')
                details = json.loads(str(jvm.com.nvidia.spark.rapids.benchmark.FullResultReference.compare(
                    rows, str(binary), reference['binary_sha256'])))
                return dict(details, method='complete Q11 key/value comparison',
                            independent_cpu_reference=False, exact_fingerprint_equal=(
                                reference['sum'] == actual['sum'] and reference['xor'] == actual['xor']))
        elif query == 20:
            if generated:
                write(path, actual)
            elif read(path) != actual:
                raise AssertionError('Complete Q20 multiset fingerprint differs')
        else:
            if len(rows) > 100000:
                raise AssertionError('A complete large-result comparator is required')
            values = [[None if row.isNullAt(i) else str(row.get(i))
                       for i in range(len(schema['fields']))] for row in rows]
            actual['values'] = values
            if generated:
                write(path, actual)
            else:
                reference = read(path)
                if any(reference[k] != actual[k] for k in ('schema', 'rows', 'sql_sha256')):
                    raise AssertionError(f'Q{query}: schema, row count or original SQL differs')
                if reference['sum'] != actual['sum'] or reference['xor'] != actual['xor']:
                    floating = [i for i,f in enumerate(schema['fields']) if f['type'] in ('double','float')]
                    exact = [i for i in range(len(schema['fields'])) if i not in floating]
                    def grouped(values):
                        groups = defaultdict(list)
                        for row in values:
                            groups[tuple(row[i] for i in exact)].append(tuple(row[i] for i in floating))
                        return groups
                    expected, observed = grouped(reference['values']), grouped(values)
                    if expected.keys() != observed.keys():
                        raise AssertionError(f'Q{query}: exact row identities differ')
                    max_abs = max_rel = 0.0
                    for key, expected_values in expected.items():
                        observed_values = observed[key]
                        if len(expected_values) != len(observed_values):
                            raise AssertionError('Row multiplicity differs')
                        sort_key = lambda row: tuple((v is not None, float(v) if v is not None else 0) for v in row)
                        for a,b in zip(sorted(expected_values, key=sort_key), sorted(observed_values, key=sort_key)):
                            for x,y in zip(a,b):
                                if x is None or y is None:
                                    if x != y:
                                        raise AssertionError('NULL placement differs')
                                    continue
                                x,y = float(x),float(y)
                                if not math.isfinite(x) or not math.isfinite(y) or not math.isclose(x,y,rel_tol=1e-8,abs_tol=.01):
                                    raise AssertionError(f'Q{query}: floating output differs: {x} vs {y}')
                                max_abs = max(max_abs,abs(x-y))
                                max_rel = max(max_rel,abs(x-y)/max(abs(x),abs(y),1e-300))
                    return {'status': 'passed', 'method': 'all rows; exact non-floats, float rel_tol=1e-8 abs_tol=0.01',
                            'independent_cpu_reference': False, 'exact_fingerprint_equal': False,
                            'max_absolute_error': max_abs, 'max_relative_error': max_rel}
        return {'status': 'reference-generated' if generated else 'passed',
                'method': 'complete GPU baseline reference', 'independent_cpu_reference': False,
                'exact_fingerprint_equal': not generated}
