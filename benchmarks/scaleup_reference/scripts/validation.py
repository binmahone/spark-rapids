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

"""Validate full outputs against retained references outside the SQL timer."""
from collections import Counter
import json

from common import SUITE, read
from q3_tie_gate import validate as validate_q3
from validate_q18_topn import validate_topn
from validate_nvl_result import validate_result


class Validator:
    def __init__(self, platform):
        self.platform = platform
        self.root = SUITE / 'inputs' / platform
        if platform == 'aws48x':
            self.catalog = read(self.root / 'frozen-reference-catalog.json')['references']
            self.contracts = read(self.root / 'query-contracts.json')['queries']
            self.q3 = read(self.root / 'references/q3-cutoff-rows.json')
            self.q18 = None

    def validate(self, query, rows, schema, sql_sha, digest, jvm, q11_reference=None):
        if self.platform == 'nvl72':
            references = [read(self.root / 'references' / f'q{query}.iter{i}.result.json')
                          for i in (1, 2, 3)]
            def typed(row):
                return [{'type': field['type'], 'value': None if row.isNullAt(i) else
                         str(row.get(i)).lower() if field['type'] == 'boolean' else str(row.get(i))}
                        for i, field in enumerate(schema['fields'])]
            count = len(rows)
            window = min(100 if query == 11 else 10, count)
            result = {'rows': count, 'typed_first_rows': [typed(r) for r in rows[:min(10, count)]],
                      'typed_last_rows': [typed(r) for r in rows[max(0, count - 10):]],
                      'multiset_sha256_sum': str(digest[2]), 'multiset_sha256_xor': str(digest[3])}
            if count <= 1000:
                result['typed_all_rows'] = [typed(r) for r in rows]
            if query == 11:
                if q11_reference is None:
                    raise AssertionError('Q11 requires a complete non-hybrid reference')
                reference = read(q11_reference + '.json')
                if reference['schema'] != schema or reference['sql_sha256'] != sql_sha:
                    raise AssertionError('Q11 reference schema or SQL differs')
                result = json.loads(str(jvm.com.nvidia.spark.rapids.benchmark.FullResultReference.compare(
                    rows, q11_reference + '.bin', reference['sha256'])))
                return result
            return validate_result(query, result, references)
        contract = self.contracts[str(query)]
        if contract['sql_sha256'] != sql_sha or contract['schema'] != schema:
            raise AssertionError('Reference SQL or output schema differs')
        if contract['result_rows'] != len(rows):
            raise AssertionError('Reference output row count differs')
        if query in (11, 20):
            expected = self.catalog[str(query)]['record']['result_digest']
            if [len(rows), str(digest[2]), str(digest[3])] != [expected['row_count'],
                    expected['multiset_sha256_sum'], expected['multiset_sha256_xor']]:
                raise AssertionError('Complete-result fingerprints differ')
            return {'status': 'passed', 'method': 'complete multiset fingerprints'}
        values = [[None if row.isNullAt(i) else str(row.get(i)) for i in range(len(schema['fields']))]
                  for row in rows]
        if query == 3:
            return {'status': 'passed', 'details': validate_q3(self.q3, values)}
        if query == 18:
            if self.q18 is None:
                self.q18 = [json.loads(row) for row in read(
                    self.root / 'references/q18-complete-rows.json.gz')]
            record = {'schema': schema, 'all_rows_retained': True,
                      'rows': values, 'result_rows': len(rows)}
            return {'status': 'passed', 'details': validate_topn(record, self.q18,
                    read(self.root / 'references/q18-metadata.json')['schema'])}
        if values != self.catalog[str(query)]['record']['rows']:
            raise AssertionError('Complete ordered rows differ')
        return {'status': 'passed', 'method': 'complete ordered rows'}
