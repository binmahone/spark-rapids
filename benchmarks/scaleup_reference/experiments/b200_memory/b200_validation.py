"""Compare against this dataset's retained GPU baseline, including Q18 cutoff ties."""
from decimal import Decimal
from pathlib import Path
from common import read, write
from validate_q18_topn import validate_topn


class Validator:
    def __init__(self, work):
        self.root = Path(work) / 'evidence/references'
        self.q18 = None

    @staticmethod
    def write_q18_reference(work, schema, values):
        if len(values) < 100:
            raise AssertionError('Expected at least 100 Q18 rows')
        key = lambda row: (-Decimal(row[4]), row[3])
        if len(values) == 10000 and key(values[-1]) <= key(values[99]):
            raise AssertionError('Reference prefix does not contain the full cutoff tie')
        target = Path(work) / 'evidence/references/q18.json'
        if target.exists():
            raise AssertionError('Q18 reference exists')
        write(target, {'schema': schema, 'rows': values,
                       'independent_cpu_reference': False, 'cutoff_tie_complete': True})

    def validate(self, query, rows, schema, sql_sha, digest, jvm, unused=None):
        if query == 18:
            if self.q18 is None:
                self.q18 = read(self.root / 'q18.json')
            values = [[None if row.isNullAt(i) else str(row.get(i))
                       for i in range(len(schema['fields']))] for row in rows]
            result = validate_topn({'schema': schema, 'rows': values,
                'all_rows_retained': True, 'result_rows': len(values)},
                self.q18['rows'], self.q18['schema'])
            return {'status': 'passed', 'details': result,
                    'reference': 'GPU baseline with complete cutoff ties'}
        if query != 20:
            raise AssertionError('Unsupported query')
        actual = {'schema': schema, 'rows': len(rows),
                  'sum': str(digest[2]), 'xor': str(digest[3])}
        path = self.root / 'q20.json'
        generated = not path.exists()
        if generated:
            write(path, actual)
        elif read(path) != actual:
            raise AssertionError('Complete Q20 result fingerprint differs from baseline')
        return {'status': 'reference-generated' if generated else 'passed',
                'method': 'complete multiset fingerprints', 'independent_cpu_reference': False}
