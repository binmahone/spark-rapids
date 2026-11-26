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

"""Check decimal probe results, schemas, duplicates, nulls and empty inputs on CPU."""
import argparse
from collections import Counter
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
import subprocess

from pyspark.sql import SparkSession
from ceiling_variants import cases, prepare_frame, refinements, q11_total_reuse
from toy_oracle import SCHEMAS, fixture
from variants import original


def result(spark, sql):
    frame = spark.sql(sql)
    return Counter(tuple(row) for row in frame.collect()), frame.schema.jsonValue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--refinements', action='store_true')
    group.add_argument('--q11', action='store_true')
    args = parser.parse_args()
    requested = [q11_total_reuse()] if args.q11 else refinements() if args.refinements else cases()
    report = {'status': 'running', 'checks': [], 'mutations': [],
              'scope': 'CPU semantic counterexamples; full-scale GPU qualification remains required'}
    here = Path(__file__).resolve().parent
    report['source_revision'] = subprocess.check_output(
        ['git', '-C', str(here), 'rev-parse', 'HEAD'], text=True).strip()
    report['source_sha256'] = {name: hashlib.sha256((here / name).read_bytes()).hexdigest()
        for name in ('ceiling_oracle.py', 'ceiling_variants.py', 'toy_oracle.py', 'variants.py')}
    with tempfile.TemporaryDirectory(prefix='scaleup-ceiling-cpu-') as scratch:
        os.environ['SPARK_LOCAL_DIRS'] = scratch
        spark = (SparkSession.builder.master('local[2]').appName('scaleup-ceiling-decimal-oracle')
            .config('spark.plugins', '').config('spark.sql.extensions', '')
            .config('spark.sql.shuffle.partitions', '2').config('spark.local.dir', scratch)
            .config('spark.sql.adaptive.enabled', 'false').config('spark.sql.ansi.enabled', 'false')
            .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
        report['spark_version'] = spark.version
        data = fixture()
        try:
            modes = ['duplicates-null', 'empty-lineitem', 'large-signed-decimals']
            if args.q11:
                modes.append('threshold-tie')
            for mode in modes:
                for table, rows in data.items():
                    values = rows
                    if (table == 'lineitem' or args.q11 and table == 'partsupp') and mode == 'empty-lineitem':
                        values = []
                    elif table == 'lineitem' and mode == 'large-signed-decimals':
                        values = []
                        for index, row in enumerate(rows):
                            row = list(row)
                            row[5] = Decimal('9999999999999.99') * (-1 if index % 3 == 0 else 1)
                            row[6] = Decimal('0.07')
                            values.append(tuple(row))
                    if args.q11 and table == 'partsupp' and mode == 'large-signed-decimals':
                        values = [(*row[:3], Decimal('9999999999999.99') * (-1 if i % 3 == 0 else 1), row[4])
                                  for i, row in enumerate(rows)]
                    if args.q11 and mode == 'threshold-tie':
                        if table == 'nation':
                            values = [(888, 'GERMANY', 0, '')]
                        elif table == 'supplier':
                            values = [(888, 'S', 'A', 888, 'P', Decimal('0'), '')]
                        elif table == 'partsupp':
                            values = [(1, 888, 1, Decimal('0.02'), ''),
                                      (2, 888, 1, Decimal('999999.99'), ''),
                                      (3, 888, 1, Decimal('999999.99'), ''),
                                      (None, 888, 1, None, '')]
                    spark.createDataFrame(values, SCHEMAS[table]).createOrReplaceTempView(table)
                for case in requested:
                    expected, schema = result(spark, original(case.query))
                    frame = prepare_frame(spark, case)
                    observed = Counter(tuple(row) for row in frame.collect())
                    candidate_schema = frame.schema.jsonValue()
                    assert expected == observed, (mode, case.name, expected - observed, observed - expected)
                    assert schema == candidate_schema, (mode, case.name, schema, candidate_schema)
                    if mode == 'duplicates-null':
                        assert expected, ('Positive output is required', case.name)
                    report['checks'].append({'query': case.query, 'variant': case.name,
                        'fixture': mode, 'rows': sum(expected.values()), 'status': 'passed'})
            if args.q11:
                case = q11_total_reuse()
                mutant = case.sql.replace('SUM(value) OVER ()', 'MAX(value) OVER ()')
                assert result(spark, original(11))[0] != result(spark, mutant)[0]
                report['mutations'].append({'query': 11, 'mutation': 'Use MAX instead of the shared total SUM at a threshold tie',
                                            'detected': True})
            for table, rows in data.items():
                spark.createDataFrame(rows, SCHEMAS[table]).createOrReplaceTempView(table)
            for case in requested:
                if case.query not in (3, 10, 18):
                    continue
                variable = 'probe_quantity' if case.query == 18 else 'probe_revenue'
                mutant = case.sql.replace('SUM(' + variable + ')', 'MAX(' + variable + ')')
                assert mutant != case.sql
                assert result(spark, original(case.query))[0] != result(spark, mutant)[0], case.name
                report['mutations'].append({'query': case.query,
                    'mutation': 'Discard duplicate join multiplicities with MAX', 'detected': True})
            report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', error=repr(error))
            raise
        finally:
            Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
            spark.stop()


if __name__ == '__main__':
    main()
