"""Check decimal probe results, schemas, duplicates, nulls and empty inputs on CPU."""
import argparse
from collections import Counter
from decimal import Decimal
import json
import os
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession
from ceiling_variants import cases, prepare_frame
from toy_oracle import SCHEMAS, fixture
from variants import original


def result(spark, sql):
    frame = spark.sql(sql)
    return Counter(tuple(row) for row in frame.collect()), frame.schema.jsonValue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    report = {'status': 'running', 'checks': [], 'mutations': [],
              'scope': 'CPU semantic counterexamples; full-scale GPU qualification remains required'}
    with tempfile.TemporaryDirectory(prefix='scaleup-ceiling-cpu-') as scratch:
        os.environ['SPARK_LOCAL_DIRS'] = scratch
        spark = (SparkSession.builder.master('local[2]').appName('scaleup-ceiling-decimal-oracle')
            .config('spark.plugins', '').config('spark.sql.extensions', '')
            .config('spark.sql.shuffle.partitions', '2').config('spark.local.dir', scratch)
            .config('spark.sql.adaptive.enabled', 'false').config('spark.sql.ansi.enabled', 'false')
            .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
        data = fixture()
        try:
            for mode in ('duplicates-null', 'empty-lineitem', 'large-signed-decimals'):
                for table, rows in data.items():
                    values = rows
                    if table == 'lineitem' and mode == 'empty-lineitem':
                        values = []
                    elif table == 'lineitem' and mode == 'large-signed-decimals':
                        values = []
                        for index, row in enumerate(rows):
                            row = list(row)
                            row[5] = Decimal('9999999999999.99') * (-1 if index % 3 == 0 else 1)
                            row[6] = Decimal('0.07')
                            values.append(tuple(row))
                    spark.createDataFrame(values, SCHEMAS[table]).createOrReplaceTempView(table)
                for case in cases():
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
            for table, rows in data.items():
                spark.createDataFrame(rows, SCHEMAS[table]).createOrReplaceTempView(table)
            for case in cases():
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
