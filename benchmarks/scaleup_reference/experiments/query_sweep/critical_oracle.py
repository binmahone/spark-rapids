"""Check targeted SQL semantics and prove the fixed dataset permits integer narrowing."""
import argparse
from collections import Counter
from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession
from critical_variants import cases
from toy_oracle import SCHEMAS, fixture
from variants import original


def value(spark, sql):
    frame = spark.sql(sql)
    return Counter(tuple(row) for row in frame.collect()), frame.schema.jsonValue()


def bounds(spark, dataset):
    records = []
    files = sorted((Path(dataset) / 'lineitem').rglob('*.parquet'))
    assert files, 'No lineitem parquet files'
    for path in files:
        before = path.stat()
        footer = spark._jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
            spark._jsc.hadoopConfiguration(), spark._jvm.org.apache.hadoop.fs.Path(str(path)))
        blocks = footer.getBlocks().iterator()
        extrema = []
        groups = 0
        while blocks.hasNext():
            block = blocks.next()
            columns = block.getColumns().iterator()
            found = False
            while columns.hasNext():
                column = columns.next()
                if str(column.getPath().toDotString()) == 'l_suppkey':
                    found = True
                    stat = column.getStatistics()
                    if stat.hasNonNullValue():
                        low, high = int(stat.genericGetMin()), int(stat.genericGetMax())
                        assert -(2**31) <= low <= high < 2**31, (str(path), low, high)
                        extrema.extend([low, high])
                    else:
                        assert stat.isNumNullsSet() and stat.getNumNulls() == block.getRowCount()
            assert found
            groups += 1
        after = path.stat()
        assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
        records.append({'path': str(path), 'bytes': before.st_size, 'mtime_ns': before.st_mtime_ns,
                        'row_groups': groups, 'min': min(extrema) if extrema else None,
                        'max': max(extrema) if extrema else None})
    return {'status': 'passed', 'dataset': str(dataset), 'files': records,
            'contract': 'Every non-null l_suppkey fits signed int32 according to all parquet row-group bounds; reject changed files'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--dataset', required=True)
    args = parser.parse_args()
    report = {'status': 'running', 'checks': [], 'mutations': []}
    with tempfile.TemporaryDirectory(prefix='critical-cpu-') as scratch:
        os.environ['SPARK_LOCAL_DIRS'] = scratch
        spark = (SparkSession.builder.master('local[2]').appName('critical-query-cpu-oracle')
            .config('spark.plugins', '').config('spark.sql.extensions', '')
            .config('spark.sql.shuffle.partitions', '2').config('spark.sql.adaptive.enabled', 'false')
            .config('spark.sql.ansi.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
            .config('spark.local.dir', scratch).getOrCreate())
        try:
            data = fixture()
            order = next(row for row in data['orders'] if row[1] in (3, 4) and
                         date(1995, 1, 1) <= row[4] <= date(1996, 12, 31))
            line = list(data['lineitem'][0]); line[0:3] = [order[0], 3, 4]
            data['lineitem'].append(tuple(line))
            data['part'].append(next(row for row in data['part'] if row[0] == 3))
            data['customer'].append(next(row for row in data['customer'] if row[0] == order[1]))
            data['supplier'].append(next(row for row in data['supplier'] if row[0] == 4))
            requested = cases()
            for mode in ('duplicates-null', 'empty-lineitem', 'signed-decimals'):
                for table, rows in data.items():
                    values = [] if table == 'lineitem' and mode == 'empty-lineitem' else rows
                    if table == 'lineitem' and mode == 'signed-decimals':
                        values = []
                        for i, row in enumerate(rows):
                            row = list(row)
                            row[5] = Decimal('9999999999999.99') * (-1 if i % 3 == 0 else 1)
                            values.append(tuple(row))
                    spark.createDataFrame(values, SCHEMAS[table]).createOrReplaceTempView(table)
                for case in requested:
                    expected = value(spark, original(case.query))
                    actual = value(spark, case.sql)
                    assert expected == actual, (case.name, mode, expected, actual)
                    if mode == 'duplicates-null':
                        assert expected[0], ('Positive results required', case.name)
                    report['checks'].append({'query': case.query, 'case': case.name, 'mode': mode, 'passed': True})
            for table, rows in data.items():
                spark.createDataFrame(rows, SCHEMAS[table]).createOrReplaceTempView(table)
            for case in requested[:2]:
                mutant = (case.sql.replace("r_name = 'AMERICA'", "r_name = 'ASIA'") if case.query == 8 else
                          case.sql.replace('ps_partkey = p_partkey', 'ps_partkey = p_partkey AND false'))
                assert mutant != case.sql and value(spark, original(case.query))[0] != value(spark, mutant)[0]
                report['mutations'].append({'query': case.query, 'detected': True})
            # A colliding out-of-range key must demonstrate why the narrowing guard is mandatory.
            values = list(data['lineitem'])
            witness = next(row for row in values if row[0] == 200 and row[2] == 3)
            changed = list(witness); changed[2] = 5 + 2**32
            values[values.index(witness)] = tuple(changed)
            spark.createDataFrame(values, SCHEMAS['lineitem']).createOrReplaceTempView('lineitem')
            narrow = next(c for c in requested if c.name == 'critical_witness_int32')
            assert value(spark, original(21))[0] != value(spark, narrow.sql)[0]
            report['mutations'].append({'query': 21, 'mutation': 'Bypass int32 domain guard', 'detected': True})
            report['dataset_bounds'] = bounds(spark, args.dataset)
            report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', error=repr(error))
            raise
        finally:
            Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
            spark.stop()


if __name__ == '__main__':
    main()
