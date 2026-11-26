#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
"""Prove integer domains before changing the internal representation of Q9 join keys."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession
from toy_oracle import SCHEMAS, fixture
from variants import original

KEYS = {'lineitem': ['l_partkey', 'l_suppkey'], 'part': ['p_partkey'],
        'partsupp': ['ps_partkey', 'ps_suppkey'], 'supplier': ['s_suppkey']}


def views(spark, target_type):
    for table, keys in KEYS.items():
        frame = spark.table(table)
        frame.select(*[frame[f.name].cast(target_type).alias(f.name) if f.name in keys
                       else frame[f.name] for f in frame.schema.fields]).createOrReplaceTempView(table)


def value(spark, sql):
    frame = spark.sql(sql)
    return Counter(tuple(row) for row in frame.collect()), frame.schema.jsonValue()


def bounds(spark, dataset):
    records = []
    for table, keys in KEYS.items():
        files = sorted((Path(dataset) / table).rglob('*.parquet'))
        assert files, table
        for path in files:
            before = path.stat()
            footer = spark._jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
                spark._jsc.hadoopConfiguration(), spark._jvm.org.apache.hadoop.fs.Path(str(path)))
            schema = footer.getFileMetaData().getSchema()
            for key in keys:
                assert str(schema.getType(key).asPrimitiveType().getPrimitiveTypeName()) == 'INT64'
            extrema = {key: [] for key in keys}
            groups = 0
            blocks = footer.getBlocks().iterator()
            while blocks.hasNext():
                block = blocks.next()
                columns = block.getColumns().iterator()
                found = set()
                while columns.hasNext():
                    column = columns.next()
                    key = str(column.getPath().toDotString())
                    if key not in keys:
                        continue
                    found.add(key)
                    statistics = column.getStatistics()
                    if statistics.hasNonNullValue():
                        low, high = int(statistics.genericGetMin()), int(statistics.genericGetMax())
                        assert -(2**31) <= low <= high < 2**31, (str(path), key, low, high)
                        extrema[key].extend([low, high])
                    else:
                        assert statistics.isNumNullsSet() and statistics.getNumNulls() == block.getRowCount()
                assert found == set(keys), (str(path), found)
                groups += 1
            after = path.stat()
            assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
            records.append({'path': str(path), 'table': table, 'bytes': before.st_size,
                'mtime_ns': before.st_mtime_ns, 'row_groups': groups,
                'bounds': {key: {'min': min(v) if v else None, 'max': max(v) if v else None}
                           for key, v in extrema.items()}})
    return {'status': 'passed', 'dataset': str(dataset), 'files': records,
            'contract': 'All six internal join-key columns fit signed int32 in every retained Parquet row group. Reject changed files; do not infer safety from TPC-H scale factor.'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dataset', required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    report = {'status': 'running', 'checks': []}
    with tempfile.TemporaryDirectory(prefix='narrow-keys-') as scratch:
        os.environ['SPARK_LOCAL_DIRS'] = scratch
        spark = (SparkSession.builder.master('local[2]').appName('narrow-key-domain-oracle')
            .config('spark.plugins', '').config('spark.sql.extensions', '')
            .config('spark.sql.shuffle.partitions', '2').config('spark.sql.adaptive.enabled', 'false')
            .config('spark.sql.ansi.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
            .config('spark.local.dir', scratch).getOrCreate())
        try:
            data = fixture()
            for mode in ('duplicates-null', 'empty-lineitem'):
                for table, rows in data.items():
                    actual = [] if table == 'lineitem' and mode == 'empty-lineitem' else rows
                    spark.createDataFrame(actual, SCHEMAS[table]).createOrReplaceTempView(table)
                expected = value(spark, original(9))
                views(spark, 'int')
                assert value(spark, original(9)) == expected, mode
                report['checks'].append({'case': mode, 'passed': True})
            # Skipping the domain guard must change actual join results for colliding keys.
            spark.createDataFrame([(7, 'a'), (7 + 2**32, 'b')], 'k long, v string').createOrReplaceTempView('guard_data')
            wide = value(spark, 'SELECT a.v,b.v FROM guard_data a JOIN guard_data b ON a.k=b.k')
            narrow = value(spark, 'SELECT a.v,b.v FROM guard_data a JOIN guard_data b ON CAST(a.k AS INT)=CAST(b.k AS INT)')
            assert wide != narrow
            report['checks'].append({'case': 'bypassed-domain-guard collision is detected', 'passed': True})
            report['dataset_bounds'] = bounds(spark, a.dataset)
            report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', error=repr(error))
            raise
        finally:
            Path(a.output).write_text(json.dumps(report, indent=2) + '\n')
            spark.stop()


if __name__ == '__main__':
    main()
