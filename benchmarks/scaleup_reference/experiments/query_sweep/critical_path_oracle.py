# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Verify join multiplicities, nulls, and country-pair predicates before GPU screening."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession
from critical_path_variants import cases
from toy_oracle import SCHEMAS, fixture
from variants import original


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', required=True, type=Path)
    a = p.parse_args()
    assert not a.output.exists()
    report = {'status': 'running', 'checks': [], 'mutations': []}
    with tempfile.TemporaryDirectory(prefix='critical-path-oracle-') as scratch:
        os.environ['SPARK_LOCAL_DIRS'] = scratch
        spark = (SparkSession.builder.master('local[2]').appName('critical-path-oracle')
            .config('spark.plugins', '').config('spark.sql.extensions', '')
            .config('spark.sql.shuffle.partitions', '2').config('spark.sql.adaptive.enabled', 'false')
            .config('spark.sql.ansi.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
            .config('spark.local.dir', scratch).getOrCreate())
        def value(sql):
            f = spark.sql(sql)
            return Counter(tuple(row) for row in f.collect()), f.schema.jsonValue()
        try:
            data = fixture()
            for mode in ('duplicates-null', 'empty-lineitem', 'empty-customer'):
                for table, rows in data.items():
                    values = [] if mode == 'empty-' + table else rows
                    spark.createDataFrame(values, SCHEMAS[table]).createOrReplaceTempView(table)
                for c in cases():
                    expected, actual = value(original(c.query)), value(c.sql)
                    assert actual == expected, (mode, c.name, expected, actual)
                    if mode == 'duplicates-null':
                        assert expected[0], 'Fixture must exercise a positive result'
                        mutant = c.sql.replace('SELECT c_custkey, n_name AS cust_nation',
                                               'SELECT DISTINCT c_custkey, n_name AS cust_nation')
                        assert value(mutant)[0] != expected[0], 'Fixture must detect lost customer multiplicities'
                        report['mutations'].append({'mutation': 'Deduplicate customer-country rows', 'detected': True})
                    report['checks'].append({'case': c.name, 'mode': mode, 'passed': True})
            report['status'] = 'passed'
        except BaseException as e:
            report.update(status='failed', error=repr(e))
            raise
        finally:
            a.output.write_text(json.dumps(report, indent=2) + '\n')
            spark.stop()


if __name__ == '__main__':
    main()
