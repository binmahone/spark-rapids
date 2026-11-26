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

"""Prove lossless integer narrowing from every row group's physical INT64 statistics."""
import json
from pathlib import Path

INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1


def inspect(spark, folder, columns):
    from py4j.protocol import Py4JJavaError
    try:
        result = spark._jvm.com.nvidia.spark.rapids.benchmark.ParquetDomainInspector.inspect(
            str(folder), list(columns))
        return json.loads(str(result))
    except Py4JJavaError as error:
        if error.java_exception.getClass().getName() == 'java.lang.IllegalArgumentException':
            raise ValueError(str(error.java_exception)) from error
        raise



def require_unchanged(work, dataset, required):
    proof = json.loads((Path(work)/'evidence/integer-domains.json').read_text())
    if proof['dataset'] != dataset or not proof['boundary_checks_passed']:
        raise ValueError('Integer proof does not match this dataset')
    for table, columns in required.items():
        entry = proof['tables'][table]
        actual = {str(p) for p in (Path(dataset)/table).glob('*.parquet')}
        if actual != {f['path'] for f in entry['files']}:
            raise ValueError('Dataset file inventory changed after integer proof')
        for f in entry['files']:
            stat = Path(f['path']).stat()
            if (stat.st_size, stat.st_mtime_ns) != (f['size'], f['mtime_ns']):
                raise ValueError('Dataset file changed after integer proof')
        for column in columns:
            domain = entry['columns'][column]
            if not domain['row_groups'] or (domain['minimum'] is not None and
                    not INT32_MIN <= domain['minimum'] <= domain['maximum'] <= INT32_MAX):
                raise ValueError('Invalid integer-domain proof')
    return proof


def main():
    import argparse
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    args = parser.parse_args()
    work = Path(args.work).resolve()
    target = work/'evidence/integer-domains.json'
    if target.exists():
        raise RuntimeError('Integer-domain evidence exists')
    spark = SparkSession.builder.appName('b200-integer-domain-audit').getOrCreate()
    try:
        tests = work/'scratch/integer-domain-boundaries'
        for name, rows, valid in [
                ('edges', [(INT32_MIN,), (INT32_MAX,), (None,)], True),
                ('overflow', [(INT32_MAX+1,)], False),
                ('underflow', [(INT32_MIN-1,)], False)]:
            path = tests/name
            spark.createDataFrame(rows, 'key long').coalesce(1).write.mode('overwrite').parquet(str(path))
            try:
                inspect(spark, path, ['key'])
            except ValueError:
                if valid:
                    raise
            else:
                if not valid:
                    raise AssertionError('Out-of-range Parquet input was accepted')
        dataset = '/raid/knataraj/datasets/tpch/sf10k_v2_float'
        tables = {}
        for table, columns in {'lineitem': ['l_suppkey','l_partkey'],
                               'partsupp': ['ps_suppkey','ps_partkey'],
                               'supplier': ['s_suppkey'], 'part': ['p_partkey']}.items():
            tables[table] = inspect(spark, Path(dataset)/table, columns)
            print(json.dumps({'table': table, 'bounds': tables[table]['columns'],
                              'files': len(tables[table]['files'])}), flush=True)
        target.write_text(json.dumps({'dataset': dataset, 'boundary_checks_passed': True,
                                      'tables': tables}, indent=2)+'\n')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
