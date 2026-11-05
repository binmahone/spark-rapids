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

import json
from pathlib import Path
import sys
import traceback

from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.getOrCreate()
root = Path(sys.argv[1])
path = str(root / 'wide-strings')
rows = 8_000_000
width = 300
spark.conf.set('spark.rapids.sql.enabled', 'false')
spark.conf.set('spark.sql.files.maxPartitionBytes', '16g')
spark.conf.set('spark.sql.files.openCostInBytes', '0')
spark.conf.set('spark.sql.files.minPartitionNum', '1')
spark.conf.set('spark.sql.files.maxPartitionNum', '1')
spark.range(rows, numPartitions=1).selectExpr('id', f"repeat('A', {width}) AS payload").write.option(
    'parquet.block.size', 64 * 1024 * 1024).option('compression', 'snappy').parquet(path)


def query():
    frame = spark.read.parquet(path)
    assert frame.rdd.getNumPartitions() == 1
    return frame.agg(
        F.sum(F.length('payload')).alias('payload_bytes'), F.sum('id').alias('sum_ids'))


expected = query().collect()
assert expected[0].payload_bytes == rows * width
assert expected[0].sum_ids == rows * (rows - 1) // 2
print(json.dumps({'cpu_reference': list(expected[0]), 'rows': rows,
                  'decoded_string_bytes': rows * width,
                  'parquet_files': len(list(Path(path).glob('*.parquet')))}), flush=True)
spark.conf.set('spark.rapids.sql.enabled', 'true')
spark.conf.set('spark.rapids.sql.format.parquet.hybridScan.enabled', 'true')
spark.conf.set('spark.rapids.sql.format.parquet.reader.type', 'MULTITHREADED')
spark.conf.set('spark.rapids.sql.test.format.parquet.hybridScan.failOnFallback', 'true')
spark.conf.set('spark.rapids.sql.reader.chunked', 'false')
try:
    actual = query().collect()
except Exception:
    details = traceback.format_exc()
    (root / 'hybrid-error.txt').write_text(details)
    print(details, flush=True)
    reproduced = 'String column' in details and (
        'overflow' in details or 'column size limit' in details)
    print(json.dumps({'hybrid_result': 'FAIL', 'string_overflow_reproduced': reproduced}), flush=True)
    spark.stop()
    raise SystemExit(31 if reproduced else 32)
assert actual == expected, (expected, actual)
print(json.dumps({'hybrid_result': 'PASS', 'string_overflow_reproduced': False}), flush=True)
print('PAUL_HYBRID_TEST_PASS', flush=True)
spark.stop()
