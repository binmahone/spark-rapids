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
import sys

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
root = sys.argv[1]
spark.conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'EXCEPTION')
spark.conf.set('spark.sql.parquet.int96RebaseModeInRead', 'EXCEPTION')
spark.conf.set('spark.rapids.sql.format.parquet.hybridScan.enabled', 'true')
spark.conf.set('spark.rapids.sql.test.format.parquet.hybridScan.failOnFallback', 'true')
spark.conf.set('spark.rapids.sql.reader.chunked', 'false')
spark.conf.set('spark.sql.files.maxPartitionBytes', '128m')
spark.conf.set('spark.sql.files.openCostInBytes', '4m')

cases = [
    ('nullable_or', 'nullable_int < 5 OR sorted_int > 19900'),
    ('nullable_and', 'nullable_int IS NULL AND filter_int = 7'),
    ('nullable_not', 'NOT (nullable_int = 500) AND sorted_int < 300'),
    ('residual_string_filter', "sorted_int < 2000 AND filter_str LIKE 'key_1%'"),
    ('all_pruned_statistics', 'sorted_int < 0'),
    ('all_pruned_dictionary', "filter_str = 'key_500'"),
    ('some_files_pruned', 'sorted_int < 100'),
    ('unfiltered', None),
]
completed = 0


def verify(name, factory):
    global completed
    spark.conf.set('spark.rapids.sql.enabled', 'false')
    expected = sorted(factory().collect())
    spark.conf.set('spark.rapids.sql.enabled', 'true')
    frame = factory()
    actual = sorted(frame.collect())
    assert actual == expected, (name, len(expected), len(actual))
    plan = frame._jdf.queryExecution().executedPlan().toString()
    assert 'GpuScan' in plan or 'GpuBatchScan' in plan, (name, plan)
    completed += 1
    print(json.dumps({'case': name, 'rows': len(actual), 'result': 'PASS'}), flush=True)


for codec in ('snappy', 'zstd'):
    path = root + '/' + codec
    partitioned = root + '/' + codec + '-partitioned'
    spark.conf.set('spark.rapids.sql.enabled', 'false')
    source = spark.range(20000, numPartitions=8).selectExpr(
        'CAST(id AS INT) AS sorted_int',
        'CAST(id % 100 AS INT) AS filter_int',
        "CONCAT('key_', CAST(id % 50 AS STRING)) AS filter_str",
        'CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id % 1000 AS INT) END AS nullable_int',
        'CAST(id AS DOUBLE) / 3 AS payload_double')
    source.write.option('parquet.block.size', 8192).option('compression', codec).parquet(path)
    source.withColumn('part', source.sorted_int % 4).write.partitionBy('part').option(
        'compression', codec).parquet(partitioned)
    for mode in ('PERFILE', 'MULTITHREADED'):
        spark.conf.set('spark.rapids.sql.format.parquet.reader.type', mode)
        for name, predicate in cases:
            def factory(predicate=predicate):
                frame = spark.read.parquet(path)
                return frame if predicate is None else frame.filter(predicate)
            verify(f'{codec}/{mode}/{name}', factory)

        for threshold in ('0.5', '1.0'):
            spark.conf.set('spark.rapids.sql.format.parquet.hybridScan.singleStage.filterKeepThreshold',
                           threshold)
            verify(f'{codec}/{mode}/projection-order/{threshold}',
                   lambda: spark.read.schema('filter_int INT, sorted_int INT').parquet(path)
                   .filter('sorted_int < 500'))
        spark.conf.set('spark.rapids.sql.format.parquet.hybridScan.singleStage.filterKeepThreshold',
                       '0.5')
        spark.conf.set('spark.sql.files.maxPartitionBytes', '4k')
        spark.conf.set('spark.sql.files.openCostInBytes', '0')
        def split_factory():
            frame = spark.read.parquet(path)
            assert frame.rdd.getNumPartitions() > len(frame.inputFiles())
            return frame
        verify(f'{codec}/{mode}/split-ownership', split_factory)
        spark.conf.set('spark.sql.files.maxPartitionBytes', '128m')
        spark.conf.set('spark.sql.files.openCostInBytes', '4m')
        verify(f'{codec}/{mode}/partition-values',
               lambda: spark.read.parquet(partitioned).filter('sorted_int < 500'))

        spark.conf.set('spark.rapids.sql.enabled', 'true')
        limited = spark.read.parquet(path).filter('sorted_int >= 0').limit(5).collect()
        assert len(limited) == 5
        assert len({row.sorted_int for row in limited}) == 5
        assert all(0 <= row.sorted_int < 20000 for row in limited)
        verify(f'{codec}/{mode}/read-after-early-limit', lambda: spark.read.parquet(path))

print(json.dumps({'result': 'PASS', 'complete_result_comparisons': completed}), flush=True)
print('PAUL_HYBRID_COUNTEREXAMPLES_PASS', flush=True)
spark.stop()
