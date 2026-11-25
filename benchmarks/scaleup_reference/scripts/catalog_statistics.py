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

"""Load the selected read-only descriptive statistics and verify catalog readback."""
from common import TABLES, read, sha


def register_tables(spark, dataset, snapshot_path):
    snapshot = read(snapshot_path)
    receipt = {'snapshot_sha256': sha(snapshot_path), 'source': snapshot['source'], 'tables': {}}
    for table in TABLES:
        path = dataset.rstrip('/') + '/' + table
        spark.sql(f"CREATE TABLE `{table}` USING PARQUET LOCATION '{path}'")
        stats = snapshot['tables'][table]
        maps = {key: spark._jvm.java.util.HashMap() for key in
                ('distinct_count', 'minimum', 'maximum', 'null_count')}
        for column, values in stats['columns'].items():
            for key, value in values.items():
                if key in maps:
                    maps[key].put(column, str(value))
        spark._jvm.com.nvidia.spark.rapids.bench.CatalogStatsBridge.install(
            spark._jsparkSession, table, str(stats['row_count']),
            *[maps[key] for key in ('distinct_count', 'minimum', 'maximum', 'null_count')])
        plan = spark.table(table)._jdf.queryExecution().optimizedPlan()
        if not plan.stats().rowCount().isDefined():
            raise AssertionError('Catalog row count missing: ' + table)
        if int(str(plan.stats().rowCount().get())) != stats['row_count']:
            raise AssertionError('Catalog row count differs: ' + table)
        fields = {}
        for index in range(plan.output().size()):
            attribute = plan.output().apply(index)
            name = str(attribute.name())
            if name not in stats['columns']:
                continue
            actual = plan.stats().attributeStats().get(attribute)
            if not actual.isDefined():
                raise AssertionError('Catalog column statistics missing: ' + name)
            checked = {}
            for key, method in [('distinct_count', 'distinctCount'), ('null_count', 'nullCount')]:
                if key in stats['columns'][name]:
                    value = getattr(actual.get(), method)()
                    if not value.isDefined() or int(str(value.get())) != stats['columns'][name][key]:
                        raise AssertionError(f'Catalog {table}.{name}.{key} differs')
                    checked[key] = int(str(value.get()))
            fields[name] = checked
        receipt['tables'][table] = {'rows': stats['row_count'], 'path': path,
                                     'files': len(spark.table(table).inputFiles()),
                                     'column_statistics': fields,
                                     'schema': spark.table(table).schema.jsonValue()}
    return receipt
