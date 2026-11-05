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

from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.getOrCreate()
path = sys.argv[1]
spark.conf.set("spark.rapids.sql.enabled", "false")
data = spark.range(4096, numPartitions=16).selectExpr(
    "id", "CAST(id % 23 AS INT) AS k",
    "CAST(id * 0.25 AS DOUBLE) AS amount",
    "date_add(DATE '2000-01-01', CAST(id % 365 AS INT)) AS d")
data.write.mode("errorifexists").parquet(path)


def query():
    return spark.read.parquet(path).filter(
        (F.col("k") < 7) & (F.col("d") >= F.lit("2000-03-01").cast("date"))
    ).select("id", "k", "amount", "d")


expected = sorted(query().collect())
spark.conf.set("spark.rapids.sql.enabled", "true")
for mode in ["PERFILE", "MULTITHREADED"]:
    spark.conf.set("spark.rapids.sql.format.parquet.reader.type", mode)
    actual_query = query()
    actual = sorted(actual_query.collect())
    assert actual == expected, (mode, len(actual), len(expected))
    plan = actual_query._jdf.queryExecution().executedPlan().toString()
    assert "GpuScan" in plan or "GpuBatchScan" in plan, plan
    print(json.dumps({"reader": mode, "rows": len(actual), "result": "PASS"}), flush=True)
print("PAUL_HYBRID_READER_SMOKE_PASS", flush=True)
spark.stop()
