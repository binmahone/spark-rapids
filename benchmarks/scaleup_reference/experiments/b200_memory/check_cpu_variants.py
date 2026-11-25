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

"""Validate relational transformations on small adversarial inputs without allocating GPUs."""
import argparse
from pathlib import Path
import sys
from pyspark.sql import SparkSession
from suite_variants import check_semantics

SUITE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SUITE/'scripts'))
from common import write

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--output',required=True)
a=p.parse_args()
spark=SparkSession.builder.appName('b200-small-cpu-semantic-checks').getOrCreate()
try:
    checks=[]
    for arm,queries in [('preaggregate',[3,4,10,13,18]),('row-propagation',[5,9])]:
        checks.extend(dict(row,arm=arm) for row in check_semantics(
            spark,SUITE,queries,arm,require_gpu=False))
    write(a.output,{'status':'passed','spark_version':spark.version,'gpu_execution_checked':False,
                    'checks':checks})
finally:
    spark.stop()
