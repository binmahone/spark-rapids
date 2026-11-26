#!/usr/bin/env bash
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

set -euo pipefail
readonly work=${1:?Owned work directory required}
readonly here=$(cd "$(dirname "$0")" && pwd)
exec > >(tee "$work/evidence/integer-domain-audit.log") 2>&1
ulimit -c 0
export JAVA_HOME="$work/jdk17"
export CUDA_VISIBLE_DEVICES=''
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_LOCAL_IP=10.34.1.4
readonly spark_home=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["spark_home"])' "$work/evidence/deployment.json")
mkdir -p "$work/runtime/domain-inspector"
"$JAVA_HOME/bin/javac" -cp "$spark_home/jars/*" -d "$work/runtime/domain-inspector" \
    "$here/ParquetDomainInspector.java"
"$spark_home/bin/spark-submit" --master 'local[1]' --driver-memory 2g \
    --driver-class-path "$work/runtime/domain-inspector" \
    --conf spark.plugins= --conf spark.ui.enabled=false \
    --conf spark.driver.host=10.34.1.4 --conf spark.driver.bindAddress=10.34.1.4 \
    --conf "spark.local.dir=$work/scratch/domain-audit" \
    "$here/integer_domains.py" --work "$work"
