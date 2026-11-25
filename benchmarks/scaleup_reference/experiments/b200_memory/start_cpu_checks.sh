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
readonly work=${1:?Owned work directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly output="$work/evidence/cpu-variant-checks"
mkdir "$output"
mkdir -p "$work/scratch/cpu-variant-checks"
exec > >(tee "$output/driver.log") 2>&1
finish() { printf '%s\n' "$?" > "$output/exit"; }
trap finish EXIT
ulimit -c 0
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
export CUDA_VISIBLE_DEVICES=''
export SPARK_LOCAL_IP=10.34.1.4
export SPARK_LOCAL_HOSTNAME=10.34.1.4
spark_home=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spark_home"])' "$work/evidence/deployment.json")
"$spark_home/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
    --conf spark.plugins= --conf spark.ui.enabled=false \
    --conf spark.sql.shuffle.partitions=2 --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.catalogImplementation=in-memory \
    --conf "spark.local.dir=$work/scratch/cpu-variant-checks" \
    "$here/check_cpu_variants.py" --output "$output/result.json"
