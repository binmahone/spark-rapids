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
readonly own=/home/mahonem/paul_hybrid_multithreaded_261105
readonly legacy=/home/prestouser/mahonem/wild_spark_rapids
readonly source_root=$own/src/cudf-spark
readonly logs=$own/logs/job-${SLURM_JOB_ID:?}
readonly jar=$source_root/dist/target/rapids-4-spark_2.12-26.10.0-SNAPSHOT-cuda13-arm64.jar
mkdir -p "$logs" /var/lib/apt/lists/partial /var/cache/apt/archives/partial
readonly cache=$legacy/artifacts/apt-cache/ubuntu2404-arm64-cuda13.1
find "$cache/lists" -maxdepth 1 -type f ! -name lock -exec cp -t /var/lib/apt/lists/ -- {} +
find "$cache/archives" -maxdepth 1 -type f ! -name lock -exec cp -t /var/cache/apt/archives/ -- {} +
export DEBIAN_FRONTEND=noninteractive
apt-get install --no-download -y -qq --no-install-recommends \
  openjdk-17-jre-headless python3 libnuma1 libibverbs1 librdmacm1t64
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$JAVA_HOME/bin:$PATH"
printf '%s  %s\n' c04f75de16ddc862b814ee78bfc88c2ee6bc9f8f2a1343c143990a3da7e7f7ce "$jar" \
  | sha256sum --check
"$legacy/software/spark-3.5.3-bin-hadoop3/bin/spark-submit" --master 'local[4]' \
  --driver-memory 4g --jars "$jar" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.rapids.memory.gpu.allocFraction=0.1 \
  --conf spark.rapids.memory.gpu.maxAllocFraction=0.2 \
  --conf spark.rapids.memory.gpu.minAllocFraction=0.05 \
  "$source_root/experiments/paul-hybrid-261105/reader_counterexamples.py" \
  "$own/data/counterexamples-${SLURM_JOB_ID}" 2>&1 | tee "$logs/reader-counterexamples.log"
grep -q '^PAUL_HYBRID_COUNTEREXAMPLES_PASS$' "$logs/reader-counterexamples.log"
