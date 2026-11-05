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
readonly private_root=$own/src/cudf-spark-private
readonly repository=$own/maven-repository
readonly logs=$own/logs/job-${SLURM_JOB_ID:?}
readonly jar=$source_root/dist/target/rapids-4-spark_2.12-26.10.0-SNAPSHOT-cuda13-arm64.jar
mkdir -p "$logs" "$repository"
if [[ -s "$jar" ]]; then
  mkdir -p "$own/artifacts"
  previous_sha=$(sha256sum "$jar" | awk '{print $1}')
  cp --reflink=never --no-clobber "$jar" "$own/artifacts/$previous_sha.jar"
fi
df -hT / /home/mahonem /scratch | tee "$logs/capacity.txt"
test "$(df -PB1 /home/mahonem | awk 'NR==2 {print $4}')" -gt 53687091200
test "$(df -PB1 / | awk 'NR==2 {print $4}')" -gt 2147483648
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv > "$logs/gpus.csv"
readonly cache=$legacy/artifacts/apt-cache/ubuntu2404-arm64-cuda13.1
mkdir -p /var/lib/apt/lists/partial /var/cache/apt/archives/partial
find "$cache/lists" -maxdepth 1 -type f ! -name lock -exec cp -t /var/lib/apt/lists/ -- {} +
find "$cache/archives" -maxdepth 1 -type f ! -name lock -exec cp -t /var/cache/apt/archives/ -- {} +
export DEBIAN_FRONTEND=noninteractive
apt-get install --no-download -y -qq --no-install-recommends \
  openjdk-17-jre-headless python3 libnuma1 libibverbs1 librdmacm1t64
apt-get -o Acquire::Retries=1 -o Acquire::http::Timeout=20 \
  -o Acquire::https::Timeout=20 install -y -qq --no-install-recommends openjdk-17-jdk-headless git
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$legacy/tools/maven/bin:$JAVA_HOME/bin:$PATH"
export MAVEN_OPTS='-Xmx12g -XX:ReservedCodeCacheSize=512m'
test "$(git -C "$source_root" rev-parse HEAD)" = "${PUBLIC_REVISION:?}"
test "$(git -C "$private_root" rev-parse HEAD)" = "${PRIVATE_REVISION:?}"
test -z "$(git -C "$source_root" status --short)"
test -z "$(git -C "$private_root" status --short)"
build_args=(-B -ntp -Dmaven.repo.local="$repository" -Dmaven.wagon.rto=30000
  -Dbuildver=353 -Dmaven.scaladoc.skip=true -DskipTests)
cd "$source_root"
mvn "${build_args[@]}" -Parm64 -Dcuda.version=cuda13 -pl sql-plugin-api -am install \
  2>&1 | tee "$logs/api-build.log"
cd "$private_root"
mvn "${build_args[@]}" -pl core -am install 2>&1 | tee "$logs/private-build.log"
cd "$source_root"
mvn "${build_args[@]}" -nsu -Parm64 -Dcuda.version=cuda13 \
  -DallowConventionalDistJar=true -pl dist -am package 2>&1 | tee "$logs/package.log"
test -s "$jar"
sha256sum "$jar" > "$logs/jar.sha256"
python3 - "$source_root/experiments/paul-hybrid-261105/suite/policy.json" \
  "$own/runtime-policy.json" "$jar" "$PUBLIC_REVISION" "$PRIVATE_REVISION" <<'PY'
import hashlib
import json
import sys
from pathlib import Path
source, target, jar, public, private = sys.argv[1:]
policy = json.loads(Path(source).read_text())
with open(jar, "rb") as stream:
    digest = hashlib.file_digest(stream, "sha256").hexdigest()
policy["artifact_identity"] = {
    "build_job": int(__import__("os").environ["SLURM_JOB_ID"]),
    "public_revision": public,
    "private_revision": private,
    "jar_sha256": digest,
}
Path(target).write_text(json.dumps(policy, indent=2) + "\\n")
PY
readonly spark=$legacy/software/spark-3.5.3-bin-hadoop3
"$spark/bin/spark-submit" --master 'local[4]' --jars "$jar" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.rapids.memory.gpu.allocFraction=0.1 \
  --conf spark.rapids.memory.gpu.maxAllocFraction=0.2 \
  --conf spark.rapids.memory.gpu.minAllocFraction=0.05 \
  --conf spark.rapids.sql.format.parquet.hybridScan.enabled=true \
  --conf spark.rapids.sql.test.format.parquet.hybridScan.failOnFallback=true \
  --conf spark.rapids.sql.reader.chunked=false \
  --conf spark.sql.parquet.datetimeRebaseModeInRead=EXCEPTION \
  --conf spark.sql.parquet.int96RebaseModeInRead=EXCEPTION \
  --conf spark.rapids.sql.explain=ALL \
  "$source_root/experiments/paul-hybrid-261105/smoke_reader.py" \
  "$own/data/smoke-${SLURM_JOB_ID}" 2>&1 | tee "$logs/reader-smoke.log"
grep -q '^PAUL_HYBRID_READER_SMOKE_PASS$' "$logs/reader-smoke.log"
