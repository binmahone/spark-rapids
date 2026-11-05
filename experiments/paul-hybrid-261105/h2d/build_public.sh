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
readonly src=$own/src/cudf-spark-h2d
readonly logs=$own/logs/job-${SLURM_JOB_ID:?}
readonly repo=$own/maven-repository
readonly jar=$src/dist/target/rapids-4-spark_2.12-26.10.0-SNAPSHOT-cuda13-arm64.jar
mkdir -p "$logs"
df -hT / /home/mahonem > "$logs/capacity-before.txt"
test "$(df -PB1 /home/mahonem | awk 'NR==2 {print $4}')" -gt 53687091200
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
test "$(git -C "$src" rev-parse HEAD)" = "${PUBLIC_REVISION:?}"
test -z "$(git -C "$src" status --short)"
sha256sum --check "$own/logs/job-${NATIVE_JOB:?}/jni-jar.sha256" > "$logs/jni-integrity.txt"
cd "$src"
mvn -B -ntp -nsu -Dmaven.repo.local="$repo" -Dbuildver=353 \
  -Dmaven.scaladoc.skip=true -DskipTests -Parm64 -Dcuda.version=cuda13 \
  -DallowConventionalDistJar=true -pl dist -am install 2>&1 | tee "$logs/public-build.log"
test -s "$jar"
python3 - "$jar" "$JNI_REVISION" <<'PY'
from zipfile import ZipFile
import sys
with ZipFile(sys.argv[1]) as jar:
    assert 'revision=' + sys.argv[2] in jar.read('cudf-spark-jni-version-info.properties').decode()
    assert 'revision=d984f0a215980763781ae1b26be3816875c18cd9' in jar.read('cudf-java-version-info.properties').decode()
    assert 'com/nvidia/spark/rapids/jni/PackedHostToDeviceCopy.class' in jar.namelist()
PY
sha256sum "$jar" > "$logs/jar.sha256"
artifact_sha=$(sha256sum "$jar" | awk '{print $1}')
cp --reflink=never --no-clobber "$jar" "$own/artifacts/$artifact_sha.jar"
readonly spark=$legacy/software/spark-3.5.3-bin-hadoop3
for mode in false true; do
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
    --conf "spark.rapids.sql.test.format.parquet.hybridScan.packedH2D.enabled=$mode" \
    "$src/experiments/paul-hybrid-261105/reader_counterexamples.py" \
    "$own/data/h2d-${SLURM_JOB_ID}-$mode" 2>&1 | tee "$logs/reader-cases-$mode.log"
  if [[ $mode == true ]]; then
    grep -q 'PAUL_HYBRID_H2D_COPY mode=packed-async' "$logs/reader-cases-$mode.log"
  else
    grep -q 'PAUL_HYBRID_H2D_COPY mode=legacy-sync' "$logs/reader-cases-$mode.log"
  fi
  grep -q 'PAUL_HYBRID_COUNTEREXAMPLES_PASS' "$logs/reader-cases-$mode.log"
done
python3 - "$own/runtime-policy.json" "$own/h2d-runtime-policy.json" "$jar" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import sys
policy = json.loads(Path(sys.argv[1]).read_text())
policy['name'] = 'paul-hybrid-h2d-copy-ab'
policy['artifact_identity'] = {
    'build_job': int(os.environ['SLURM_JOB_ID']),
    'public_revision': os.environ['PUBLIC_REVISION'],
    'jni_revision': os.environ['JNI_REVISION'],
    'private_revision': 'a7fa053380e5c422dbf66a5b4a20a945751f9f4d',
    'jar_sha256': hashlib.file_digest(Path(sys.argv[3]).open('rb'), 'sha256').hexdigest(),
}
Path(sys.argv[2]).write_text(json.dumps(policy, indent=2) + '\n')
PY
echo PAUL_HYBRID_H2D_BUILD_AND_CASES_PASS
