#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
readonly root=/home/mahonem/paul_q6_kvikio_261110
readonly source=$root/src/cudf-spark
readonly scripts=$source/experiments/kvikio-q6-261110
readonly previous=/home/mahonem/paul_hybrid_multithreaded_261105
readonly legacy=/home/prestouser/mahonem/wild_spark_rapids
readonly repo=$previous/maven-repository
readonly output=$root/builds/job-${SLURM_JOB_ID:?}
readonly base=$previous/artifacts/ab3b63e7bf10d4e304dfdfd6c06b236b7898c6c82859bdc65c01184522aab432.jar
readonly python=/home/mahonem/kvikio_range_validation_261110/host-only-venv/bin/python
mkdir -p "$output" "$root/artifacts"
exec > >(tee "$output/full.log") 2>&1
df -hT / /home/mahonem
test "$(df -PB1 /home/mahonem | awk 'NR==2 {print $4}')" -gt 10737418240
test "$(df -PB1 / | awk 'NR==2 {print $4}')" -gt 4294967296
readonly apt_cache=$legacy/artifacts/apt-cache/ubuntu2404-arm64-cuda13.1
mkdir -p /var/lib/apt/lists/partial /var/cache/apt/archives/partial
mkdir -p "$root/apt-cache/partial"
find "$apt_cache/lists" -maxdepth 1 -type f ! -name lock -exec cp -t /var/lib/apt/lists/ -- {} +
find "$apt_cache/archives" -maxdepth 1 -type f ! -name lock -exec cp -t /var/cache/apt/archives/ -- {} +
find "$apt_cache/archives" -maxdepth 1 -type f ! -name lock -exec cp -n -t "$root/apt-cache" -- {} +
export DEBIAN_FRONTEND=noninteractive
apt-get -o Dir::Cache::archives="$root/apt-cache" \
  -o Acquire::Retries=1 -o Acquire::http::Timeout=20 -o Acquire::https::Timeout=20 \
  install -y -qq --no-install-recommends openjdk-17-jdk-headless g++ git python3 libnuma1
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$legacy/tools/maven/bin:$JAVA_HOME/bin:$PATH"
export MAVEN_OPTS='-Xmx6g -XX:ReservedCodeCacheSize=256m'
test "$(git -C "$source" rev-parse HEAD)" = "${KVIKIO_SOURCE_REVISION:?}"
test -z "$(git -C "$source" status --short)"
printf '%s\n' "$KVIKIO_SOURCE_REVISION" > "$output/source-revision.txt"
cd "$source"
mvn -B -ntp -nsu -Dmaven.repo.local="$repo" -Dbuildver=353 -Parm64 \
  -Dcuda.version=cuda13 -Dmaven.scaladoc.skip=true \
  -pl sql-plugin-fileio -am install 2>&1 | tee "$output/fileio-build.log"
site=$("$python" -c 'import site; print(site.getsitepackages()[0])')
cufile_include=$("$python" -c 'import pathlib,site; print(next((pathlib.Path(site.getsitepackages()[0])/"nvidia").rglob("cufile.h")).parent)')
test -s "$cufile_include/cufile.h"
g++ -std=c++20 -O2 -shared -fPIC -pthread \
  -DKVIKIO_LIBCURL_FOUND -DKVIKIO_CUFILE_FOUND -DKVIKIO_CUFILE_VERSION_API_FOUND \
  -DBS_THREAD_POOL_ENABLE_PAUSE=1 \
  -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" \
  -I"$site/libkvikio/include" -I"$site/rapids_logger/include" \
  -I"$cufile_include" -I/host-cuda-include \
  "$scripts/kvikio_host_jni.cpp" \
  -L"$site/libkvikio/lib64" -lkvikio -L"$site/rapids_logger/lib64" -lrapids_logger -ldl \
  -Wl,-rpath,"$site/libkvikio/lib64:$site/rapids_logger/lib64" \
  -o "$output/libkvikio_host_diagnostic.so"
g++ -std=c++20 -O2 -shared -fPIC -pthread "$scripts/io_trace.cpp" -ldl -o "$output/libio_trace.so"
ldd "$output/libkvikio_host_diagnostic.so" | tee "$output/native-dependencies.txt"
sha256sum "$output/libkvikio_host_diagnostic.so" "$site/libkvikio/lib64/libkvikio.so" \
  > "$output/native.sha256"
class_file=$(find "$source/sql-plugin-fileio/target" -path '*/classes/com/nvidia/spark/rapids/fileio/hadoop/HadoopInputFile.class')
test "$(printf '%s\n' "$class_file" | wc -l)" -eq 1
classes=${class_file%/com/nvidia/spark/rapids/fileio/hadoop/HadoopInputFile.class}
"$python" "$scripts/package_diagnostic.py" --base "$base" --classes "$classes" \
  --native "$output/libkvikio_host_diagnostic.so" --tracer "$output/libio_trace.so" --source-revision "$KVIKIO_SOURCE_REVISION" \
  --output "$output/rapids-kvikio-q6.jar" | tee "$output/identity.json"
readonly jar=$output/rapids-kvikio-q6.jar
readonly spark=$legacy/software/spark-3.5.3-bin-hadoop3
mkdir -p "$output/probe-classes"
javac -cp "$jar:$spark/jars/*" -d "$output/probe-classes" "$scripts/KvikioRangeReadProbe.java"
mkdir -p "$output/trace-probes"
for variant in filechannel:4:1:1:0:0 kvikio:4:1:1:0:0 kvikio:4:4:1:0:0 kvikio:4:4:4:0:0 kvikio:4:4:4:4:0 kvikio:4:4:4:0:1; do
  IFS=: read -r backend lanes window handles lane_tasks direct_tasks <<< "$variant"
  RAPIDS_TEST_KVIKIO_FILE_HANDLES=$handles RAPIDS_TEST_KVIKIO_LANE_TASKS=$lane_tasks \
    RAPIDS_TEST_KVIKIO_DIRECT_TASKS=$direct_tasks LD_PRELOAD="$output/libio_trace.so" \
    RAPIDS_IO_TRACE_HOST="$(hostname)" RAPIDS_IO_TRACE_GPU="${CUDA_VISIBLE_DEVICES:-0}" \
    RAPIDS_IO_TRACE_DIR="$output/trace-probes" RAPIDS_IO_TRACE_PREFIX=/tmp/kvikio-range-probe- \
    java -cp "$output/probe-classes:$jar:$spark/jars/*" \
    -Drapids.test.directHostRead=true -Drapids.test.rangeIoTiming=true \
    -Drapids.test.rangeReadConcurrency="$lanes" \
    -Drapids.test.kvikioHostRead="$([[ $backend == kvikio ]] && echo true || echo false)" \
    -Drapids.test.kvikioLibrary="$output/libkvikio_host_diagnostic.so" \
    -Drapids.test.kvikioThreads=32 -Drapids.test.kvikioTaskBytes=4194304 \
    -Drapids.test.kvikioRangeWindow="$window" \
    com.nvidia.spark.rapids.fileio.hadoop.KvikioRangeReadProbe \
    2>&1 | tee "$output/probe-$backend-$window-$handles-$lane_tasks-$direct_tasks.log"
  grep -q '^KVIKIO_RANGE_READ_PROBE_PASS' "$output/probe-$backend-$window-$handles-$lane_tasks-$direct_tasks.log"
done
python3 "$scripts/analyze_io_trace.py" "$output/trace-probes" --probe > "$output/trace-probe-analysis.log"
grep -q 'KVIKIO_NATIVE_TIMING handles=4 ranges=4 .*completed=true scheduling=strided-lanes' "$output/probe-kvikio-4-4-4-0.log"
grep -q 'KVIKIO_NATIVE_TIMING handles=4 ranges=4 .*completed=true scheduling=direct-per-range' "$output/probe-kvikio-4-4-0-1.log"
export RAPIDS_TEST_KVIKIO_FILE_HANDLES=4
for scheduling in direct-per-range strided-lanes; do
export RAPIDS_TEST_KVIKIO_LANE_TASKS=$([[ "$scheduling" == strided-lanes ]] && echo 4 || echo 0)
export RAPIDS_TEST_KVIKIO_DIRECT_TASKS=$([[ "$scheduling" == direct-per-range ]] && echo 1 || echo 0)
"$spark/bin/spark-submit" --master 'local[4]' --jars "$jar" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf "spark.driver.extraJavaOptions=-Drapids.test.kvikioHostRead=true -Drapids.test.kvikioRangeWindow=4 -Drapids.test.kvikioLibrary=$output/libkvikio_host_diagnostic.so" \
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
  --conf spark.rapids.sql.test.format.parquet.hybridScan.packedH2D.enabled=true \
  "$source/experiments/paul-hybrid-261105/reader_counterexamples.py" \
  "$root/data/probe-${SLURM_JOB_ID}-$scheduling" 2>&1 | tee "$output/hybrid-correctness-$scheduling.log"
grep -q '^PAUL_HYBRID_COUNTEREXAMPLES_PASS' "$output/hybrid-correctness-$scheduling.log"
grep -q 'KVIKIO_HOST_RANGE .*completed=true' "$output/hybrid-correctness-$scheduling.log"
grep -q "KVIKIO_NATIVE_TIMING .*completed=true scheduling=$scheduling" "$output/hybrid-correctness-$scheduling.log"
done
printf '%s\n' KVIKIO_Q6_BUILD_AND_CASES_PASS
