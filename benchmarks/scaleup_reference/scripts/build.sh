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
readonly root=$(cd "$(dirname "$0")/../../.." && pwd)
readonly suite="$root/benchmarks/scaleup_reference"
readonly arch=${1:?Usage: build.sh arm64|amd64}
case "$arch" in arm64|amd64) ;; *) exit 2 ;; esac
host_arch=$(uname -m)
case "$host_arch" in aarch64) java_arch=arm64 ;; x86_64) java_arch=amd64 ;; *) exit 2 ;; esac
export JAVA_HOME=${BUILD_JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-$java_arch}
export PATH="$JAVA_HOME/bin:$PATH"
readonly repository=${MAVEN_REPOSITORY:-$root/.build/maven-repository}
readonly private_source=${PRIVATE_SOURCE:-${root}-private}
mkdir -p "$root/.build/logs" "$root/.build/artifacts/$arch" "$repository"
exec 9>"$root/.build/maven-build.lock"
flock -n 9
export MAVEN_SKIP_RC=1
export MAVEN_OPTS=${MAVEN_OPTS:--Xmx12g -XX:ReservedCodeCacheSize=1g}
unset JDK_JAVA_OPTIONS
readonly native=26.12.0-20261122.042214-52
readonly private_revision=61e9e96803771371dec116593f0c4eced9ac44d8
if [[ ! -d "$private_source/.git" ]]; then
  git clone https://gitlab-master.nvidia.com/mahonem/cudf-spark-private.git "$private_source"
fi
test "$(git -C "$private_source" rev-parse HEAD)" = "$private_revision" || \
  git -C "$private_source" checkout --detach "$private_revision"
test -z "$(git -C "$private_source" status --porcelain)"
git -C "$root" submodule update --init thirdparty/spark-rapids-common
classifier=cuda13
profiles=()
if [[ "$arch" == arm64 ]]; then
  classifier=cuda13-arm64
  profiles=(-Parm64)
fi
args=(-B -ntp -nsu "-Dmaven.repo.local=$repository" -Dbuildver=353 \
  -Dmaven.scaladoc.skip=true "-Dcudf-spark-jni.version=$native")
cd "$root"
set +e
mvn -o -N "${args[@]}" spotless:apply > "$root/.build/logs/spotless-$arch.log" 2>&1
style=$?
set -e
if [[ "$style" != 0 ]]; then
  python3 - "$root/.build/logs/spotless-$arch.log" <<'PY'
from pathlib import Path
import sys
assert "No plugin found for prefix 'spotless'" in Path(sys.argv[1]).read_text()
PY
fi
mvn "${args[@]}" "${profiles[@]}" -Dcuda.version=cuda13 -pl sql-plugin-api -am install \
  -DskipTests 2>&1 | tee "$root/.build/logs/api-$arch.log"
cd "$private_source"
mvn "${args[@]}" "-Dcudf.spark.jni.classifier=$classifier" "-Dcudf.spark.jni.version=$native" -pl core -am install \
  -DwildcardSuites=com.nvidia.spark.rapids.optimizer \
  2>&1 | tee "$root/.build/logs/private-$arch.log"
cd "$root"
suites=org.apache.spark.sql.rapids.execution.ExecutorBroadcastCacheSuite
suites+=,org.apache.spark.sql.rapids.execution.GpuShuffleEnvReclaimSuite
suites+=,org.apache.spark.sql.rapids.execution.IndependentBroadcastRetirementSuite
suites+=,org.apache.spark.broadcast.ReferenceBroadcastSnapshotSuite
mvn "${args[@]}" "${profiles[@]}" -Dcuda.version=cuda13 -DallowConventionalDistJar=true \
  -pl dist -am install "-DwildcardSuites=$suites" \
  2>&1 | tee "$root/.build/logs/distribution-$arch.log"
mvn "${args[@]}" "${profiles[@]}" -Dcuda.version=cuda13 -DallowConventionalDistJar=true \
  -pl tests -am test-compile 2>&1 | tee "$root/.build/logs/test-compile-$arch.log"
mvn "${args[@]}" "${profiles[@]}" -Dcuda.version=cuda13 -pl tests \
  initialize scalatest:test@test "-DwildcardSuites=$suites" \
  2>&1 | tee "$root/.build/logs/cpu-tests-$arch.log"
cp --reflink=never "$root/dist/target/cudf-spark_2.12-26.12.0-SNAPSHOT-$classifier.jar" \
  "$root/.build/artifacts/$arch/"
python3 "$suite/scripts/build_tools.py" --arch "$arch"
python3 "$suite/scripts/artifact_manifest.py" --arch "$arch"
