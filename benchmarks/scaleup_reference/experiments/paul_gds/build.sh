#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
set -euo pipefail
readonly root=$(git rev-parse --show-toplevel)
readonly repository="$root/.build/maven-repository"
readonly private="$root/.build/private"
readonly native=26.12.0-20261122.042214-52
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$JAVA_HOME/bin:$PATH"
export MAVEN_SKIP_RC=1 MAVEN_OPTS='-Xmx12g -XX:ReservedCodeCacheSize=1g'
unset JDK_JAVA_OPTIONS JAVA_TOOL_OPTIONS _JAVA_OPTIONS
mkdir -p "$root/.build/logs" "$root/.build/artifacts/arm64"
exec 9>"$root/.build/maven-build.lock"
flock -n 9
test -z "$(git status --porcelain)"
git submodule update --init thirdparty/spark-rapids-common
args=(-B -ntp -nsu "-Dmaven.repo.local=$repository" -Dbuildver=353
      -Dmaven.scaladoc.skip=true "-Dcudf-spark-jni.version=$native" -Dcuda.version=cuda13)
suites=org.apache.spark.sql.rapids.execution.ExecutorBroadcastCacheSuite
suites+=,org.apache.spark.sql.rapids.execution.GpuShuffleEnvReclaimSuite
suites+=,org.apache.spark.sql.rapids.execution.IndependentBroadcastRetirementSuite
suites+=,org.apache.spark.broadcast.ReferenceBroadcastSnapshotSuite
if ! mvn -o -N "${args[@]}" spotless:apply > "$root/.build/logs/spotless.log" 2>&1; then
  python3 - "$root/.build/logs/spotless.log" <<'PY'
from pathlib import Path
import sys
assert "No plugin found for prefix 'spotless'" in Path(sys.argv[1]).read_text()
PY
fi
mvn "${args[@]}" -Parm64 -pl sql-plugin-api -am install -DskipTests \
  2>&1 | tee "$root/.build/logs/api.log"
test "$(git -C "$private" rev-parse HEAD)" = 61e9e96803771371dec116593f0c4eced9ac44d8
test -z "$(git -C "$private" status --porcelain)"
cd "$private"
mvn "${args[@]}" -Dcudf.spark.jni.classifier=cuda13-arm64 \
  "-Dcudf.spark.jni.version=$native" -pl core -am install \
  -DwildcardSuites=com.nvidia.spark.rapids.optimizer \
  2>&1 | tee "$root/.build/logs/private.log"
cd "$root"
mvn "${args[@]}" -Parm64 -DallowConventionalDistJar=true -pl integration_tests -am install \
  "-DwildcardSuites=$suites" 2>&1 | tee "$root/.build/logs/public.log"
cp --reflink=never dist/target/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13-arm64.jar \
  .build/artifacts/arm64/
cp --reflink=never integration_tests/target/cudf-spark-integration-tests_2.12-26.12.0-SNAPSHOT-spark353.jar \
  .build/artifacts/arm64/
python3 benchmarks/scaleup_reference/experiments/paul_gds/manifest.py \
  --source "$root" --artifacts "$root/.build/artifacts/arm64"
