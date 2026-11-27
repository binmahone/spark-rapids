#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly root="$work/join-stats-engine"
readonly engine_revision=1c0643063cab7a6febc80dc233c7678795a2012d
mkdir -p "$work/join-stats-build" "$work/tools"
if [[ -e "$work/join-stats-build/build.exit" ]]; then
  previous=$(date -u +%Y%m%dT%H%M%SZ)
  mkdir "$work/join-stats-build/attempt-$previous"
  mv "$work/join-stats-build/build.log" "$work/join-stats-build/build.exit" \
    "$work/join-stats-build/attempt-$previous/"
fi
exec > >(tee "$work/join-stats-build/build.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/join-stats-build/build.exit"' EXIT
exec 8>"$work/join-stats-build/build.lock"
flock -n 8
readonly maven=apache-maven-3.9.9
if [[ ! -x "$work/tools/$maven/bin/mvn" ]]; then
  curl --fail --location --retry 3 \
    "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.9/$maven-bin.tar.gz" \
    -o "$work/tools/$maven-bin.tar.gz"
  curl --fail --location --retry 3 \
    "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.9/$maven-bin.tar.gz.sha512" \
    -o "$work/tools/$maven-bin.tar.gz.sha512"
  python3 - "$work/tools/$maven-bin.tar.gz" <<'PY'
import hashlib
from pathlib import Path
import sys
p = Path(sys.argv[1])
assert hashlib.sha512(p.read_bytes()).hexdigest() == Path(str(p) + '.sha512').read_text().split()[0]
PY
  tar -xzf "$work/tools/$maven-bin.tar.gz" -C "$work/tools"
fi
export JAVA_HOME="$work/jdk17"
export BUILD_JAVA_HOME="$JAVA_HOME"
export PATH="$JAVA_HOME/bin:$work/tools/$maven/bin:$PATH"
export LD_LIBRARY_PATH="$work/ucx/lib:/usr/local/cuda-13.1/lib64:/usr/local/cuda-13.1/targets/x86_64-linux/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export CUDA_VISIBLE_DEVICES=0
export MAVEN_REPOSITORY="$work/maven-repository"
export PRIVATE_SOURCE="$work/source-private"
if [[ ! -d "$PRIVATE_SOURCE/.git" ]]; then
  git clone "$work/evidence/copy-stream-private.bundle" "$PRIVATE_SOURCE"
fi
git -C "$PRIVATE_SOURCE" checkout --detach fe7a2e4c261bff14dd95c331d6a4384a44cad842
test -z "$(git -C "$PRIVATE_SOURCE" status --porcelain)"
if [[ ! -d "$root/.git" ]]; then
  git clone --branch experiment/aws-join-stats-engine-261127 \
    https://github.com/binmahone/spark-rapids.git "$root"
fi
git -C "$root" checkout --detach "$engine_revision"
git -C "$root" rev-parse HEAD > "$work/join-stats-build/source-revision.txt"
git -C "$root" diff --exit-code
bash "$root/benchmarks/scaleup_reference/scripts/build.sh" amd64
cd "$root"
mvn -B -ntp -nsu "-Dmaven.repo.local=$MAVEN_REPOSITORY" -Dbuildver=353 \
  -Dcuda.version=cuda13 -Dcudf-spark-jni.version=26.12.0-20261124.043801-53 \
  -pl tests initialize scalatest:test@test \
  -DwildcardSuites=org.apache.spark.sql.rapids.execution.DriverlessJoinStatsCacheSuite,org.apache.spark.sql.rapids.execution.GpuDriverlessBroadcastHelperSuite,org.apache.spark.sql.rapids.execution.HashBuildSemaphoreSuite \
  2>&1 | tee "$work/join-stats-build/statistics-tests.log"
