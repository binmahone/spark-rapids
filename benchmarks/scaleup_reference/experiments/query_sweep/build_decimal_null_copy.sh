#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly root="$work/decimal-null-copy-engine"
readonly engine_revision=cac64336424d02a38f3421e577783bb8bd170749
mkdir -p "$work/decimal-null-copy-build" "$work/tools"
if [[ -e "$work/decimal-null-copy-build/build.exit" ]]; then
  previous=$(date -u +%Y%m%dT%H%M%SZ)
  mkdir "$work/decimal-null-copy-build/attempt-$previous"
  mv "$work/decimal-null-copy-build/build.log" "$work/decimal-null-copy-build/build.exit" \
    "$work/decimal-null-copy-build/attempt-$previous/"
fi
exec > >(tee "$work/decimal-null-copy-build/build.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/decimal-null-copy-build/build.exit"' EXIT
exec 8>"$work/decimal-null-copy-build/build.lock"
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
  git clone --branch experiment/aws-decimal-null-copy-261127 \
    https://github.com/binmahone/spark-rapids.git "$root"
fi
git -C "$root" fetch origin experiment/aws-decimal-null-copy-261127
git -C "$root" checkout --detach "$engine_revision"
git -C "$root" rev-parse HEAD > "$work/decimal-null-copy-build/source-revision.txt"
git -C "$root" diff --exit-code
bash "$root/benchmarks/scaleup_reference/scripts/build.sh" amd64
cd "$root"
mvn -B -ntp -nsu "-Dmaven.repo.local=$MAVEN_REPOSITORY" -Dbuildver=353 \
  -Dcuda.version=cuda13 -Dcudf-spark-jni.version=26.12.0-20261124.043801-53 \
  -pl tests initialize scalatest:test@test \
  -DwildcardSuites=org.apache.spark.sql.rapids.aggregate.ExtractChunkNullReplacementSuite \
  2>&1 | tee "$work/decimal-null-copy-build/chunk-tests.log"
