#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
set -euo pipefail
readonly root=$(cd "$PAUL_SCALA_SUITE/../../../.." && pwd)
readonly repository="$root/.build/maven-repository"
readonly private="$root/../261124_paul_gds_private"
readonly native=26.12.0-20261122.042214-52
cd "$root"
exec 9>"$root/.build/maven-build.lock"
flock -n 9
test -z "$(git status --porcelain)"
git submodule update --init thirdparty/spark-rapids-common
test "$(git -C "$private" rev-parse HEAD)" = 61e9e96803771371dec116593f0c4eced9ac44d8
test -z "$(git -C "$private" status --porcelain)"
args=(-B -ntp -nsu "-Dmaven.repo.local=$repository" -Dbuildver=353
      -Dmaven.scaladoc.skip=true "-Dcudf-spark-jni.version=$native" -Dcuda.version=cuda13)
# Dependency installation compiles tests without executing GPU suites on the CPU allocation.
mvn "${args[@]}" -Parm64 -pl sql-plugin-api -am install -DskipTests \
  2>&1 | tee "$PAUL_SCALA_RUN/prepare-api.log"
cd "$private"
mvn "${args[@]}" -Dcudf.spark.jni.classifier=cuda13-arm64 \
  "-Dcudf.spark.jni.version=$native" -pl core -am install -DskipTests \
  2>&1 | tee "$PAUL_SCALA_RUN/prepare-private.log"
cd "$root"
mvn "${args[@]}" -Parm64 -pl tests -am install -DskipTests \
  2>&1 | tee "$PAUL_SCALA_RUN/prepare-tests.log"
