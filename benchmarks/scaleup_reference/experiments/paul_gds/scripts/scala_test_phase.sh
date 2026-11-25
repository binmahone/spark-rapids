#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
set -euo pipefail
: "${PAUL_SCALA_RUN:?}" "${PAUL_SCALA_SUITE:?}" "${PAUL_SCALA_PHASE:?}"
bash "$SCALEUP_CONTAINER_SETUP"
export DEBIAN_FRONTEND=noninteractive
apt-get install -y --no-install-recommends openjdk-17-jdk-headless maven git
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$JAVA_HOME/bin:$PATH"
export MAVEN_SKIP_RC=1 MAVEN_OPTS='-Xmx12g -XX:ReservedCodeCacheSize=1g'
unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
exec python3 "$PAUL_SCALA_SUITE/scala_tests.py" node \
  --run "$PAUL_SCALA_RUN" --phase "$PAUL_SCALA_PHASE"
