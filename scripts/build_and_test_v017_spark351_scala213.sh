#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LOG_DIR="$ROOT/v017-validation/build-logs"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)

mkdir -p "$LOG_DIR"

cd "$ROOT"

# The GPU/native suites require x86_64 with CUDA and are validated in the L4 preflight.
mvn -f scala2.13/pom.xml \
  -Prelease351 \
  -Dbuildver=351 \
  -pl sql-plugin -am \
  -DskipTests \
  package \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-compile.log"

mvn -f scala2.13/pom.xml \
  -Prelease351 \
  -Dbuildver=351 \
  -pl sql-plugin -am \
  -DwildcardSuites=com.nvidia.spark.rapids.AdaptiveShuffleCompressionSuite,com.nvidia.spark.rapids.AdaptiveShuffleCompressionEventSuite \
  package \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-adaptive-tests.log"

if [[ "${RUN_NATIVE_TESTS:-0}" == "1" ]]; then
  mvn -f scala2.13/pom.xml \
    -Prelease351 \
    -Dbuildver=351 \
    -pl tests -am \
    -DwildcardSuites=org.apache.spark.sql.rapids.RapidsShuffleThreadedWriterSuite \
    package \
    2>&1 | tee "$LOG_DIR/${TIMESTAMP}-threaded-writer-tests.log"
else
  echo "Skipping native threaded-writer tests; set RUN_NATIVE_TESTS=1 on a CUDA host."
fi
