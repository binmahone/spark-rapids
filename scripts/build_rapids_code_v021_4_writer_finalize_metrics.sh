#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LOG_DIR=${RAPIDS_BUILD_LOG_DIR:-${TMPDIR:-/tmp}/rapids-code-v021-4-writer-finalize-metrics-build-logs}
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)

mkdir -p "$LOG_DIR"
cd "$ROOT"

mvn --offline -f scala2.13/pom.xml \
  -Prelease353 \
  -Dbuildver=353 \
  -Drapids.iceberg.artifactId=rapids-4-spark-iceberg-stub \
  -Drapids.iceberg.artifactId2=rapids-4-spark-iceberg-stub \
  -pl dist -am \
  -DskipTests \
  clean package \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-compile.log"

mvn --offline --non-recursive \
  -Dspark.rapids.source.basedir="$ROOT" \
  antrun:run@scalastyle-all-modules \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-scalastyle.log"
