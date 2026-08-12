#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LOG_DIR="$ROOT/v021-2-validation/build-logs"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)

mkdir -p "$LOG_DIR"
cd "$ROOT"

mvn --offline -f scala2.13/pom.xml \
  -Prelease353 \
  -Dbuildver=353 \
  -pl sql-plugin -am \
  -DskipTests \
  package \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-compile.log"

mvn --offline --non-recursive \
  -Dspark.rapids.source.basedir="$ROOT" \
  antrun:run@scalastyle-all-modules \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-scalastyle.log"
