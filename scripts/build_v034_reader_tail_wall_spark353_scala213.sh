#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LOG_DIR="$ROOT/v034-validation/build-logs"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
: "${MAVEN_LOCAL_REPO:?Set MAVEN_LOCAL_REPO to the isolated Maven repository}"

COMMON_ARGS=(
  --offline
  -Dmaven.repo.local="$MAVEN_LOCAL_REPO"
  -f "$ROOT/scala2.13/pom.xml"
  -Prelease353
  -Dbuildver=353
  -Drapids.iceberg.artifactId=rapids-4-spark-iceberg-stub
  -Drapids.iceberg.artifactId2=rapids-4-spark-iceberg-stub
)

mkdir -p "$LOG_DIR"
cd "$ROOT"

mvn "${COMMON_ARGS[@]}" \
  -pl sql-plugin -am \
  -DwildcardSuites=org.apache.spark.sql.rapids.ReaderTaskAdmissionGateSuite \
  package \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-reader-admission-tests.log"

mvn "${COMMON_ARGS[@]}" \
  -pl dist -am \
  -DskipTests \
  clean package \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-dist.log"

mvn --offline --non-recursive \
  -Dmaven.repo.local="$MAVEN_LOCAL_REPO" \
  -Dspark.rapids.source.basedir="$ROOT" \
  antrun:run@scalastyle-all-modules \
  2>&1 | tee "$LOG_DIR/${TIMESTAMP}-scalastyle.log"
