#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
: "${MAVEN_LOCAL_REPO:?Set MAVEN_LOCAL_REPO to the physical session Maven repository}"
: "${VALIDATION_ROOT:?Set VALIDATION_ROOT to a new external evidence directory}"

test -d "${MAVEN_LOCAL_REPO}"
test ! -e "${VALIDATION_ROOT}"
mkdir -p "${VALIDATION_ROOT}"

COMMON_ARGS=(
  --offline
  -B
  -Dmaven.repo.local="${MAVEN_LOCAL_REPO}"
  -f "${ROOT}/scala2.13/pom.xml"
  -Prelease353
  -Dbuildver=353
  -Drapids.delta.artifactId1=rapids-4-spark-delta-stub
  -Drapids.iceberg.artifactId=rapids-4-spark-iceberg-stub
  -Drapids.iceberg.artifactId2=rapids-4-spark-iceberg-stub
)

mvn --offline --non-recursive -B \
  -Dmaven.repo.local="${MAVEN_LOCAL_REPO}" \
  -Dspark.rapids.source.basedir="${ROOT}" \
  antrun:run@scalastyle-all-modules \
  2>&1 | tee "${VALIDATION_ROOT}/scalastyle.log"

mvn "${COMMON_ARGS[@]}" \
  -pl sql-plugin -am \
  -DwildcardSuites=com.nvidia.spark.rapids.GcsReadWarmupSuite,com.nvidia.spark.rapids.ExecutorReaderDecodeWarmupSuite \
  package \
  2>&1 | tee "${VALIDATION_ROOT}/warmup-tests.log"
