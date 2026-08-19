#!/usr/bin/env bash
set -euo pipefail

readonly SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly WORK_ROOT="${WORK_ROOT_OVERRIDE:-/raid/mahonem/ios-v17-cold-start-instrumentation-20260819}"
readonly SEED_REPOSITORY="${SEED_REPOSITORY_OVERRIDE:-/home/nfs/mahonem/.m2/repository}"
readonly BUILD_IMAGE="${BUILD_IMAGE_OVERRIDE:-vsc-spark-gluten-233dddf04a9b42d03aa48197379a844b66b0aac2d316dcc634a0b0d0962586ce-uid:latest}"
readonly CONTAINER_HOME="${WORK_ROOT}/container-home"
readonly JNI_JAR="${SEED_REPOSITORY}/com/nvidia/spark-rapids-jni/26.08.0-SNAPSHOT/spark-rapids-jni-26.08.0-SNAPSHOT-cuda12.jar"
readonly PRIVATE_JAR="${SEED_REPOSITORY}/com/nvidia/rapids-4-spark-private_2.13/26.08.0-SNAPSHOT/rapids-4-spark-private_2.13-26.08.0-SNAPSHOT-spark353.jar"
readonly JNI_JAR_SHA256="dcc488968055819250bb0f28c873016aa42324f2adda3d306de211c272c9a3a1"
readonly PRIVATE_JAR_SHA256="f54abc27b59106524817940e91ede19ea95dbf5823e0e5472fb7d33e0e4a1fe1"

[[ "$(uname -m)" == "x86_64" ]]
[[ -z "$(git -C "${SOURCE_ROOT}" status --short)" ]]
[[ "$(sha256sum "${JNI_JAR}" | awk '{print $1}')" == "${JNI_JAR_SHA256}" ]]
[[ "$(sha256sum "${PRIVATE_JAR}" | awk '{print $1}')" == "${PRIVATE_JAR_SHA256}" ]]
mkdir -p "${CONTAINER_HOME}"

docker run --rm \
  --name ios-v17-cold-start-seed-prep \
  --user "$(id -u):$(id -g)" \
  --env HOME=/work/home \
  --env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  --env PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  --volume "${SOURCE_ROOT}:/work/source" \
  --volume "${SEED_REPOSITORY}:/work/m2/repository" \
  --volume "${CONTAINER_HOME}:/work/home" \
  --workdir /work/source \
  "${BUILD_IMAGE}" \
  mvn --batch-mode --no-snapshot-updates \
    --settings scripts/ios_v17_cold_start_seed_settings.xml \
    -Dmaven.repo.local=/work/m2/repository \
    -f scala2.13/pom.xml \
    -pl dist -am \
    -Dbuildver=353 \
    -Dcuda.version=cuda12 \
    -Djni.classifier=cuda12 \
    -Dspark-rapids-jni.version=26.08.0-SNAPSHOT \
    -Drapids.iceberg.artifactId=rapids-4-spark-iceberg-stub \
    -Drapids.iceberg.artifactId2=rapids-4-spark-iceberg-stub \
    dependency:go-offline

[[ "$(sha256sum "${JNI_JAR}" | awk '{print $1}')" == "${JNI_JAR_SHA256}" ]]
[[ "$(sha256sum "${PRIVATE_JAR}" | awk '{print $1}')" == "${PRIVATE_JAR_SHA256}" ]]
