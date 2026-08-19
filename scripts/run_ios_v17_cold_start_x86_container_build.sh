#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${EXPECTED_SOURCE_COMMIT:-}" ]]; then
  echo "EXPECTED_SOURCE_COMMIT must name the committed instrumentation source" >&2
  exit 2
fi

readonly SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly WORK_ROOT="${WORK_ROOT_OVERRIDE:-/raid/mahonem/ios-v17-cold-start-instrumentation-20260819}"
readonly SEED_REPOSITORY="${SEED_REPOSITORY_OVERRIDE:-/home/nfs/mahonem/.m2/repository}"
readonly BUILD_IMAGE="${BUILD_IMAGE_OVERRIDE:-vsc-spark-gluten-233dddf04a9b42d03aa48197379a844b66b0aac2d316dcc634a0b0d0962586ce-uid:latest}"
readonly MAVEN_PARENT="${WORK_ROOT}/maven"
readonly EVIDENCE_PARENT="${WORK_ROOT}/evidence"
readonly CONTAINER_HOME="${WORK_ROOT}/container-home"
readonly MAVEN_ROOT="${MAVEN_PARENT}/isolated-${EXPECTED_SOURCE_COMMIT}"
readonly EVIDENCE_ROOT="${EVIDENCE_PARENT}/build-${EXPECTED_SOURCE_COMMIT}"

[[ "$(uname -m)" == "x86_64" ]]
[[ "$(git -C "${SOURCE_ROOT}" rev-parse HEAD)" == "${EXPECTED_SOURCE_COMMIT}" ]]
[[ -z "$(git -C "${SOURCE_ROOT}" status --short)" ]]
[[ -d "${SEED_REPOSITORY}" ]]
if [[ -e "${MAVEN_ROOT}" || -e "${EVIDENCE_ROOT}" ]]; then
  echo "Refusing to reuse an existing build or evidence directory" >&2
  exit 2
fi

mkdir -p "${MAVEN_PARENT}" "${EVIDENCE_PARENT}" "${CONTAINER_HOME}"

docker run --rm \
  --name "ios-v17-cold-start-build-${EXPECTED_SOURCE_COMMIT:0:12}" \
  --user "$(id -u):$(id -g)" \
  --env HOME=/work/home \
  --env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  --env EXPECTED_SOURCE_COMMIT="${EXPECTED_SOURCE_COMMIT}" \
  --env MAVEN_ROOT_OVERRIDE="/work/maven/isolated-${EXPECTED_SOURCE_COMMIT}" \
  --env EVIDENCE_ROOT_OVERRIDE="/work/evidence/build-${EXPECTED_SOURCE_COMMIT}" \
  --volume "${SOURCE_ROOT}:${SOURCE_ROOT}" \
  --volume "${SEED_REPOSITORY}:/home/vscode/.m2/repository:ro" \
  --volume "${MAVEN_PARENT}:/work/maven" \
  --volume "${EVIDENCE_PARENT}:/work/evidence" \
  --volume "${CONTAINER_HOME}:/work/home" \
  --workdir "${SOURCE_ROOT}" \
  "${BUILD_IMAGE}" \
  bash scripts/build_ios_v17_cold_start_spark353_scala213.sh
