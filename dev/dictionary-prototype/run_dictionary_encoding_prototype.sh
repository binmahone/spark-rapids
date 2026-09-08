#!/usr/bin/env bash

set -euo pipefail

readonly WORK_ROOT=/home/prestouser/mahonem/260908_nvl72_ucx_compression
readonly JOB_ID=${SLURM_JOB_ID:?SLURM_JOB_ID is required}
readonly RESULT_DIR=${WORK_ROOT}/results/dictionary-prototype/job-${JOB_ID}
readonly SOURCE=${WORK_ROOT}/src/cudf-spark-main/dev/dictionary-prototype/DictionaryEncodingPrototype.java
readonly RAPIDS_JAR=${WORK_ROOT}/artifacts/cudf-spark-main-build/job-14453/rapids-4-spark_2.12-26.10.0-SNAPSHOT-cuda13-arm64.jar
readonly SPARK_HOME=${WORK_ROOT}/software/spark-3.5.3-bin-hadoop3
readonly ROWS=${DICTIONARY_PROTOTYPE_ROWS:-10000000}
readonly REPEATS=${DICTIONARY_PROTOTYPE_REPEATS:-3}

mkdir -p "${RESULT_DIR}/classes"
test -f "${SOURCE}"
test -f "${RAPIDS_JAR}"
test -d "${SPARK_HOME}/jars"

{
  echo "slurm_job_id=${JOB_ID}"
  echo "node=${SLURMD_NODENAME:-unknown}"
  echo "rows=${ROWS}"
  echo "repeats=${REPEATS}"
  echo "source_sha256=$(sha256sum "${SOURCE}" | awk '{print $1}')"
  echo "rapids_jar_sha256=$(sha256sum "${RAPIDS_JAR}" | awk '{print $1}')"
  nvidia-smi --query-gpu=name,uuid,memory.total --format=csv,noheader
} | tee "${RESULT_DIR}/provenance.txt"

javac -cp "${RAPIDS_JAR}:${SPARK_HOME}/jars/*" \
  -d "${RESULT_DIR}/classes" "${SOURCE}"

java -Xms1g -Xmx4g \
  -cp "${RESULT_DIR}/classes:${RAPIDS_JAR}:${SPARK_HOME}/jars/*" \
  DictionaryEncodingPrototype "${ROWS}" "${REPEATS}" \
  2>&1 | tee "${RESULT_DIR}/prototype.log"
