#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${EXPECTED_SOURCE_COMMIT:-}" ]]; then
  echo "EXPECTED_SOURCE_COMMIT must name the committed instrumentation source" >&2
  exit 2
fi

readonly SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SEED_REPOSITORY="/home/vscode/.m2/repository"
readonly MAVEN_ROOT="${MAVEN_ROOT_OVERRIDE:-/home/vscode/.m2-ios-v17-cold-start-20260819}"
readonly MAVEN_REPOSITORY="${MAVEN_ROOT}/repository"
readonly EVIDENCE_ROOT="${EVIDENCE_ROOT_OVERRIDE:-/home/nvidia/mahone_gluten/industry-workloads/evidence/container-builds/ios-v17-cold-start-instrumentation-spark353-scala213-20260819}"
readonly LOG_ROOT="${EVIDENCE_ROOT}/logs"
readonly SETTINGS_PATH="${EVIDENCE_ROOT}/seed-only-settings.xml"
readonly JNI_VERSION="26.08.0-20260806.042042-62"
readonly JNI_SNAPSHOT="26.08.0-SNAPSHOT"
readonly JNI_JAR_SHA256="dcc488968055819250bb0f28c873016aa42324f2adda3d306de211c272c9a3a1"
readonly JNI_POM_SHA256="00e7fe9bd96809f5382adc72943c1b1a0d4089d7714817d3ef6a57defb648588"
readonly JNI_REVISION="14c6c1e757290ac78330276be9fe122c3ba09811"
readonly CUDF_REVISION="ff5b362d7c06ae5837fd7a7337e2ae20895f324d"
readonly PRIVATE_VERSION="26.08.0-20260809.031830-58"
readonly PRIVATE_SNAPSHOT="26.08.0-SNAPSHOT"
readonly PRIVATE_JAR_SHA256="f54abc27b59106524817940e91ede19ea95dbf5823e0e5472fb7d33e0e4a1fe1"
readonly PRIVATE_POM_SHA256="44363e882d165fce1fe9dee39411b64810e991f346e051d2409e77f23d01273c"
readonly PRIVATE_REVISION="d3dbdee582f0d84ebe5bb8fadd766682e924962a"
readonly EXPECTED_JAR="${SOURCE_ROOT}/scala2.13/dist/target/rapids-4-spark_2.13-26.08.0-SNAPSHOT-cuda12.jar"

[[ -d "${SEED_REPOSITORY}" ]]
[[ "$(git -C "${SOURCE_ROOT}" rev-parse HEAD)" == "${EXPECTED_SOURCE_COMMIT}" ]]
[[ -z "$(git -C "${SOURCE_ROOT}" status --short)" ]]
if [[ -e "${MAVEN_ROOT}" ]]; then
  echo "Isolated Maven root already exists; refusing reuse: ${MAVEN_ROOT}" >&2
  exit 2
fi

mkdir -p "${MAVEN_REPOSITORY}" "${LOG_ROOT}"

readonly SEED_JNI_ROOT="${SEED_REPOSITORY}/com/nvidia/spark-rapids-jni/${JNI_SNAPSHOT}"
readonly LOCAL_JNI_ROOT="${MAVEN_REPOSITORY}/com/nvidia/spark-rapids-jni/${JNI_SNAPSHOT}"
readonly SEED_JNI_JAR="${SEED_JNI_ROOT}/spark-rapids-jni-${JNI_VERSION}-cuda12.jar"
readonly SEED_JNI_POM="${SEED_JNI_ROOT}/spark-rapids-jni-${JNI_VERSION}.pom"

[[ "$(sha256sum "${SEED_JNI_JAR}" | awk '{print $1}')" == "${JNI_JAR_SHA256}" ]]
[[ "$(sha256sum "${SEED_JNI_POM}" | awk '{print $1}')" == "${JNI_POM_SHA256}" ]]
mkdir -p "${LOCAL_JNI_ROOT}"
cp "${SEED_JNI_JAR}" "${LOCAL_JNI_ROOT}/spark-rapids-jni-${JNI_VERSION}-cuda12.jar"
cp "${SEED_JNI_POM}" "${LOCAL_JNI_ROOT}/spark-rapids-jni-${JNI_VERSION}.pom"
cp "${SEED_JNI_JAR}" "${LOCAL_JNI_ROOT}/spark-rapids-jni-${JNI_SNAPSHOT}-cuda12.jar"
cp "${SEED_JNI_POM}" "${LOCAL_JNI_ROOT}/spark-rapids-jni-${JNI_SNAPSHOT}.pom"

readonly SEED_PRIVATE_ROOT="${SEED_REPOSITORY}/com/nvidia/rapids-4-spark-private_2.13/${PRIVATE_SNAPSHOT}"
readonly LOCAL_PRIVATE_ROOT="${MAVEN_REPOSITORY}/com/nvidia/rapids-4-spark-private_2.13/${PRIVATE_SNAPSHOT}"
readonly SEED_PRIVATE_JAR="${SEED_PRIVATE_ROOT}/rapids-4-spark-private_2.13-${PRIVATE_VERSION}-spark353.jar"
readonly SEED_PRIVATE_POM="${SEED_PRIVATE_ROOT}/rapids-4-spark-private_2.13-${PRIVATE_VERSION}.pom"

[[ "$(sha256sum "${SEED_PRIVATE_JAR}" | awk '{print $1}')" == "${PRIVATE_JAR_SHA256}" ]]
[[ "$(sha256sum "${SEED_PRIVATE_POM}" | awk '{print $1}')" == "${PRIVATE_POM_SHA256}" ]]
mkdir -p "${LOCAL_PRIVATE_ROOT}"
cp "${SEED_PRIVATE_JAR}" \
  "${LOCAL_PRIVATE_ROOT}/rapids-4-spark-private_2.13-${PRIVATE_SNAPSHOT}-spark353.jar"
cp "${SEED_PRIVATE_POM}" \
  "${LOCAL_PRIVATE_ROOT}/rapids-4-spark-private_2.13-${PRIVATE_SNAPSHOT}.pom"

cat > "${SETTINGS_PATH}" <<EOF
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">
  <mirrors>
    <mirror>
      <id>ios-v17-read-only-local-seed</id>
      <mirrorOf>*</mirrorOf>
      <url>file://${SEED_REPOSITORY}</url>
    </mirror>
  </mirrors>
</settings>
EOF

maven_common=(
  --batch-mode
  --no-snapshot-updates
  --settings "${SETTINGS_PATH}"
  -Dmaven.repo.local="${MAVEN_REPOSITORY}"
  -Dbuildver=353
  -Dcuda.version=cuda12
  -Djni.classifier=cuda12
  -Dspark-rapids-jni.version="${JNI_SNAPSHOT}"
)

{
  echo "source_commit=${EXPECTED_SOURCE_COMMIT}"
  echo "build_host_architecture=$(uname -m)"
  echo "buildver=353"
  echo "scala_version=2.13.18"
  echo "cuda_version=cuda12"
  echo "jni_version=${JNI_VERSION}"
  echo "jni_jar_sha256=${JNI_JAR_SHA256}"
  echo "jni_revision=${JNI_REVISION}"
  echo "cudf_revision=${CUDF_REVISION}"
  echo "private_version=${PRIVATE_VERSION}"
  echo "private_jar_sha256=${PRIVATE_JAR_SHA256}"
  echo "private_revision=${PRIVATE_REVISION}"
  java -version 2>&1
  mvn -version
} | tee "${LOG_ROOT}/input-identity.log"

cd "${SOURCE_ROOT}"
mvn "${maven_common[@]}" --non-recursive \
  -Dspark.rapids.source.basedir="${SOURCE_ROOT}" \
  antrun:run@scalastyle-all-modules \
  2>&1 | tee "${LOG_ROOT}/scalastyle.log"

mvn "${maven_common[@]}" -f scala2.13/pom.xml \
  -pl tests -am \
  -DwildcardSuites=com.nvidia.spark.rapids.ExecutorInitInstrumentationSuite,com.nvidia.spark.rapids.GcsReadWarmupSuite,com.nvidia.spark.rapids.ExecutorReaderDecodeWarmupSuite,org.apache.spark.sql.rapids.ColdStartQueryPlanningListenerSuite,org.apache.spark.sql.rapids.GpuWriteInstrumentationSuite \
  clean test 2>&1 | tee "${LOG_ROOT}/targeted-tests.log"
grep -Fq 'ExecutorInitInstrumentationSuite:' "${LOG_ROOT}/targeted-tests.log"
grep -Fq 'GcsReadWarmupSuite:' "${LOG_ROOT}/targeted-tests.log"
grep -Fq 'ExecutorReaderDecodeWarmupSuite:' "${LOG_ROOT}/targeted-tests.log"
grep -Fq 'ColdStartQueryPlanningListenerSuite:' "${LOG_ROOT}/targeted-tests.log"
grep -Fq 'GpuWriteInstrumentationSuite:' "${LOG_ROOT}/targeted-tests.log"
grep -Eq 'Tests: succeeded [1-9][0-9]*, failed 0' "${LOG_ROOT}/targeted-tests.log"

mvn "${maven_common[@]}" -f scala2.13/pom.xml \
  -pl dist -am \
  -Drapids.iceberg.artifactId=rapids-4-spark-iceberg-stub \
  -Drapids.iceberg.artifactId2=rapids-4-spark-iceberg-stub \
  -DskipTests clean package \
  2>&1 | tee "${LOG_ROOT}/package.log"

[[ -f "${EXPECTED_JAR}" ]]
jar tf "${EXPECTED_JAR}" | grep -Fq 'spark353/'
jar tf "${EXPECTED_JAR}" | grep -Fq 'amd64/Linux/libcudf.so'
jar tf "${EXPECTED_JAR}" | grep -Fq 'amd64/Linux/libcudfjni.so'
if jar tf "${EXPECTED_JAR}" | grep -Fq 'aarch64/Linux/'; then
  echo "Unexpected aarch64 native library found in the diagnostic JAR" >&2
  exit 1
fi
if jar tf "${EXPECTED_JAR}" | grep -Eq 'spark35[1245678]/'; then
  echo "Unexpected Spark shim found in the diagnostic JAR" >&2
  exit 1
fi
unzip -p "${EXPECTED_JAR}" rapids4spark-version-info.properties \
  | grep -Fx "revision=${EXPECTED_SOURCE_COMMIT}"
unzip -p "${EXPECTED_JAR}" spark-rapids-jni-version-info.properties \
  | grep -Fx "revision=${JNI_REVISION}"
unzip -p "${EXPECTED_JAR}" cudf-java-version-info.properties \
  | grep -Fx "revision=${CUDF_REVISION}"
unzip -p "${EXPECTED_JAR}" spark-shared/rapids4spark-private-version-info.properties \
  | grep -Fx "revision=${PRIVATE_REVISION}"
unzip -p "${EXPECTED_JAR}" \
  spark-shared/org/apache/spark/sql/rapids/GpuWriteJobStatsTracker\$.class \
  | strings > "${LOG_ROOT}/gpu-write-metric-keys.txt"
for metric_key in inputIteratorTime writerInitTime writerCloseTime tableWriterCloseTime \
    closeBufferedWriteTime outputStreamCloseTime statsCloseFileTime; do
  grep -Fxq "${metric_key}" "${LOG_ROOT}/gpu-write-metric-keys.txt"
done
unzip -p "${EXPECTED_JAR}" spark-shared/com/nvidia/spark/rapids/RapidsExecutorPlugin.class \
  | strings > "${LOG_ROOT}/executor-init-metric-keys.txt"
for phase in RAPIDS_EXECUTOR_INIT_METRIC cudf_version_check jni_constants_check; do
  grep -Fq "${phase}" "${LOG_ROOT}/executor-init-metric-keys.txt"
done
unzip -p "${EXPECTED_JAR}" \
  spark-shared/com/nvidia/spark/rapids/GcsReadWarmup\$.class \
  | strings > "${LOG_ROOT}/gcs-read-warmup-metric-keys.txt"
for metric_key in RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC get_file_system first_byte \
    read_remaining com.nvidia.v017.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem; do
  grep -Fq "${metric_key}" "${LOG_ROOT}/gcs-read-warmup-metric-keys.txt"
done
unzip -p "${EXPECTED_JAR}" \
  spark-shared/com/nvidia/spark/rapids/ExecutorReaderDecodeWarmup\$.class \
  | strings > "${LOG_ROOT}/reader-decode-warmup-metric-keys.txt"
unzip -p "${EXPECTED_JAR}" \
  spark-shared/com/nvidia/spark/rapids/MultiFileReaderThreadPool\$.class \
  | strings >> "${LOG_ROOT}/reader-decode-warmup-metric-keys.txt"
for metric_key in RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC pool_identity worker_thread \
    decoded_rows decoded_columns production_pool_access same_pool; do
  grep -Fq "${metric_key}" "${LOG_ROOT}/reader-decode-warmup-metric-keys.txt"
done
unzip -p "${EXPECTED_JAR}" spark-shared/com/nvidia/spark/rapids/GpuDeviceManager\$.class \
  | strings >> "${LOG_ROOT}/executor-init-metric-keys.txt"
for phase in gpu_device_init rmm_memory_init RAPIDS_MEMORY_INIT_METRIC \
    pinned_pool_and_offheap_limits_init rmm_gpu_pool_init spill_and_memory_events_init \
    gpu_shuffle_env_init; do
  grep -Fq "${phase}" "${LOG_ROOT}/executor-init-metric-keys.txt"
done
unzip -p "${EXPECTED_JAR}" \
  org/apache/spark/sql/rapids/ColdStartQueryPlanningListener.class \
  | strings > "${LOG_ROOT}/query-planning-metric-keys.txt"
for metric_key in RAPIDS_QUERY_PLANNING_METRIC RAPIDS_FILE_INDEX_METRIC \
    phase_ input_plan_identity_hash metadata_ops_time_ns root_paths input_files; do
  grep -Fq "${metric_key}" "${LOG_ROOT}/query-planning-metric-keys.txt"
done
if find "${MAVEN_REPOSITORY}" -type f -name '*.lastUpdated' -print -quit | grep -q .; then
  echo "Unresolved Maven dependency marker observed" >&2
  exit 1
fi

chmod 0644 "${EXPECTED_JAR}"
sha256sum "${EXPECTED_JAR}" | tee "${EVIDENCE_ROOT}/jar.sha256"
find "${MAVEN_REPOSITORY}" -type f -print0 \
  | sort -z \
  | xargs -0 sha256sum > "${EVIDENCE_ROOT}/isolated-maven-inputs.sha256"
