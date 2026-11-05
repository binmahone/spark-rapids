#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail

readonly WORK_ROOT=/home/mahonem/paul_hybrid_multithreaded_261105
readonly SPARK_HOME=/home/prestouser/mahonem/wild_spark_rapids/software/spark-3.5.3-bin-hadoop3
readonly RAPIDS_JAR=${TPCH_RAPIDS_JAR:-${WORK_ROOT}/artifacts/cudf-spark/26.08.1/rapids-4-spark_2.12-26.08.1-cuda13-arm64.jar}
readonly RESULT_DIGEST_JAR=${TPCH_RESULT_DIGEST_JAR:-}
readonly CATALOG_STATS_BRIDGE_JAR=${TPCH_CATALOG_STATS_BRIDGE_JAR:-}
readonly APPLICATION=${TPCH_APPLICATION:-${WORK_ROOT}/scripts/run_tpch_suite.py}
readonly SHUFFLE_MODE=${TPCH_SHUFFLE_MODE:-UCX}
readonly UCX_RUNTIME=${TPCH_RAPIDS_UCX_RUNTIME:-${WORK_ROOT}/artifacts/ucx-1.19.1-rc2-cuda13-aarch64/runtime/usr}
: "${HEAD_NODE:?HEAD_NODE must be set}"
: "${SLURM_JOB_ID:?SLURM_JOB_ID must be set}"
: "${SLURM_JOB_NUM_NODES:?SLURM_JOB_NUM_NODES must be set}"

readonly CURRENT_NODE=${SLURMD_NODENAME:-$(hostname)}
readonly MASTER_URL="spark://${HEAD_NODE}:7077"
readonly STATE_DIR=${WORK_ROOT}/state/job-${SLURM_JOB_ID}
readonly LOG_DIR=${WORK_ROOT}/logs/job-${SLURM_JOB_ID}
readonly RESULT_DIR=${WORK_ROOT}/results/cudf-spark-job-${SLURM_JOB_ID}
readonly LOCAL_JOB_ROOT=${TPCH_LOCAL_ROOT:-/tmp/prestouser/mahonem/wild-spark-rapids}/cudf-spark-${SLURM_JOB_ID}
readonly NODE_LOCAL=${LOCAL_JOB_ROOT}/${CURRENT_NODE}
readonly MIN_LOCAL_FREE_BYTES=${TPCH_MIN_LOCAL_FREE_BYTES:-322122547200}
readonly NATIVE_CRASH_DIAGNOSTICS=${TPCH_ENABLE_NATIVE_CRASH_DIAGNOSTICS:-false}

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${RESULT_DIR}/eventlog" "${NODE_LOCAL}"

{
  echo "node=${CURRENT_NODE}"
  echo "captured=$(date --iso-8601=seconds)"
  echo "enP22p3s0f1np1=$(cat /sys/class/net/enP22p3s0f1np1/operstate 2>/dev/null || echo missing)"
  echo "bond0_slaves=$(cat /sys/class/net/bond0/bonding/slaves 2>/dev/null || echo missing)"
} >"${LOG_DIR}/network-${CURRENT_NODE}.txt"

{
  echo "node=${CURRENT_NODE}"
  echo "captured=$(date --iso-8601=seconds)"
  echo "data_dir=${TPCH_DATA_DIR}"
  findmnt -T "${TPCH_DATA_DIR}" -o TARGET,SOURCE,FSTYPE,OPTIONS || true
  stat -f -c 'statfs_type=%T block_size=%S blocks=%b blocks_available=%a' "${TPCH_DATA_DIR}" || true
  grep -E '(^| )(nvidia_fs|gpfs|mmfs)( |$)' /proc/modules || true
  command -v gdscheck >/dev/null 2>&1 && gdscheck -p || true
} >"${LOG_DIR}/data-storage-${CURRENT_NODE}.txt" 2>&1

archive_executor_logs() {
  local stream path worker executor destination basename
  for stream in stdout stderr 'hs_err_pid*.log'; do
    while IFS= read -r path; do
      worker=$(sed -n 's#^.*/\(worker-[0-9][0-9]*\)/work/.*#\1#p' <<<"${path}")
      executor=$(basename "$(dirname "${path}")")
      basename=$(basename "${path}")
      case "${basename}" in
        stdout|stderr)
          destination="${LOG_DIR}/executor-${CURRENT_NODE}-${worker}-${executor}-${basename}.log"
          ;;
        *)
          destination="${LOG_DIR}/executor-${CURRENT_NODE}-${worker}-${executor}-${basename}"
          ;;
      esac
      cp "${path}" "${destination}" 2>/dev/null || true
    done < <(find "${NODE_LOCAL}" -path '*/work/*/*/'"${stream}" -type f 2>/dev/null)
  done
}

cleanup() {
  set +e
  archive_executor_logs
  mapfile -t pids < <(jobs -p)
  if ((${#pids[@]})); then
    kill "${pids[@]}" 2>/dev/null || true
    sleep 2
    kill -KILL "${pids[@]}" 2>/dev/null || true
    wait || true
  fi
  case "${NODE_LOCAL}" in
    /tmp/prestouser/mahonem/wild-spark-rapids/cudf-spark-"${SLURM_JOB_ID}"/*)
      rm -rf -- "${NODE_LOCAL}"
      ;;
    *)
      echo "Refusing to remove unexpected local path: ${NODE_LOCAL}" >&2
      ;;
  esac
}
trap cleanup EXIT INT TERM

monitor_local_storage() {
  local telemetry="${LOG_DIR}/storage-${CURRENT_NODE}.csv"
  local epoch used available
  echo "epoch,used_bytes,available_bytes,path" >"${telemetry}"
  while [[ ! -f "${STATE_DIR}/done" ]]; do
    epoch=$(date +%s)
    read -r used available < <(df -B1 --output=used,avail "${NODE_LOCAL}" | tail -n 1)
    echo "${epoch},${used},${available},${NODE_LOCAL}" >>"${telemetry}"
    if (( available < MIN_LOCAL_FREE_BYTES )); then
      echo "Disk guard tripped on ${CURRENT_NODE}: ${available} bytes available, floor=${MIN_LOCAL_FREE_BYTES}" \
        | tee "${STATE_DIR}/disk-guard-${CURRENT_NODE}.txt" >&2
      kill -TERM "$$"
      return
    fi
    sleep 5
  done
}
monitor_local_storage &

test -d "${TPCH_DATA_DIR}"
test -d "${TPCH_QUERY_DIR}"
test -x "${SPARK_HOME}/bin/spark-submit"
test -s "${RAPIDS_JAR}"
if [[ ${TPCH_RESULT_DIGEST_ENABLED:-false} == true ]]; then
  test -s "${RESULT_DIGEST_JAR}"
fi
if [[ -n ${TPCH_CATALOG_STATS_PATH:-} ]]; then
  test -s "${CATALOG_STATS_BRIDGE_JAR}"
fi
test -f "${APPLICATION}"

export DEBIAN_FRONTEND=noninteractive
sed -i 's|http://ports\.ubuntu\.com/ubuntu-ports|https://ports.ubuntu.com/ubuntu-ports|g' \
  /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
apt_ready=false
if [[ -n ${TPCH_OFFLINE_APT_CACHE:-} ]]; then
  test -s "${TPCH_OFFLINE_APT_CACHE}/SHA256SUMS"
  test -d "${TPCH_OFFLINE_APT_CACHE}/lists"
  test -d "${TPCH_OFFLINE_APT_CACHE}/archives"
  mkdir -p /var/lib/apt/lists/partial /var/cache/apt/archives/partial
  find "${TPCH_OFFLINE_APT_CACHE}/lists" -maxdepth 1 -type f ! -name lock \
    -exec cp -t /var/lib/apt/lists/ -- {} +
  find "${TPCH_OFFLINE_APT_CACHE}/archives" -maxdepth 1 -type f ! -name lock \
    -exec cp -t /var/cache/apt/archives/ -- {} +
  if apt-get install --no-download -y -qq --no-install-recommends \
      openjdk-17-jre-headless python3 curl ca-certificates procps findutils numactl libucx0; then
    apt_ready=true
  fi
else
  for attempt in 1 2 3; do
    if timeout 180 apt-get update -qq \
        -o Acquire::ForceIPv4=true \
        -o Acquire::Retries=3 \
        -o Acquire::http::Timeout=30 \
        -o Acquire::https::Timeout=30; then
      if timeout 180 apt-get \
          -o Acquire::ForceIPv4=true \
          -o Acquire::Retries=3 \
          -o Acquire::http::Timeout=30 \
          -o Acquire::https::Timeout=30 \
          install -y -qq --no-install-recommends \
          openjdk-17-jre-headless python3 curl ca-certificates procps findutils numactl libucx0; then
        apt_ready=true
        break
      fi
    fi
    sleep "$((attempt * 5))"
  done
fi
if [[ "${apt_ready}" != true ]]; then
  echo "APT initialization failed on ${CURRENT_NODE}" >&2
  exit 100
fi

export JAVA_HOME
JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:${PATH}"

if [[ ${TPCH_ENABLE_NODE_TELEMETRY:-false} == true ]]; then
  python3 "${WORK_ROOT}/scripts/monitor_q6_node_telemetry.py" \
    --output-dir "${LOG_DIR}" \
    --done-file "${STATE_DIR}/done" \
    >"${LOG_DIR}/node-telemetry-${CURRENT_NODE}.log" 2>&1 &
fi

if [[ ${TPCH_CAPTURE_EXECUTOR_COPY_LOGS:-false} == true ]]; then
  python3 "${WORK_ROOT}/scripts/capture_executor_copy_logs.py" \
    --worker-root "${NODE_LOCAL}" \
    --output "${LOG_DIR}/ucx-receive-copies-${CURRENT_NODE}.log" \
    --done-file "${STATE_DIR}/done" \
    >"${LOG_DIR}/ucx-receive-copy-capture-${CURRENT_NODE}.log" 2>&1 &
fi

declare -a UCX_SPARK_CONF=()
declare -a GDS_SPARK_CONF=()
declare -a PROFILER_SPARK_CONF=()
declare -a OPTIMIZER_METADATA_SPARK_CONF=()
if [[ -n ${TPCH_TRUSTED_METADATA_PATH:-} ]]; then
  test -s "${TPCH_TRUSTED_METADATA_PATH}"
  OPTIMIZER_METADATA_SPARK_CONF=(
    --conf "spark.rapids.sql.optimizer.trustedMetadata.path=${TPCH_TRUSTED_METADATA_PATH}"
  )
fi
CUFILE_BASE_CONFIG=
CUFILE_SECOND_NUMA_CONFIG=
if [[ ${TPCH_PARQUET_READER_TYPE:-MULTITHREADED} == GDS ]]; then
  readonly GDS_RUNTIME=/usr/local/cuda-13.0/targets/sbsa-linux
  test -s "${GDS_RUNTIME}/lib/libcufile.so.0"
  export LD_LIBRARY_PATH="${GDS_RUNTIME}/lib:/usr/local/cuda/lib64:${LD_LIBRARY_PATH:-}"
  export KVIKIO_COMPAT_MODE=${TPCH_KVIKIO_COMPAT_MODE:-OFF}
  export KVIKIO_TASK_SIZE=${TPCH_KVIKIO_TASK_SIZE:-16777216}
  export KVIKIO_NTHREADS=${TPCH_KVIKIO_NTHREADS:-16}
  GDS_SPARK_CONF=(
    --conf "spark.executorEnv.KVIKIO_COMPAT_MODE=${KVIKIO_COMPAT_MODE}"
    --conf "spark.executorEnv.KVIKIO_TASK_SIZE=${KVIKIO_TASK_SIZE}"
    --conf "spark.executorEnv.KVIKIO_NTHREADS=${KVIKIO_NTHREADS}"
    --conf "spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED"
    --conf "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"
  )
  readonly CUFILE_STATS=${TPCH_CUFILE_STATS:-0}
  if [[ ! "${CUFILE_STATS}" =~ ^[0-9]+$ ]]; then
    echo "TPCH_CUFILE_STATS must be a non-negative integer" >&2
    exit 2
  fi
  test -r /etc/cufile.json
  CUFILE_BASE_CONFIG="${NODE_LOCAL}/cufile-base.json"
  cp /etc/cufile.json "${CUFILE_BASE_CONFIG}"
  sed -E -i \
    's#("cufile_stats"[[:space:]]*:[[:space:]]*)[0-9]+#\1'"${CUFILE_STATS}"'#' \
    "${CUFILE_BASE_CONFIG}"
  grep -Eq '^[[:space:]]*"cufile_stats"[[:space:]]*:[[:space:]]*'"${CUFILE_STATS}" \
    "${CUFILE_BASE_CONFIG}"
  cp "${CUFILE_BASE_CONFIG}" \
    "${LOG_DIR}/cufile-base-${CURRENT_NODE}.json"
  if [[ -n "${TPCH_CUFILE_FORCE_COMPAT_MODE:-}" ]]; then
    export CUFILE_FORCE_COMPAT_MODE=${TPCH_CUFILE_FORCE_COMPAT_MODE}
    GDS_SPARK_CONF+=(
      --conf "spark.executorEnv.CUFILE_FORCE_COMPAT_MODE=${CUFILE_FORCE_COMPAT_MODE}"
    )
  fi

  case "${TPCH_CUFILE_SECOND_NUMA_ROUTE:-native}" in
    native)
      ;;
    first-rail-sysmem)
      test -r /etc/cufile.json
      CUFILE_SECOND_NUMA_CONFIG="${NODE_LOCAL}/cufile-second-numa-first-rail.json"
      cp "${CUFILE_BASE_CONFIG:-/etc/cufile.json}" "${CUFILE_SECOND_NUMA_CONFIG}"
      sed -E -i \
        -e 's#("rdma_dev_addr_list"[[:space:]]*:[[:space:]]*\[[[:space:]]*"[^"]+"[[:space:]]*,[[:space:]]*"[^"]+")[^]]*(\])#\1 \2#' \
        -e 's#("rdma_dynamic_routing"[[:space:]]*:[[:space:]]*)false#\1true#' \
        -e 's#("rdma_dynamic_routing_order"[[:space:]]*:[[:space:]]*)\[[^]]*\]#\1[ "SYS_MEM" ]#' \
        "${CUFILE_SECOND_NUMA_CONFIG}"
      grep -Eq '^[[:space:]]*"rdma_dynamic_routing"[[:space:]]*:[[:space:]]*true' \
        "${CUFILE_SECOND_NUMA_CONFIG}"
      grep -Eq '^[[:space:]]*"rdma_dynamic_routing_order"[[:space:]]*:[[:space:]]*\[[[:space:]]*"SYS_MEM"[[:space:]]*\]' \
        "${CUFILE_SECOND_NUMA_CONFIG}"
      cp "${CUFILE_SECOND_NUMA_CONFIG}" \
        "${LOG_DIR}/cufile-second-numa-first-rail-${CURRENT_NODE}.json"
      ;;
    *)
      echo "Invalid TPCH_CUFILE_SECOND_NUMA_ROUTE=${TPCH_CUFILE_SECOND_NUMA_ROUTE}; " \
        "expected native or first-rail-sysmem" >&2
      exit 2
      ;;
  esac
fi

if [[ ${TPCH_ENABLE_ASYNC_PROFILER:-false} == true ]]; then
  readonly ASYNC_PROFILER_DIR=${RESULT_DIR}/async-profiler
  mkdir -p "${ASYNC_PROFILER_DIR}"
  PROFILER_SPARK_CONF=(
    --conf spark.scheduler.mode=FIFO
    --conf "spark.rapids.flameGraph.pathPrefix=${ASYNC_PROFILER_DIR}"
    --conf "spark.rapids.flameGraph.executors=${TPCH_ASYNC_PROFILER_EXECUTORS:-*}"
    --conf "spark.rapids.flameGraph.asyncProfiler.options=${TPCH_ASYNC_PROFILER_OPTIONS:-jfr,event=cpu,wall=10ms}"
    --conf "spark.rapids.flameGraph.stageEpochInterval=${TPCH_ASYNC_PROFILER_STAGE_EPOCH_SECONDS:-1}"
  )
fi
if [[ "${SHUFFLE_MODE}" == "UCX" ]]; then
  test -x "${UCX_RUNTIME}/bin/ucx_info"
  test -s "${UCX_RUNTIME}/lib/libucp.so"
  test -s "${UCX_RUNTIME}/lib/ucx/libuct_cuda.so"
  export LD_LIBRARY_PATH="${UCX_RUNTIME}/lib:${LD_LIBRARY_PATH:-}"
  export UCX_MODULE_DIR="${UCX_RUNTIME}/lib/ucx"
  export UCX_TLS=${TPCH_RAPIDS_UCX_TLS:-^posix}
  export UCX_NET_DEVICES=${TPCH_RAPIDS_UCX_NET_DEVICES:-bond0}
  export UCX_ERROR_SIGNALS=
  if [[ -n "${TPCH_RAPIDS_UCX_CUDA_IPC_CACHE:-}" ]]; then
    export UCX_CUDA_IPC_CACHE=${TPCH_RAPIDS_UCX_CUDA_IPC_CACHE}
  fi
  if [[ -n "${TPCH_RAPIDS_UCX_CUDA_IPC_ENABLE_GET_ZCOPY:-}" ]]; then
    export UCX_CUDA_IPC_ENABLE_GET_ZCOPY=${TPCH_RAPIDS_UCX_CUDA_IPC_ENABLE_GET_ZCOPY}
  fi
  if [[ -n "${TPCH_RAPIDS_UCX_CUDA_IPC_ENABLE_MNNVL:-}" ]]; then
    export UCX_CUDA_IPC_ENABLE_MNNVL=${TPCH_RAPIDS_UCX_CUDA_IPC_ENABLE_MNNVL}
  fi
  if [[ -n "${TPCH_RAPIDS_UCX_RNDV_SCHEME:-}" ]]; then
    export UCX_RNDV_SCHEME=${TPCH_RAPIDS_UCX_RNDV_SCHEME}
  fi
  if [[ -n "${TPCH_RAPIDS_UCX_PROTO_INFO:-}" ]]; then
    export UCX_PROTO_INFO=${TPCH_RAPIDS_UCX_PROTO_INFO}
    export UCX_PROTO_INFO_DIR=${TPCH_RAPIDS_UCX_PROTO_INFO_DIR:-${LOG_DIR}/ucx-proto}
    mkdir -p "${UCX_PROTO_INFO_DIR}"
  fi
  if [[ "${NATIVE_CRASH_DIAGNOSTICS}" == true ]]; then
    export UCX_HANDLE_ERRORS=none
    export UCX_LOG_LEVEL=${TPCH_UCX_LOG_LEVEL:-warn}
    export UCX_LOG_FILE="${LOG_DIR}/ucx-%h-%p.log"
  fi
  ulimit -l unlimited 2>/dev/null || true
  "${UCX_RUNTIME}/bin/ucx_info" -v >"${LOG_DIR}/ucx-version-${CURRENT_NODE}.log" 2>&1
  "${UCX_RUNTIME}/bin/ucx_info" -d >"${LOG_DIR}/ucx-devices-${CURRENT_NODE}.log" 2>&1
  "${UCX_RUNTIME}/bin/ucx_info" -f >"${LOG_DIR}/ucx-config-${CURRENT_NODE}.log" 2>&1
  UCX_SPARK_CONF=(
    --conf "spark.executorEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
    --conf "spark.executorEnv.UCX_MODULE_DIR=${UCX_MODULE_DIR}"
    --conf "spark.executorEnv.UCX_TLS=${UCX_TLS}"
    --conf "spark.executorEnv.UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    --conf "spark.executorEnv.UCX_ERROR_SIGNALS=${UCX_ERROR_SIGNALS}"
    --conf "spark.rapids.shuffle.transport.earlyStart=${TPCH_RAPIDS_UCX_EARLY_START:-true}"
    --conf "spark.rapids.shuffle.transport.earlyStart.heartbeatInterval=${TPCH_RAPIDS_UCX_HEARTBEAT_INTERVAL_MS:-5000}"
    --conf "spark.rapids.shuffle.transport.earlyStart.heartbeatTimeout=${TPCH_RAPIDS_UCX_HEARTBEAT_TIMEOUT_MS:-10000}"
    --conf "spark.rapids.shuffle.ucx.useWakeup=${TPCH_RAPIDS_UCX_USE_WAKEUP:-true}"
    --conf "spark.rapids.shuffle.ucx.activeMessages.forceRndv=${TPCH_RAPIDS_UCX_FORCE_RNDV:-false}"
  )
  if [[ -n "${UCX_CUDA_IPC_CACHE:-}" ]]; then
    UCX_SPARK_CONF+=(
      --conf "spark.executorEnv.UCX_CUDA_IPC_CACHE=${UCX_CUDA_IPC_CACHE}"
    )
  fi
  if [[ -n "${UCX_CUDA_IPC_ENABLE_GET_ZCOPY:-}" ]]; then
    UCX_SPARK_CONF+=(
      --conf "spark.executorEnv.UCX_CUDA_IPC_ENABLE_GET_ZCOPY=${UCX_CUDA_IPC_ENABLE_GET_ZCOPY}"
    )
  fi
  if [[ -n "${UCX_CUDA_IPC_ENABLE_MNNVL:-}" ]]; then
    UCX_SPARK_CONF+=(
      --conf "spark.executorEnv.UCX_CUDA_IPC_ENABLE_MNNVL=${UCX_CUDA_IPC_ENABLE_MNNVL}"
    )
  fi
  if [[ -n "${UCX_RNDV_SCHEME:-}" ]]; then
    UCX_SPARK_CONF+=(
      --conf "spark.executorEnv.UCX_RNDV_SCHEME=${UCX_RNDV_SCHEME}"
    )
  fi
  if [[ -n "${UCX_PROTO_INFO:-}" ]]; then
    UCX_SPARK_CONF+=(
      --conf "spark.executorEnv.UCX_PROTO_INFO=${UCX_PROTO_INFO}"
      --conf "spark.executorEnv.UCX_PROTO_INFO_DIR=${UCX_PROTO_INFO_DIR}"
    )
  fi
  if [[ "${NATIVE_CRASH_DIAGNOSTICS}" == true ]]; then
    UCX_SPARK_CONF+=(
      --conf "spark.executorEnv.UCX_HANDLE_ERRORS=${UCX_HANDLE_ERRORS}"
      --conf "spark.executorEnv.UCX_LOG_LEVEL=${UCX_LOG_LEVEL}"
      --conf "spark.executorEnv.UCX_LOG_FILE=${UCX_LOG_FILE}"
    )
  fi
fi

{
  echo "node=${CURRENT_NODE}"
  echo "captured=$(date --iso-8601=seconds)"
  echo "native_crash_diagnostics=${NATIVE_CRASH_DIAGNOSTICS}"
  uname -a
  nvidia-smi --query-gpu=index,uuid,pci.bus_id,driver_version,memory.total \
    --format=csv,noheader || true
  for library in \
      /usr/lib/aarch64-linux-gnu/libcuda.so.1 \
      /usr/local/cuda-13.0/targets/sbsa-linux/lib/libcufile.so.0 \
      "${UCX_RUNTIME}/lib/libucp.so" \
      "${UCX_RUNTIME}/lib/libuct.so" \
      "${UCX_RUNTIME}/lib/ucx/libuct_cuda.so"; do
    if [[ -e "${library}" ]]; then
      printf 'library=%s resolved=%s\n' "${library}" "$(readlink -f "${library}")"
      sha256sum "${library}" || true
    else
      printf 'library=%s missing\n' "${library}"
    fi
  done
} >"${LOG_DIR}/native-runtime-${CURRENT_NODE}.txt" 2>&1

declare -a WILD_SPARK_CONF=()
readonly WILD_ALL=${TPCH_WILD_ENABLED:-false}
readonly WILD_EAGER_CLEANUP=${TPCH_WILD_EAGER_CLEANUP_ENABLED:-${WILD_ALL}}
readonly WILD_RANGE_SORT=${TPCH_WILD_RANGE_SORT_ENABLED:-${WILD_ALL}}
readonly WILD_LEFT_SEMI=${TPCH_WILD_LEFT_SEMI_ENABLED:-${WILD_ALL}}
readonly WILD_SHUFFLE_BROADCAST=${TPCH_WILD_SHUFFLE_BROADCAST_ENABLED:-${WILD_ALL}}
readonly WILD_SHUFFLE_BROADCAST_TRUST_SPARK_PLAN=${TPCH_WILD_SHUFFLE_BROADCAST_TRUST_SPARK_PLAN_ENABLED:-false}
WILD_SPARK_CONF=(
  --conf "spark.rapids.shuffle.gpuCatalog.eagerCleanup.enabled=${WILD_EAGER_CLEANUP}"
  --conf "spark.rapids.sql.optimizer.rangeSortSinglePartition.enabled=${WILD_RANGE_SORT}"
  --conf "spark.rapids.sql.optimizer.rewriteLargeLeftSemi.enabled=${WILD_LEFT_SEMI}"
  --conf "spark.rapids.sql.optimizer.rewriteLargeLeftSemi.thresholdMultiplier=${TPCH_WILD_LEFT_SEMI_THRESHOLD_MULTIPLIER:-2}"
  --conf "spark.rapids.shuffle.broadcast.enabled=${WILD_SHUFFLE_BROADCAST}"
  --conf "spark.rapids.shuffle.broadcast.maxSize=${TPCH_WILD_BROADCAST_MAX_SIZE:-8g}"
  --conf "spark.rapids.shuffle.broadcast.trustSparkPlan.enabled=${WILD_SHUFFLE_BROADCAST_TRUST_SPARK_PLAN}"
)

declare -a FILE_PARTITION_CONF=()
if [[ -n "${TPCH_FILES_MAX_PARTITION_NUM:-}" ]]; then
  FILE_PARTITION_CONF=(
    --conf "spark.sql.files.maxPartitionNum=${TPCH_FILES_MAX_PARTITION_NUM}"
  )
fi

if [[ "${CURRENT_NODE}" == "${HEAD_NODE}" ]]; then
  SPARK_LOG_DIR="${LOG_DIR}" SPARK_PID_DIR="${NODE_LOCAL}/pids" \
    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${HEAD_NODE}" --port 7077 --webui-port 18080 \
      >"${LOG_DIR}/master.log" 2>&1 &
  for _ in $(seq 1 60); do
    curl -fsS "http://${HEAD_NODE}:18080/json/" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "http://${HEAD_NODE}:18080/json/" >/dev/null
  touch "${STATE_DIR}/master-ready"
else
  until [[ -f "${STATE_DIR}/master-ready" ]]; do sleep 2; done
fi

readonly EXECUTORS_PER_NODE=${TPCH_EXECUTORS_PER_NODE:-4}
if (( EXECUTORS_PER_NODE < 1 || EXECUTORS_PER_NODE > 4 )); then
  echo "TPCH_EXECUTORS_PER_NODE must be between 1 and 4" >&2
  exit 2
fi
for gpu in $(seq 0 $((EXECUTORS_PER_NODE - 1))); do
  worker_dir="${NODE_LOCAL}/worker-${gpu}"
  discovery="${NODE_LOCAL}/gpu-${gpu}.sh"
  worker_environment=(env)
  if [[ ${TPCH_ENABLE_ASYNC_PROFILER:-false} == true ]]; then
    worker_environment+=("HOME=${worker_dir}/home")
  fi
  if [[ -n "${TPCH_CUFILE_LOGGING_LEVEL:-}" ]]; then
    worker_environment+=(
      "CUFILE_LOGGING_LEVEL=${TPCH_CUFILE_LOGGING_LEVEL}"
      "CUFILE_LOGFILE_PATH=${LOG_DIR}/cufile-${CURRENT_NODE}-gpu${gpu}.log"
    )
  fi
  numa_node=$((gpu / 2))
  if [[ -n "${CUFILE_SECOND_NUMA_CONFIG}" && "${numa_node}" -gt 0 ]]; then
    worker_environment+=("CUFILE_ENV_PATH_JSON=${CUFILE_SECOND_NUMA_CONFIG}")
  elif [[ -n "${CUFILE_BASE_CONFIG}" ]]; then
    worker_environment+=("CUFILE_ENV_PATH_JSON=${CUFILE_BASE_CONFIG}")
  fi
  mkdir -p "${worker_dir}"/{home,tmp,local,work,logs,pids}
  printf '#!/usr/bin/env bash\nprintf '\''{"name":"gpu","addresses":["0"]}\\n'\''\n' >"${discovery}"
  chmod +x "${discovery}"
  "${worker_environment[@]}" \
    CUDA_VISIBLE_DEVICES="${gpu}" \
    TMPDIR="${worker_dir}/tmp" \
    SPARK_LOCAL_DIRS="${worker_dir}/local" \
    SPARK_WORKER_DIR="${worker_dir}/work" \
    SPARK_LOG_DIR="${worker_dir}/logs" \
    SPARK_PID_DIR="${worker_dir}/pids" \
    SPARK_WORKER_OPTS="-Dspark.worker.resource.gpu.amount=1 -Dspark.worker.resource.gpu.discoveryScript=${discovery}" \
    numactl --cpunodebind="${numa_node}" --membind="${numa_node}" \
    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --host "${CURRENT_NODE}" --port "$((10000 + gpu))" --webui-port "$((18082 + gpu))" \
      --cores 30 --memory 350g "${MASTER_URL}" \
      >"${LOG_DIR}/worker-${CURRENT_NODE}-${gpu}.log" 2>&1 &
done
touch "${STATE_DIR}/workers-${CURRENT_NODE}-started"

if [[ "${CURRENT_NODE}" != "${HEAD_NODE}" ]]; then
  until [[ -f "${STATE_DIR}/done" ]]; do sleep 5; done
  exit "$(cat "${STATE_DIR}/driver.rc")"
fi

readonly WORKER_START_TIMEOUT_SECONDS=${TPCH_WORKER_START_TIMEOUT_SECONDS:-1200}
if [[ ! ${WORKER_START_TIMEOUT_SECONDS} =~ ^[1-9][0-9]*$ ]]; then
  echo "TPCH_WORKER_START_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
worker_start_deadline=$((SECONDS + WORKER_START_TIMEOUT_SECONDS))
while (( SECONDS < worker_start_deadline )); do
  started=$(find "${STATE_DIR}" -name 'workers-*-started' | wc -l)
  [[ "${started}" -ge "${SLURM_JOB_NUM_NODES}" ]] && break
  sleep 2
done
started=$(find "${STATE_DIR}" -name 'workers-*-started' | wc -l)
if [[ "${started}" -lt "${SLURM_JOB_NUM_NODES}" ]]; then
  echo "Timed out waiting for Spark workers: ${started}/${SLURM_JOB_NUM_NODES} nodes started after ${WORKER_START_TIMEOUT_SECONDS}s" >&2
  exit 1
fi

JVM_OPTS="-XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log -Dio.netty.maxDirectMemory=68719476736"
if [[ "${NATIVE_CRASH_DIAGNOSTICS}" == true ]]; then
  JVM_OPTS+=' -XX:+CreateCoredumpOnCrash -XX:+UnlockDiagnosticVMOptions -XX:+PreserveFramePointer'
fi
if [[ ${TPCH_ENABLE_ASYNC_PROFILER:-false} == true ]]; then
  JVM_OPTS+=' -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer'
fi
readonly JVM_OPTS
export TPCH_ENGINE=cudf-spark
export TPCH_ENGINE_VERSION=${TPCH_RAPIDS_VERSION:-26.08.1}
export TPCH_ENGINE_ARTIFACT=${RAPIDS_JAR}
export TPCH_EXPERIMENT_PROFILE=${TPCH_EXPERIMENT_PROFILE:-baseline}
export TPCH_ENGINE_ARTIFACT_SHA256
TPCH_ENGINE_ARTIFACT_SHA256=$(sha256sum "${RAPIDS_JAR}" | awk '{print $1}')
export TPCH_OUTPUT_DIR=${RESULT_DIR}
export TPCH_STRICT_GPU_ONLY=true

declare -a EVIDENCE_ARTIFACTS=(
  "${TPCH_SUBMIT_SCRIPT:-${WORK_ROOT}/scripts/submit_cudf_spark_ucx_qualification.slurm}"
  "${WORK_ROOT}/scripts/run_cudf_spark_cluster.sh"
  "${WORK_ROOT}/scripts/monitor_q6_node_telemetry.py"
  "${APPLICATION}"
  "${RAPIDS_JAR}"
)
if [[ -n "${RESULT_DIGEST_JAR}" ]]; then
  EVIDENCE_ARTIFACTS+=("${RESULT_DIGEST_JAR}")
fi
if [[ -n "${CATALOG_STATS_BRIDGE_JAR}" ]]; then
  EVIDENCE_ARTIFACTS+=("${CATALOG_STATS_BRIDGE_JAR}")
fi
if [[ -n "${TPCH_QUERY_PROFILE_PATH:-}" ]]; then
  test -s "${TPCH_QUERY_PROFILE_PATH}"
  EVIDENCE_ARTIFACTS+=("${TPCH_QUERY_PROFILE_PATH}")
  cp "${TPCH_QUERY_PROFILE_PATH}" "${RESULT_DIR}/query-profile.json"
fi
if [[ -n "${TPCH_POLICY_MANIFEST_PATH:-}" ]]; then
  test -s "${TPCH_POLICY_MANIFEST_PATH}"
  EVIDENCE_ARTIFACTS+=("${TPCH_POLICY_MANIFEST_PATH}")
  cp "${TPCH_POLICY_MANIFEST_PATH}" "${RESULT_DIR}/policy-manifest.json"
fi
if [[ -n "${TPCH_TRUSTED_METADATA_PATH:-}" ]]; then
  test -s "${TPCH_TRUSTED_METADATA_PATH}"
  EVIDENCE_ARTIFACTS+=("${TPCH_TRUSTED_METADATA_PATH}")
  cp "${TPCH_TRUSTED_METADATA_PATH}" "${RESULT_DIR}/trusted-optimizer-metadata.properties"
fi
if [[ -n "${TPCH_CATALOG_STATS_PATH:-}" ]]; then
  test -s "${TPCH_CATALOG_STATS_PATH}"
  EVIDENCE_ARTIFACTS+=("${TPCH_CATALOG_STATS_PATH}")
  cp "${TPCH_CATALOG_STATS_PATH}" "${RESULT_DIR}/catalog-statistics.properties"
fi
sha256sum "${EVIDENCE_ARTIFACTS[@]}" >"${RESULT_DIR}/artifact-sha256.txt"
printf '%s\n' "${SLURM_JOB_NODELIST}" >"${RESULT_DIR}/slurm-nodelist.txt"
printf '%s\n' "${TPCH_CONTAINER_IMAGE:-nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04}" \
  >"${RESULT_DIR}/container-image.txt"

DRIVER_EXTRA_CLASSPATH=${RAPIDS_JAR}
if [[ -n "${RESULT_DIGEST_JAR}" ]]; then
  DRIVER_EXTRA_CLASSPATH+=:${RESULT_DIGEST_JAR}
fi
if [[ -n "${CATALOG_STATS_BRIDGE_JAR}" ]]; then
  DRIVER_EXTRA_CLASSPATH+=:${CATALOG_STATS_BRIDGE_JAR}
fi
readonly DRIVER_EXTRA_CLASSPATH

declare -a POLICY_SPARK_CONF=()
mapfile -d '' -t policy_pairs < <(python3 - "${TPCH_POLICY_MANIFEST_PATH:?}" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    values = json.load(source)["uniform_configuration"]
for key, value in sorted(values.items()):
    if key.startswith("spark."):
        sys.stdout.buffer.write((key + "=" + str(value) + "\0").encode())
PY
)
for pair in "${policy_pairs[@]}"; do
  POLICY_SPARK_CONF+=(--conf "$pair")
done

set +e
"${SPARK_HOME}/bin/spark-submit" \
  --master "${MASTER_URL}" \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.test.enabled=true \
  --conf spark.rapids.sql.test.allowedNonGpu= \
  --conf spark.rapids.sql.allowMultipleJars=NEVER \
  --conf spark.driver.extraClassPath="${DRIVER_EXTRA_CLASSPATH}" \
  --conf spark.executor.extraClassPath="${RAPIDS_JAR}" \
  --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
  --conf spark.rapids.shuffle.mode="${SHUFFLE_MODE}" \
  "${UCX_SPARK_CONF[@]}" \
  "${GDS_SPARK_CONF[@]}" \
  "${PROFILER_SPARK_CONF[@]}" \
  "${WILD_SPARK_CONF[@]}" \
  "${OPTIMIZER_METADATA_SPARK_CONF[@]}" \
  --conf "spark.rapids.shuffle.transport.maxReceiveInflightBytes=${TPCH_RAPIDS_MAX_RECEIVE_INFLIGHT_BYTES:-1g}" \
  --conf "spark.rapids.shuffle.compression.codec=${TPCH_SHUFFLE_COMPRESSION_CODEC:-none}" \
  --conf "spark.rapids.shuffle.compression.maxBatchMemory=${TPCH_SHUFFLE_COMPRESSION_MAX_BATCH_MEMORY:-1g}" \
  --conf "spark.rapids.shuffle.ucx.bounceBuffers.size=${TPCH_RAPIDS_UCX_BOUNCE_BUFFER_SIZE:-4m}" \
  --conf "spark.rapids.shuffle.ucx.bounceBuffers.device.count=${TPCH_RAPIDS_UCX_DEVICE_BUFFER_COUNT:-32}" \
  --conf spark.rapids.shuffle.multiThreaded.reader.threads=32 \
  --conf spark.rapids.shuffle.multiThreaded.writer.threads=32 \
  --conf spark.rapids.shuffle.multiThreaded.maxBytesInFlight=512m \
  --conf spark.rapids.shuffle.multithreaded.skipMerge=true \
  --conf spark.shuffle.service.enabled=false \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.instances="${TPCH_EXPECTED_EXECUTORS}" \
  --conf "spark.executor.cores=${TPCH_EXECUTOR_CORES:-16}" \
  --conf "spark.executor.memory=${TPCH_EXECUTOR_MEMORY:-32g}" \
  --conf "spark.executor.memoryOverhead=${TPCH_EXECUTOR_MEMORY_OVERHEAD:-288g}" \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf "spark.task.resource.gpu.amount=${TPCH_TASK_GPU_AMOUNT:-0.125}" \
  --conf spark.task.cpus=1 \
  --conf "spark.rapids.sql.concurrentGpuTasks=${TPCH_CONCURRENT_GPU_TASKS:-2}" \
  --conf "spark.rapids.sql.concurrentGpuTasks.dynamic=${TPCH_DYNAMIC_CONCURRENT_GPU_TASKS:-false}" \
  --conf "spark.rapids.sql.maxConcurrentGpuTasks=${TPCH_MAX_CONCURRENT_GPU_TASKS:-0}" \
  --conf "spark.rapids.sql.join.hash.numSubPartitions=${TPCH_HASH_JOIN_NUM_SUB_PARTITIONS:-16}" \
  --conf "spark.rapids.sql.join.useShuffledSymmetricHashJoin=${TPCH_USE_SHUFFLED_SYMMETRIC_HASH_JOIN:-true}" \
  --conf "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.enabled=${TPCH_PUSH_DIMENSION_CHAIN_BEFORE_FACT_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.maxChainRows=${TPCH_PUSH_DIMENSION_CHAIN_MAX_ROWS:-10000000}" \
  --conf "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.maxChainBytes=${TPCH_PUSH_DIMENSION_CHAIN_MAX_BYTES:-1g}" \
  --conf "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.independentSelectiveLeaf.enabled=${TPCH_PUSH_DIMENSION_CHAIN_INDEPENDENT_LEAF_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.selectiveFilteredDimensionBroadcast.enabled=${TPCH_SELECTIVE_FILTERED_DIMENSION_BROADCAST_ENABLED:-${TPCH_PUSH_DIMENSION_CHAIN_BEFORE_FACT_ENABLED:-false}}" \
  --conf "spark.rapids.sql.optimizer.pushSelectiveDimensionFilterIntoAggregate.enabled=${TPCH_PUSH_SELECTIVE_DIMENSION_FILTER_INTO_AGGREGATE_ENABLED:-${TPCH_PUSH_DIMENSION_CHAIN_BEFORE_FACT_ENABLED:-false}}" \
  --conf "spark.rapids.sql.optimizer.selectiveFilteredDimensionBroadcast.costGate.enabled=${TPCH_SELECTIVE_FILTERED_DIMENSION_COST_GATE_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.selectiveFilteredDimensionBroadcast.minNetworkSavingsRatio=${TPCH_SELECTIVE_FILTERED_DIMENSION_MIN_NETWORK_SAVINGS_RATIO:-1.25}" \
  --conf "spark.rapids.sql.optimizer.pushSelectiveKeysetToJoinInputs.enabled=${TPCH_PUSH_SELECTIVE_KEYSET_TO_JOIN_INPUTS_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.pushAggregateMeasureBeforeJoin.enabled=${TPCH_PUSH_AGGREGATE_MEASURE_BEFORE_JOIN_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.reorderSelectiveFactChain.enabled=${TPCH_REORDER_SELECTIVE_FACT_CHAIN_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.preferSmallerShuffleHashBuild.enabled=${TPCH_PREFER_SMALLER_SHUFFLE_HASH_BUILD_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.preferSmallerShuffleHashBuild.minSizeRatio=${TPCH_PREFER_SMALLER_SHUFFLE_HASH_BUILD_MIN_RATIO:-1.25}" \
  --conf "spark.rapids.sql.optimizer.trustedMetadata.constraints.enabled=${TPCH_TRUSTED_METADATA_CONSTRAINTS_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.deduplicateLargeLeftAntiBuildSide.enabled=${TPCH_DEDUPLICATE_LARGE_LEFT_ANTI_BUILD_SIDE_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery=${TPCH_OPTIMIZE_CORRELATED_AGGREGATE_SUBQUERY_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery.pushUniqueFilterIntoGroupedState.enabled=${TPCH_PUSH_UNIQUE_FILTER_INTO_GROUPED_STATE_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery.reuseSelfExtremum.enabled=${TPCH_REUSE_SELF_EXTREMUM_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery.reuseGroupedInState.enabled=${TPCH_REUSE_GROUPED_IN_STATE_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery.countedSelfExistence.enabled=${TPCH_COUNTED_SELF_EXISTENCE_ENABLED:-true}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery.summarizeExistenceRhs.enabled=${TPCH_SUMMARIZE_EXISTENCE_RHS_ENABLED:-false}" \
  --conf "spark.rapids.sql.optimizer.optimizeCorrelatedAggregateSubquery.generalRewrites.enabled=${TPCH_GENERAL_CORRELATED_REWRITES_ENABLED:-true}" \
  --conf "spark.rapids.memory.gpu.pool=${TPCH_RMM_POOL:-ASYNC}" \
  --conf "spark.rapids.memory.gpu.allocFraction=${TPCH_GPU_ALLOC_FRACTION:-0.90}" \
  --conf spark.rapids.memory.gpu.minAllocFraction=0.50 \
  --conf spark.rapids.memory.gpu.maxAllocFraction=0.95 \
  --conf spark.rapids.memory.gpu.reserve=2g \
  --conf "spark.rapids.memory.pinnedPool.size=${TPCH_PINNED_POOL_SIZE:-16g}" \
  --conf spark.rapids.memory.pinnedPool.setCuioDefault=true \
  --conf spark.rapids.memory.host.offHeapLimit.enabled=true \
  --conf spark.rapids.memory.host.offHeapLimit.size=256g \
  --conf spark.rapids.memory.host.spillStorageSize=256g \
  --conf "spark.rapids.sql.batchSizeBytes=${TPCH_BATCH_SIZE_BYTES:-1073741824}" \
  --conf "spark.rapids.sql.reader.batchSizeBytes=${TPCH_READER_BATCH_SIZE_BYTES:-1073741824}" \
  --conf "spark.rapids.sql.format.parquet.reader.type=${TPCH_PARQUET_READER_TYPE:-MULTITHREADED}" \
  --conf "spark.rapids.sql.format.parquet.reader.footer.type=${TPCH_PARQUET_FOOTER_TYPE:-NATIVE}" \
  --conf "spark.rapids.sql.format.parquet.gds.filterPushdown.enabled=${TPCH_GDS_FILTER_PUSHDOWN_ENABLED:-false}" \
  --conf "spark.sql.parquet.filterPushdown=${TPCH_PARQUET_FILTER_PUSHDOWN:-true}" \
  --conf "spark.hadoop.parquet.filter.dictionary.enabled=${TPCH_PARQUET_DICTIONARY_FILTER_ENABLED:-true}" \
  --conf "spark.rapids.sql.multiThreadedRead.numThreads=${TPCH_MULTITHREADED_READ_THREADS:-16}" \
  --conf spark.rapids.sql.explain=NOT_ON_GPU \
  --conf spark.rapids.sql.metrics.level=MODERATE \
  --conf "spark.sql.adaptive.enabled=${TPCH_ADAPTIVE_ENABLED:-false}" \
  --conf "spark.sql.adaptive.autoBroadcastJoinThreshold=${TPCH_ADAPTIVE_AUTO_BROADCAST_JOIN_THRESHOLD:-${TPCH_AUTO_BROADCAST_JOIN_THRESHOLD:-12g}}" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=${TPCH_ADAPTIVE_COALESCE_PARTITIONS_ENABLED:-true}" \
  --conf "spark.sql.adaptive.localShuffleReader.enabled=${TPCH_ADAPTIVE_LOCAL_SHUFFLE_READER_ENABLED:-true}" \
  --conf "spark.sql.adaptive.skewJoin.enabled=${TPCH_ADAPTIVE_SKEW_JOIN_ENABLED:-true}" \
  --conf "spark.sql.optimizer.runtime.bloomFilter.enabled=${TPCH_RUNTIME_BLOOM_FILTER_ENABLED:-true}" \
  --conf spark.sql.shuffle.partitions="${TPCH_SHUFFLE_PARTITIONS:-1024}" \
  --conf "spark.sql.files.maxPartitionBytes=${TPCH_FILES_MAX_PARTITION_BYTES:-4g}" \
  --conf "spark.sql.files.minPartitionNum=${TPCH_FILES_MIN_PARTITION_NUM:-2048}" \
  "${FILE_PARTITION_CONF[@]}" \
  --conf "spark.sql.autoBroadcastJoinThreshold=${TPCH_AUTO_BROADCAST_JOIN_THRESHOLD:-12g}" \
  --conf "spark.sql.cbo.enabled=${TPCH_CBO_ENABLED:-false}" \
  --conf "spark.sql.cbo.planStats.enabled=${TPCH_CBO_PLAN_STATS_ENABLED:-false}" \
  --conf "spark.sql.cbo.joinReorder.enabled=${TPCH_CBO_JOIN_REORDER_ENABLED:-false}" \
  --conf "spark.sql.cbo.starSchemaDetection=${TPCH_CBO_STAR_SCHEMA_DETECTION:-false}" \
  --conf "spark.sql.cbo.joinReorder.dp.threshold=${TPCH_CBO_JOIN_REORDER_DP_THRESHOLD:-12}" \
  --conf "spark.sql.cbo.joinReorder.dp.star.filter=${TPCH_CBO_JOIN_REORDER_DP_STAR_FILTER:-false}" \
  --conf "spark.rapids.sql.optimizer.joinReorder.enabled=${TPCH_PRIVATE_JOIN_REORDER_ENABLED:-true}" \
  --conf spark.speculation=false \
  --conf spark.task.maxFailures=1 \
  --conf spark.locality.wait=0 \
  --conf spark.driver.maxResultSize=32g \
  --conf "spark.driver.extraJavaOptions=${JVM_OPTS}" \
  --conf "spark.executor.extraJavaOptions=${JVM_OPTS}" \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf "spark.scheduler.listenerbus.eventqueue.shared.capacity=${TPCH_LISTENERBUS_SHARED_CAPACITY:-100000}" \
  --conf spark.eventLog.enabled=true \
  --conf "spark.eventLog.dir=file://${RESULT_DIR}/eventlog" \
  "${POLICY_SPARK_CONF[@]}" \
  "${APPLICATION}" >"${LOG_DIR}/driver.log" 2>&1
rc=$?
set -e

printf '%s\n' "${rc}" >"${STATE_DIR}/driver.rc"
printf '%s\n' "${rc}" >"${RESULT_DIR}/spark-submit.rc"
touch "${STATE_DIR}/done"
archive_executor_logs
exit "${rc}"
