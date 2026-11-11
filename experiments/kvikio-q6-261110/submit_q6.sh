#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
readonly build=${1:?Specify the successful build-and-correctness job}
readonly backend=${2:?Specify filechannel or kvikio}
readonly dependency=${3:--}
readonly nodes=${4:-presto-gb200-gcn-[04,07-08,10,13-16]}
readonly profile=${5:-false}
readonly trace=${6:-false}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly revision=$(git -C "$here" rev-parse HEAD)
[[ "$build" =~ ^[0-9]+$ ]]
[[ "$backend" == filechannel || "$backend" == kvikio || "$backend" == kvikio-window4 || "$backend" == kvikio-range4 || "$backend" == kvikio-range4-fd4 || "$backend" == kvikio-range128 || "$backend" == kvikio-range128-fd4 || "$backend" == kvikio-lanes4 || "$backend" == kvikio-direct4-fd4 ]]
[[ "$dependency" == - || "$dependency" =~ ^[0-9]+$ ]]
[[ "$profile" == true || "$profile" == false ]]
[[ "$trace" == true || "$trace" == false ]]
test -z "$(git -C "$here" status --short)"
ssh -F /home/nvidia/mahone_gluten/cluster_ssh_config -o ConnectTimeout=12 \
  presto-cp-02-via-mac bash -s -- "$build" "$backend" "$dependency" "$revision" "$nodes" "$profile" "$trace" <<'REMOTE'
set -euo pipefail
readonly build=$1
readonly backend=$2
readonly dependency=$3
readonly revision=$4
readonly root=/home/mahonem/paul_q6_kvikio_261110
readonly previous=/home/mahonem/paul_hybrid_multithreaded_261105
readonly source=$root/src/cudf-spark
readonly output=$root/builds/job-$build
readonly nodes=$5
export TPCH_ENABLE_ASYNC_PROFILER=$6
export TPCH_ASYNC_PROFILER_EXECUTORS=0
export TPCH_ASYNC_PROFILER_OPTIONS=jfr,event=cpu,wall=10ms
export SLURM_CONF=/cm/shared/apps/slurm/etc/slurm/slurm.conf
readonly slurm=/cm/local/apps/slurm/current/bin
readonly node_count=$("$slurm/scontrol" show hostnames "$nodes" | wc -l)
readonly trace=$7
readonly trace_host=$("$slurm/scontrol" show hostnames "$nodes" | sed -n '1p')
[[ "$node_count" -ge 1 && "$node_count" -le 8 ]]
test "$("$slurm/sacct" -j "$build" -X -n -o State | xargs)" = COMPLETED
grep -q '^KVIKIO_Q6_BUILD_AND_CASES_PASS$' "$output/full.log"
test -z "$(git -C "$source" status --short)"
git -C "$source" pull --ff-only
test "$(git -C "$source" rev-parse HEAD)" = "$revision"
mkdir -p "$root/policies"
export TPCH_POLICY_MANIFEST_PATH
TPCH_POLICY_MANIFEST_PATH=$(mktemp "$root/policies/q6-$backend-XXXXXXXX.json")
readonly trace_directory=$root/traces/$(basename "$TPCH_POLICY_MANIFEST_PATH" .json)
if [[ "$trace" == true ]]; then mkdir -p "$trace_directory"; chmod 700 "$trace_directory"; fi
python3 - "$previous/h2d-runtime-policy.json" "$output/rapids-kvikio-q6.jar.identity.json" \
  "$TPCH_POLICY_MANIFEST_PATH" "$backend" "$nodes" "$output" "$build" "$node_count" "$trace" "$trace_host" "$trace_directory" <<'PY'
import json
from pathlib import Path
import sys
base, identity, target, backend, nodes, output, build, node_count, trace, trace_host, trace_directory = sys.argv[1:]
policy = json.loads(Path(base).read_text())
assert policy['artifact_identity']['jar_sha256'] == 'ab3b63e7bf10d4e304dfdfd6c06b236b7898c6c82859bdc65c01184522aab432'
identity = json.loads(Path(identity).read_text())
if backend.endswith('-fd4'):
    assert 4 in identity.get('supported_file_handle_limits', []), 'Build lacks independent-file-handle selection'
if backend == 'kvikio-lanes4':
    assert identity.get('supports_strided_lane_tasks', False), 'Build lacks coarse lane task submission'
if backend == 'kvikio-direct4-fd4':
    assert identity.get('supports_direct_range_tasks', False), 'Build lacks direct per-range tasks'
if trace == 'true':
    assert identity.get('io_tracer_sha256'), 'Build lacks syscall tracing'
policy['name'] = 'q6-native-host-ab-' + backend
policy['artifact_identity']['build_job'] = int(build)
policy['artifact_identity']['jar_sha256'] = identity['jar_sha256']
policy['artifact_identity']['public_revision'] = identity['source_revision']
policy['artifact_identity']['kvikio_bridge_sha256'] = identity['bridge_sha256']
policy['benchmark_contract']['nodes'] = nodes
policy['benchmark_contract']['executors'] = int(node_count) * 4
conf = policy['uniform_configuration']
conf['spark.executor.instances'] = str(int(node_count) * 4)
conf['spark.rapids.sql.multiThreadedRead.numThreads'] = '16'
conf['spark.rapids.sql.test.format.parquet.hybridScan.packedH2D.enabled'] = 'true'
conf['spark.executorEnv.RAPIDS_TEST_KVIKIO_FILE_HANDLES'] = '4' if backend.endswith('-fd4') or backend == 'kvikio-lanes4' else '1'
conf['spark.executorEnv.RAPIDS_TEST_KVIKIO_LANE_TASKS'] = '4' if backend == 'kvikio-lanes4' else '0'
conf['spark.executorEnv.RAPIDS_TEST_KVIKIO_DIRECT_TASKS'] = '1' if backend == 'kvikio-direct4-fd4' else '0'
conf['spark.executorEnv.JAVA_TOOL_OPTIONS'] = (
    '-Drapids.test.rangeIoTiming=true -Drapids.test.directHostRead=true '
    '-Drapids.test.rangeReadConcurrency=4 '
    '-Drapids.test.kvikioHostRead=' + str(backend.startswith('kvikio')).lower() + ' '
    '-Drapids.test.kvikioLibrary=' + output + '/libkvikio_host_diagnostic.so '
    '-Drapids.test.kvikioThreads=32 -Drapids.test.kvikioTaskBytes='
    + ('9223372036854775807' if backend.startswith('kvikio-range') or backend in ('kvikio-lanes4', 'kvikio-direct4-fd4') else '4194304') + ' '
    '-Drapids.test.kvikioRangeWindow=' + ('128' if backend.startswith('kvikio-range128')
      else '4' if backend == 'kvikio-window4' or backend.startswith('kvikio-range4') or backend in ('kvikio-lanes4', 'kvikio-direct4-fd4') else '1'))
policy['diagnostic_trace'] = {'enabled': trace == 'true', 'directory': trace_directory,
                            'host': trace_host, 'tracer_sha256': identity.get('io_tracer_sha256')}
if trace == 'true':
    conf['spark.executorEnv.LD_PRELOAD'] = output + '/libio_trace.so'
    conf['spark.executorEnv.RAPIDS_IO_TRACE_HOST'] = trace_host
    conf['spark.executorEnv.RAPIDS_IO_TRACE_DIR'] = trace_directory
    conf['spark.executorEnv.RAPIDS_IO_TRACE_PREFIX'] = '/scratch/prestouser/tpch-rs-float-no-delta/scale-30000/lineitem/part.'
Path(target).write_text(json.dumps(policy, indent=2) + '\n')
PY
chmod 444 "$TPCH_POLICY_MANIFEST_PATH"
export TPCH_RAPIDS_JAR=$output/rapids-kvikio-q6.jar
export TPCH_EXPERIMENT_PROFILE=q6-native-host-ab-$backend
dependency_args=()
if [[ "$dependency" != - ]]; then
  dependency_args=("--dependency=afterok:$dependency" --kill-on-invalid-dep=yes)
fi
"$slurm/sbatch" --parsable --nodes="$node_count" --nodelist="$nodes" --no-requeue \
  "${dependency_args[@]}" --job-name="mahonem-q6-$backend" \
  --output="$previous/logs/slurm-%j.out" --error="$previous/logs/slurm-%j.err" \
  --export=ALL,TPCH_RAPIDS_JAR,TPCH_POLICY_MANIFEST_PATH,TPCH_EXPERIMENT_PROFILE,TPCH_ENABLE_ASYNC_PROFILER,TPCH_ASYNC_PROFILER_EXECUTORS,TPCH_ASYNC_PROFILER_OPTIONS \
  "$source/experiments/kvikio-q6-261110/run_q6.slurm"
REMOTE
