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
readonly work=${1:?Owned absolute directory is required}
readonly label=${2:?Unique result label is required}
readonly queries=${3:-1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22}
readonly arms=${4:-baseline}
readonly mode=${5:-measure}
readonly gpus=${6:-0,1,2,3}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly evidence="$work/evidence/$label"
mkdir "$evidence"
exec > >(tee "$evidence/controller.log") 2>&1
ulimit -n 65536
ulimit -c 0
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/capture_host.py" "$work" "$gpus"
cp "$work/evidence/host-prerequisites.json" "$evidence/host-prerequisites.json"
python3 "$here/monitor_host.py" --work "$work" --gpus "$gpus" --output "$evidence/host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$evidence/controller.exit"
}
trap finish EXIT
extra=()
if [ "$mode" = prefix ]; then extra=(--prefix-reference --iterations 1); fi
python3 "$here/run_matrix.py" "${extra[@]}" --work "$work" --label "$label" --suite-audit \
    --queries "$queries" --order "$arms" --gpus "$gpus" --query-timeout 600
