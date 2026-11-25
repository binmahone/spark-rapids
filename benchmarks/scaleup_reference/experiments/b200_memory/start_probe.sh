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
readonly here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$work/evidence" "$work/results"
exec > >(tee "$work/evidence/probe.log") 2>&1
test "$(cat "$work/evidence/prepare.exit")" = 0
ulimit -n 65536
ulimit -c 0
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/capture_host.py" "$work"
python3 "$here/monitor_host.py" --work "$work" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/probe.exit"
}
trap finish EXIT
python3 "$here/run_matrix.py" --work "$work" --budget default --reference-only
python3 "$here/run_matrix.py" --work "$work" --budget default
