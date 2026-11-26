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
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owning thread ID is required}
readonly mode=${3:?screen or full is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ "$mode" = screen || "$mode" = full ]] || exit 2
readonly previous=$(cat /proc/sys/vm/watermark_scale_factor)
[[ "$previous" = 10 ]] || exit 1
[[ ! -e "$work/evidence/pressure-$mode.log" ]] || exit 1
exec > >(tee "$work/evidence/pressure-$mode.log") 2>&1
finish() {
    code=$?
    sudo -n sysctl -w "vm.watermark_scale_factor=$previous" || code=1
    printf '%s\n' "$code" > "$work/evidence/pressure-$mode.exit"
}
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_pressure_followup.py" --work "$work" --thread-id "$thread" --mode "$mode"
