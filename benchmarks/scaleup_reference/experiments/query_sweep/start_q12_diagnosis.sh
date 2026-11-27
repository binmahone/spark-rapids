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
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly phase=${3:?Phase is required}
readonly observer_mode=${4:-full}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ "$phase" =~ ^(probe|probe1|probe2|probe3|probe4|probe5|probe6|probe7|short0|short1|prefix0|prefix1|light0|locks0|light1|locks1|locks2|light2|lruoff0|lruon0|lruoff1|lruon1)$ ]] || exit 2
[[ "$observer_mode" =~ ^(full|light|locks)$ ]] || exit 2
[[ ! -e "$work/evidence/q12-diagnosis-$phase.log" ]] || exit 1
exec > >(tee "$work/evidence/q12-diagnosis-$phase.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/q12-diagnosis-$phase.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_q12_diagnosis.py" --work "$work" --thread-id "$thread" --phase "$phase" --observer-mode "$observer_mode"
