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
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ "$phase" = A0 || "$phase" = B1 || "$phase" = B2 || "$phase" = A3 ]] || exit 2
[[ ! -e "$work/evidence/early-shuffle-$phase.log" ]] || exit 1
exec > >(tee "$work/evidence/early-shuffle-$phase.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/early-shuffle-$phase.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_early_shuffle.py" --work "$work" --thread-id "$thread" --phase "$phase"
