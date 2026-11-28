#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly query=${3:?Query number}
readonly attempt=${4:-0}
readonly concat_implementation=${5:-}
extra_args=()
if [[ -n "$concat_implementation" ]]; then
  extra_args=(--concat-implementation "$concat_implementation")
fi
suffix=""
if [[ "$attempt" != 0 ]]; then suffix="-attempt$attempt"; fi
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/evidence/code-cuda-q$query$suffix.log" ]] || exit 1
exec > >(tee "$work/evidence/code-cuda-q$query$suffix.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/evidence/code-cuda-q$query$suffix.exit"' EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_data_path_trace.py" --work "$work" --thread-id "$thread" \
  --variant code-profile --query "$query" --attempt "$attempt" "${extra_args[@]}"
