#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly query=${3:?Query number}
readonly attempt=${4:-0}
readonly concat_implementation=${5:-}
readonly timeline_only=${6:-false}
readonly scan_copy_implementation=${7:-}
readonly java_ranges=${8:-false}
extra_args=()
if [[ "$java_ranges" == true ]]; then extra_args+=(--java-ranges); fi
if [[ -n "$concat_implementation" ]]; then
  extra_args+=(--concat-implementation "$concat_implementation")
fi
if [[ "$timeline_only" == true ]]; then extra_args+=(--timeline-only); fi
if [[ -n "$scan_copy_implementation" ]]; then
  extra_args+=(--scan-copy-implementation "$scan_copy_implementation")
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
