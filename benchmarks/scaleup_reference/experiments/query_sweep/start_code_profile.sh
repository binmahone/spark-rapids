#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly label=${3:?Unique capture label}
readonly mode=${4:?control or stacks}
readonly queries=${5:-8,18,5}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/evidence/code-profile-$label.log" ]] || exit 1
exec > >(tee "$work/evidence/code-profile-$label.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/evidence/code-profile-'"$label"'.exit"' EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_code_profile.py" --work "$work" --thread-id "$thread" \
  --label "$label" --mode "$mode" --queries "$queries"
