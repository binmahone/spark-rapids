#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly phase=${3:?Phase}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/evidence/q9-codec-$phase.log" ]] || exit 1
exec > >(tee "$work/evidence/q9-codec-$phase.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/evidence/q9-codec-'"$phase"'.exit"' EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_q9_codec.py" --work "$work" --thread-id "$thread" \
  --phase "$phase"
