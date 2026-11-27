#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly phase=${3:?Phase is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ "$phase" =~ ^(oracle|A0|B1|B2|C1|C2|A3|S0|S1|S2)$ ]] || exit 2
[[ ! -e "$work/evidence/critical-path-$phase.log" ]] || exit 1
exec > >(tee "$work/evidence/critical-path-$phase.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/critical-path-$phase.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_critical_path.py" --work "$work" --thread-id "$thread" --phase "$phase"
