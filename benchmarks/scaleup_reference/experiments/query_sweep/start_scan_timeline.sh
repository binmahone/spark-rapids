#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/evidence/scan-timeline.log" ]] || exit 1
exec > >(tee "$work/evidence/scan-timeline.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/scan-timeline.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_scan_timeline.py" --work "$work" --thread-id "$thread"
