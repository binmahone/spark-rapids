#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly variant=${3:-frozen}
readonly query=${4:-8}
[[ "$query" == 1 || "$query" == 8 || "$query" == 9 ]] || exit 2
suffix=""
case "$variant" in
    frozen) ;;
    copy-stream|join-stats|decimal-null-copy) suffix="-$variant" ;;
    *) exit 2 ;;
esac
if [[ "$query" != 8 ]]; then suffix="-q$query$suffix"; fi
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/evidence/scan-timeline-process-tree$suffix.log" ]] || exit 1
exec > >(tee "$work/evidence/scan-timeline-process-tree$suffix.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/scan-timeline-process-tree$suffix.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_scan_timeline.py" --work "$work" --thread-id "$thread" --variant "$variant" --query "$query"
