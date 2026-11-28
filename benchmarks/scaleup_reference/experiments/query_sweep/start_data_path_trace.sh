#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly variant=${3:-control}
readonly query=${4:-8}
[[ "$query" == 5 || "$query" == 8 || "$query" == 10 || "$query" == 18 || "$query" == 20 ]] || exit 2
suffix=""
case "$variant" in
    control|scan-pack|scan-split|scan-split-cuda|scan-split-cuda2|shuffle-buffer|shuffle-wait) suffix="-$variant" ;;
    *) exit 2 ;;
esac
suffix="-q$query$suffix"
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/evidence/data-path-trace$suffix.log" ]] || exit 1
exec > >(tee "$work/evidence/data-path-trace$suffix.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/data-path-trace$suffix.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_data_path_trace.py" --work "$work" --thread-id "$thread" --variant "$variant" --query "$query"
