#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ ! -e "$work/narrow-keys/oracle.json" ]] || exit 1
mkdir -p "$work/narrow-keys"
exec > >(tee "$work/narrow-keys/oracle.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/narrow-keys/oracle.exit"; }
trap finish EXIT
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["thread_id"]==sys.argv[2]' "$work/evidence/owner.json" "$thread"
export JAVA_HOME="$work/jdk17"
export CUDA_VISIBLE_DEVICES=''
"$work/spark-venv/bin/python" "$here/narrow_key_oracle.py" \
  --dataset /opt/dlami/nvme/daily-trend-hardened/dataset --output "$work/narrow-keys/oracle.json"
