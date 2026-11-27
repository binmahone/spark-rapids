#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
python3 - "$work" "$thread" <<'PY'
import json
from pathlib import Path
import sys
work = Path(sys.argv[1])
assert json.loads((work / 'evidence/owner.json').read_text())['thread_id'] == sys.argv[2]
assert not (work / 'critical/oracle.json').exists()
assert not (work / 'evidence/sol-domain.log').exists()
(work / 'critical').mkdir(exist_ok=True)
PY
exec > >(tee "$work/evidence/sol-domain.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/sol-domain.exit"; }
trap finish EXIT
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
export CUDA_VISIBLE_DEVICES=''
timeout --signal=TERM --kill-after=30 900 "$work/spark-venv/bin/python" \
  "$here/critical_oracle.py" --output "$work/critical/oracle.json" \
  --dataset /opt/dlami/nvme/daily-trend-hardened/dataset
