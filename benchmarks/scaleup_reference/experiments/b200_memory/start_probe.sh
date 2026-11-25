#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Owned absolute directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$work/evidence" "$work/results"
exec > >(tee "$work/evidence/probe.log") 2>&1
test "$(cat "$work/evidence/prepare.exit")" = 0
ulimit -n 65536
ulimit -c 0
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/capture_host.py" "$work"
python3 "$here/monitor_host.py" --work "$work" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/probe.exit"
}
trap finish EXIT
python3 "$here/run_matrix.py" --work "$work" --budget default --reference-only
python3 "$here/run_matrix.py" --work "$work" --budget default
