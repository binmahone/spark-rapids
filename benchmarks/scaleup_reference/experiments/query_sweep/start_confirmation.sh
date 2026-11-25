#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute retained work directory is required}
readonly selection=${2:?Selection JSON path is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
exec > >(tee "$work/evidence/confirmation.log") 2>&1
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/../runtime_prefilter/monitor_host.py" --work "$work" --output "$work/evidence/confirmation-host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/confirmation.exit"
}
trap finish EXIT
python3 "$here/run_confirmation.py" --work "$work" --selection "$selection"
