#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute retained work directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly probe="$here/../runtime_prefilter"
exec > >(tee "$work/evidence/screen.log") 2>&1
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$probe/monitor_host.py" --work "$work" --output "$work/evidence/screen-host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/screen.exit"
}
trap finish EXIT
python3 "$here/run_screen.py" --work "$work"
