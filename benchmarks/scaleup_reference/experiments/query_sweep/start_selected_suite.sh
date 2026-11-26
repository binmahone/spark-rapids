#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared host directory is required}
readonly name=${2:?Unique run name is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly selection=${3:-$here/selected-cases.json}
[[ "$name" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || exit 2
mkdir -p "$work/evidence"
[[ ! -e "$work/evidence/$name.log" ]] || { echo 'Run log exists; choose a new name' >&2; exit 1; }
exec > >(tee "$work/evidence/$name.log") 2>&1
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/../runtime_prefilter/monitor_host.py" --work "$work" --output "$work/evidence/$name-host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/$name.exit"
}
trap finish EXIT
python3 "$here/run_selected_suite.py" --work "$work" --name "$name" --selection "$selection"
