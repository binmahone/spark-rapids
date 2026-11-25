#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute retained work directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$work/followups"
exec > >(tee "$work/evidence/followups.log") 2>&1
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/../runtime_prefilter/monitor_host.py" --work "$work" --output "$work/evidence/followups-host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/followups.exit"
}
trap finish EXIT
"$work/spark-venv/bin/python" "$here/toy_oracle.py" --output "$work/followups/toy-oracle.json" > "$work/followups/toy-oracle.log" 2>&1
python3 "$here/run_followups.py" --work "$work"
