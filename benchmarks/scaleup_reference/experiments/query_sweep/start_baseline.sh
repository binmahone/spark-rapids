#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute retained work directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly probe="$here/../runtime_prefilter"
mkdir -p "$work/evidence"
exec > >(tee "$work/evidence/baseline.log") 2>&1
ulimit -n 65536
python3 "$probe/capture_host.py" "$work"
python3 "$probe/monitor_host.py" --work "$work" --output "$work/evidence/baseline-host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/baseline.exit"
}
trap finish EXIT
python3 "$here/run_baseline.py" --work "$work"
