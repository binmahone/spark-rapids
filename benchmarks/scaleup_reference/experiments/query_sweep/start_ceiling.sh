#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared host work directory is required}
readonly phase=${2:?Phase sql or tune is required}
readonly attempt=${3:-initial}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ "$phase" = sql || "$phase" = tune ]] || exit 2
[[ "$attempt" =~ ^[A-Za-z0-9_-]+$ ]] || exit 2
tag="ceiling-$phase"
resume_args=()
if [[ "$attempt" != initial ]]; then
    tag="$tag-$attempt"
    resume_args=(--resume)
fi
mkdir -p "$work/evidence"
[[ ! -e "$work/evidence/$tag.log" ]] || exit 1
exec > >(tee "$work/evidence/$tag.log") 2>&1
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
if [[ ! -e "$work/evidence/host-prerequisites.json" ]]; then
    python3 "$here/../runtime_prefilter/capture_host.py" "$work"
fi
python3 "$here/../runtime_prefilter/monitor_host.py" --work "$work" \
    --output "$work/evidence/$tag-host-monitor.jsonl" &
monitor_pid=$!
finish() {
    code=$?
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    printf '%s\n' "$code" > "$work/evidence/$tag.exit"
}
trap finish EXIT
python3 "$here/run_ceiling.py" --work "$work" --phase "$phase" "${resume_args[@]}"
