#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute owned work directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$work/evidence" "$work/results"
exec > >(tee "$work/evidence/probe.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/evidence/probe.exit"' EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_matrix.py" --work "$work"
