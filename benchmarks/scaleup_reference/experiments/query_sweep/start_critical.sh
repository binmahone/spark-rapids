#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared work directory is required}
readonly thread=${2:?Owner thread is required}
readonly phase=${3:?Phase is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
[[ "$phase" = oracle || "$phase" = screen || "$phase" = confirm || "$phase" = suite ]] || exit 2
[[ ! -e "$work/evidence/critical-$phase.log" ]] || exit 1
exec > >(tee "$work/evidence/critical-$phase.log") 2>&1
finish() { printf '%s\n' "$?" > "$work/evidence/critical-$phase.exit"; }
trap finish EXIT
ulimit -n 65536
export JAVA_HOME="$work/jdk17"
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$here/run_critical.py" --work "$work" --thread-id "$thread" --phase "$phase"
