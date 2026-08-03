#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVIDENCE_ROOT="${SCRIPT_DIR}/suite-evidence-v001"
RUNNER="${SCRIPT_DIR}/run_v017_workload.py"
STAMP="$(TZ=Asia/Shanghai date +%m%d%H%M)"

run_one() {
  local workload="$1"
  local adaptive="$2"
  local suffix="$3"
  local run_id="v17-${workload:0:3}-${adaptive}-${suffix}-${STAMP}"
  python3 "${RUNNER}" \
    --workload "${workload}" \
    --adaptive "${adaptive}" \
    --run-id "${run_id}" \
    --evidence-root "${EVIDENCE_ROOT}" \
    2>&1 | tee "${EVIDENCE_ROOT}/${run_id}.log"
}

mkdir -p "${EVIDENCE_ROOT}"
run_one paypal on r3
run_one paypal off r3
run_one walmart on r3
run_one walmart off r3
run_one finra on r1
run_one finra off r1
