#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly scratch=${2:?Private test scratch directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly out="$work/artifacts/cold-host-read-v2"
readonly library="$out/libcold_host_read.so"
readonly before=$(sha256sum "$library")
for test in buffered cold-direct error short; do
  mkdir -p "$scratch"
  mkdir "$scratch/$test" "$out/extended-$test"
  mode=$test
  preload="$library"
  flags=()
  if [[ "$test" == cold-direct ]]; then flags+=(--expect-direct); fi
  if [[ "$test" == error || "$test" == short ]]; then
    mode=cold-direct
    preload="$preload:$out/libcold_host_read_fault.so"
    flags+=(--expect-fallback)
  fi
  if [[ "$test" == short ]]; then export SCALEUP_TEST_SHORT_READ=1; else unset SCALEUP_TEST_SHORT_READ; fi
  env LD_PRELOAD="$preload" SCALEUP_COLD_READ_ROOT="$scratch/$test" \
      SCALEUP_COLD_READ_STATS="$out/extended-$test" SCALEUP_COLD_READ_MODE="$mode" \
      python3 "$here/test_cold_host_read.py" --root "$scratch/$test" "${flags[@]}" > "$out/extended-$test/result.json"
done
[[ "$before" == "$(sha256sum "$library")" ]]
git -C "$here" rev-parse HEAD > "$out/extended-test-source-revision.txt"
printf '%s\n' 'Unchanged native artifact passed extended byte-parity, residency and failure tests'
