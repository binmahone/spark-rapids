#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly scratch=${2:?Private scratch directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly out="$work/artifacts/cold-host-read"
mkdir -p "$out" "$scratch"
exec > >(tee "$out/build-and-tests.log") 2>&1
g++ -std=c++17 -O2 -Wall -Wextra -Werror -fPIC -shared "$here/cold_host_read.cpp" -ldl -o "$out/libcold_host_read.so"
g++ -std=c++17 -O2 -Wall -Wextra -Werror -fPIC -shared "$here/cold_host_read_fault.cpp" -ldl -o "$out/libcold_host_read_fault.so"
for test in buffered cold-direct error short; do
  mkdir "$scratch/$test" "$out/$test"
  mode=$test
  preload="$out/libcold_host_read.so"
  flags=()
  if [[ "$test" == cold-direct ]]; then flags+=(--expect-direct); fi
  if [[ "$test" == error || "$test" == short ]]; then
    mode=cold-direct
    preload="$preload:$out/libcold_host_read_fault.so"
    flags+=(--expect-fallback)
  fi
  if [[ "$test" == short ]]; then export SCALEUP_TEST_SHORT_READ=1; else unset SCALEUP_TEST_SHORT_READ; fi
  env LD_PRELOAD="$preload" SCALEUP_COLD_READ_ROOT="$scratch/$test" \
      SCALEUP_COLD_READ_STATS="$out/$test" SCALEUP_COLD_READ_MODE="$mode" \
      python3 "$here/test_cold_host_read.py" --root "$scratch/$test" "${flags[@]}" > "$out/$test/result.json"
done
sha256sum "$out/libcold_host_read.so" "$here/cold_host_read.cpp" > "$out/sha256.txt"
git -C "$here" rev-parse HEAD > "$out/source-revision.txt"
printf '%s\n' 'Native adapter and all four byte-parity/fallback test processes passed'
