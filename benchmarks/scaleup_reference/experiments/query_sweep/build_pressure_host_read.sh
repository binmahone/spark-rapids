#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly scratch=${2:?Private scratch directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly out="$work/artifacts/pressure-host-read-v1"
mkdir -p "$out" "$scratch"
exec > >(tee "$out/build-and-tests.log") 2>&1
readonly stat_header=/usr/src/linux-aws-6.8-headers-6.8.0-1066/include/uapi/linux/stat.h
test -f "$stat_header"
mkdir -p "$out/include/linux"
cp "$stat_header" "$out/include/linux/stat.h"
g++ -std=c++17 -O2 -Wall -Wextra -Werror -I "$out/include" -fPIC -shared "$here/cold_host_read.cpp" -ldl -o "$out/libcold_host_read.so"
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
sha256sum "$stat_header" >> "$out/sha256.txt"
git -C "$here" rev-parse HEAD > "$out/source-revision.txt"
for test in pressure pressure-error pressure-short; do
  mkdir "$scratch/$test" "$out/$test"
  python3 -c 'import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(bytes(8))' "$out/$test/control.bin"
  preload="$out/libcold_host_read.so"
  flags=()
  unset SCALEUP_TEST_SHORT_READ
  if [[ "$test" != pressure ]]; then
    preload="$preload:$out/libcold_host_read_fault.so"
    flags+=(--expect-fallback)
  fi
  if [[ "$test" == pressure-short ]]; then export SCALEUP_TEST_SHORT_READ=1; fi
  env LD_PRELOAD="$preload" SCALEUP_COLD_READ_ROOT="$scratch/$test" \
      SCALEUP_COLD_READ_STATS="$out/$test" SCALEUP_COLD_READ_MODE=pressure-direct \
      SCALEUP_COLD_READ_PRESSURE="$out/$test/control.bin" \
      python3 "$here/test_pressure_host_read.py" --root "$scratch/$test" "${flags[@]}" > "$out/$test/result.json"
done
printf '%s\n' 'All original and pressure-transition byte-parity/fallback tests passed'
