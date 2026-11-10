#!/usr/bin/env bash
set -euo pipefail
readonly python=${1:?Specify the isolated Python executable}
readonly manifest=${2:?Specify the range manifest}
readonly output=${3:?Specify a new evidence directory}
readonly file_workers=${4:-1}
readonly cache_policy=${5:-warm}
readonly here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$(dirname "$output")"
mkdir "$output"
printf '%q ' "$0" "$@" > "$output/command.txt"
printf '\n' >> "$output/command.txt"
"$python" -m pip freeze > "$output/dependencies.txt" 2>/dev/null || \
  uv pip freeze --python "$python" > "$output/dependencies.txt"
cp "$manifest" "$output/manifest.json"
uname -a > "$output/host.txt"
findmnt -T "$("$python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["files"][0]["path"])' "$manifest")" \
  > "$output/mount.txt"
export CUDA_VISIBLE_DEVICES=''
export KVIKIO_COMPAT_MODE=ON
export KVIKIO_AUTO_DIRECT_IO_READ=OFF
common=(--manifest "$manifest" --file-workers "$file_workers" --repeats 3 --max-buffer-gib 32
  --cache-policy "$cache_policy")
"$python" "$here/range_benchmark.py" run "${common[@]}" --backend posix \
  | tee "$output/posix-serial-ranges.jsonl"
# A fixed global pool isolates submission-window changes from total thread count.
for window in 1 2 4 8 4 2 1; do
  ordinal=$(find "$output" -name 'kvikio-*.jsonl' | wc -l)
  "$python" "$here/range_benchmark.py" run "${common[@]}" \
    --backend kvikio --threads 32 --window "$window" --task-bytes 0 \
    | tee "$output/kvikio-$ordinal-window-$window.jsonl"
done
# A second series checks whether splitting a range substitutes for submitting more ranges.
for threads in 1 2 4 8; do
  "$python" "$here/range_benchmark.py" run "${common[@]}" \
    --backend kvikio --threads "$threads" --window 1 --task-bytes 1048576 \
    | tee "$output/kvikio-split-threads-$threads.jsonl"
done
"$python" "$here/range_benchmark.py" run "${common[@]}" --backend posix \
  | tee "$output/posix-serial-ranges-repeat.jsonl"
# Trace actual native read intervals separately from all uninstrumented timings.
g++ -std=c++17 -O2 -shared -fPIC -pthread "$here/trace_pread.cpp" -ldl \
  -o "$output/trace_pread.so"
prefix=$("$python" -c 'import json,os,sys; print(os.path.commonpath([f["path"] for f in json.load(open(sys.argv[1]))["files"]]))' "$manifest")
KVIKIO_TRACE_PREFIX="$prefix" KVIKIO_TRACE_OUTPUT="$output/native-read-trace.jsonl" \
  LD_PRELOAD="$output/trace_pread.so" \
  "$python" "$here/range_benchmark.py" run --manifest "$manifest" --backend kvikio \
    --file-workers "$file_workers" --threads 4 --window 4 --task-bytes 0 --repeats 1 \
    --max-buffer-gib 32 | tee "$output/traced-run-not-performance.jsonl"
"$python" "$here/analyze_trace.py" "$output/native-read-trace.jsonl" \
  --require-same-fd-overlap | tee "$output/native-overlap-summary.json"
KVIKIO_TRACE_PREFIX="$prefix" KVIKIO_TRACE_OUTPUT="$output/native-serial-trace.jsonl" \
  LD_PRELOAD="$output/trace_pread.so" \
  "$python" "$here/range_benchmark.py" run --manifest "$manifest" --backend kvikio \
    --file-workers "$file_workers" --threads 4 --window 1 --task-bytes 0 --repeats 1 \
    --max-buffer-gib 32 | tee "$output/traced-serial-not-performance.jsonl"
"$python" "$here/analyze_trace.py" "$output/native-serial-trace.jsonl" \
  --max-same-fd 1 | tee "$output/native-serial-summary.json"
